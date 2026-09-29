import asyncio
import csv
import datetime
import hashlib
import io
import itertools
import json
import logging
import mimetypes
import secrets
import shutil
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote as url_quote

import aiofiles
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("fileserver")

UPLOAD_DIR   = Path(__file__).parent / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)
TRASH_DIR    = UPLOAD_DIR / '.trash'
TRASH_INFO   = TRASH_DIR / '.info'
TRASH_DIR.mkdir(exist_ok=True)
TRASH_INFO.mkdir(exist_ok=True)
LOG_FILE     = Path(__file__).parent / "access.log"
CHAT_LOG     = Path(__file__).parent / "chat.log"
MEMO_FILE    = Path(__file__).parent / "memos.json"
CHUNK_SIZE        = 64 * 1024
MAX_UPLOAD        = 2 * 1024 * 1024 * 1024   # 2 GB
MAX_EDIT_SIZE     = 5 * 1024 * 1024           # 5 MB
MAX_FILENAME      = 255
SESSION_TTL       = datetime.timedelta(hours=24)
RATE_LIMIT_MAX    = 10
RATE_LIMIT_WINDOW = datetime.timedelta(minutes=5)

chat_clients:    set = set()
ws_users:        dict = {}
sessions:        dict = {}
_login_attempts: dict = {}
_log_queue       = None
_chat_queue      = None

@asynccontextmanager
async def lifespan(app):
    global _log_queue, _chat_queue
    _log_queue  = asyncio.Queue()
    _chat_queue = asyncio.Queue()

    async def _file_writer(queue, path):
        while True:
            entry = await queue.get()
            if entry is None:
                queue.task_done(); break
            try:
                async with aiofiles.open(path, "a", encoding="utf-8") as f:
                    await f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            except OSError as e:
                logger.error(f"Log write failed [{path.name}]: {e}")
            finally:
                queue.task_done()

    async def _session_cleaner():
        while True:
            await asyncio.sleep(3600)
            now = datetime.datetime.now()
            expired = [t for t, s in sessions.items() if now - s["created"] > SESSION_TTL]
            for t in expired:
                sessions.pop(t, None)
            cutoff = now - RATE_LIMIT_WINDOW
            for ip in list(_login_attempts.keys()):
                _login_attempts[ip] = [t for t in _login_attempts[ip] if t > cutoff]
                if not _login_attempts[ip]:
                    del _login_attempts[ip]

    log_task     = asyncio.create_task(_file_writer(_log_queue,  LOG_FILE))
    chat_task    = asyncio.create_task(_file_writer(_chat_queue, CHAT_LOG))
    cleaner_task = asyncio.create_task(_session_cleaner())
    yield
    await _log_queue.put(None)
    await _chat_queue.put(None)
    await log_task; await chat_task
    cleaner_task.cancel()
    try: await cleaner_task
    except asyncio.CancelledError: pass

app = FastAPI(lifespan=lifespan)

try:
    import openslide
    from openslide.deepzoom import DeepZoomGenerator
    from PIL import Image as _PILImage
    OPENSLIDE_OK = True
except (ImportError, ModuleNotFoundError):
    OPENSLIDE_OK = False

_WSI_TILE_SIZE    = 256
_WSI_TILE_OVERLAP = 1
_wsi_executor     = ThreadPoolExecutor(max_workers=4, thread_name_prefix="wsi")
_wsi_tls          = threading.local()

def _wsi_get_dz(file_path_str):
    if not hasattr(_wsi_tls, "cache"): _wsi_tls.cache = {}
    cache  = _wsi_tls.cache
    mtime  = Path(file_path_str).stat().st_mtime
    entry  = cache.get(file_path_str)
    if entry and entry[0] == mtime: return entry[1]
    slide  = openslide.OpenSlide(file_path_str)
    dz     = DeepZoomGenerator(slide, tile_size=_WSI_TILE_SIZE, overlap=_WSI_TILE_OVERLAP)
    cache[file_path_str] = (mtime, dz)
    return dz

def now_str():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def fmt_size(n):
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024: return f"{n:.0f} {unit}"
        n //= 1024
    return f"{n:.1f} TB"

def _unique_path(dest_dir, filename):
    dest = dest_dir / filename
    if not dest.exists(): return dest
    stem = Path(filename).stem; suffix = Path(filename).suffix; n = 1
    while True:
        candidate = dest_dir / f"{stem}({n}){suffix}"
        if not candidate.exists(): return candidate
        n += 1

def safe_path(base, rel):
    target = (base / rel).resolve()
    if not str(target).startswith(str(base.resolve())):
        raise ValueError("허용되지 않은 경로입니다")
    return target

def write_log(action, path, name, size=None, ip=None, user=None):
    entry = {"time": now_str(), "action": action, "name": name, "path": path}
    if size is not None: entry["size"] = size
    if ip:   entry["ip"]   = ip
    if user: entry["user"] = user
    if _log_queue is not None: _log_queue.put_nowait(entry)
    else:
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except OSError as e: logger.error(f"Log write failed: {e}")

def write_chat_log(entry):
    if _chat_queue is not None: _chat_queue.put_nowait(entry)
    else:
        try:
            with open(CHAT_LOG, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except OSError as e: logger.error(f"Chat log write failed: {e}")

def dir_tree(path, base):
    rel = str(path.relative_to(base)) if path != base else ""
    node = {"name": path.name or "홈", "path": rel, "children": []}
    try:
        for child in sorted(path.iterdir()):
            if child.is_dir() and not child.name.startswith('.'):
                node["children"].append(dir_tree(child, base))
    except PermissionError: pass
    return node

async def _err(ws, msg):
    try: await ws.send_text(json.dumps({"type": "error", "msg": msg}))
    except Exception: pass

async def broadcast_chat(message):
    global chat_clients
    if not chat_clients: return
    text = json.dumps(message, ensure_ascii=False)
    dead = set()
    for client in list(chat_clients):
        try: await client.send_text(text)
        except Exception: dead.add(client)
    chat_clients -= dead

async def broadcast_online_users():
    users = list(ws_users.values())
    await broadcast_chat({"type": "online_users", "users": users})

def create_session(name, ip):
    token = secrets.token_urlsafe(32)
    sessions[token] = {"name": name, "ip": ip, "created": datetime.datetime.now()}
    return token

def _check_rate_limit(ip):
    now = datetime.datetime.now()
    attempts = _login_attempts.get(ip, [])
    attempts = [t for t in attempts if now - t < RATE_LIMIT_WINDOW]
    if len(attempts) >= RATE_LIMIT_MAX:
        _login_attempts[ip] = attempts; return False
    attempts.append(now); _login_attempts[ip] = attempts; return True

def get_session(token):
    if not token: return None
    sess = sessions.get(token)
    if not sess: return None
    if datetime.datetime.now() - sess["created"] > SESSION_TTL:
        sessions.pop(token, None); return None
    return sess

@app.get("/")
async def index():
    try:
        with open(Path(__file__).parent / "index.html", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>index.html not found</h1>", status_code=500)

@app.post("/auth/login")
async def auth_login(request: Request):
    ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(ip):
        return JSONResponse({"error": "로그인 시도가 너무 많습니다."}, status_code=429)
    try: body = await request.json()
    except Exception: return JSONResponse({"error": "잘못된 요청 형식"}, status_code=400)
    name = str(body.get("name", "")).strip()[:30]
    if not name: return JSONResponse({"error": "이름을 입력해주세요"}, status_code=400)
    token = create_session(name, ip)
    write_log("login", "", name, ip=ip, user=name)
    return JSONResponse({"token": token, "name": name})

@app.get("/auth/verify")
async def auth_verify(token: str = Query(None)):
    sess = get_session(token)
    if not sess: return JSONResponse({"valid": False}, status_code=401)
    return JSONResponse({"valid": True, "name": sess["name"]})

@app.delete("/auth/logout")
async def auth_logout(token: str = Query(None)):
    if token:
        sessions.pop(token, None)
    return JSONResponse({"ok": True})

def load_memos():
    if MEMO_FILE.exists():
        try: return json.loads(MEMO_FILE.read_text(encoding="utf-8"))
        except Exception: return {}
    return {}

def save_memos(memos):
    try: MEMO_FILE.write_text(json.dumps(memos, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError as e: logger.error(f"Memo save error: {e}")

@app.get("/memos")
async def get_memos(token: str = Query(None)):
    if not get_session(token): return JSONResponse({"error": "인증 필요"}, status_code=401)
    loop = asyncio.get_running_loop()
    memos = await loop.run_in_executor(None, load_memos)
    return JSONResponse(memos)

@app.post("/memos")
async def set_memo(request: Request, token: str = Query(None)):
    sess = get_session(token)
    if not sess: return JSONResponse({"error": "인증 필요"}, status_code=401)
    try: body = await request.json()
    except Exception: return JSONResponse({"error": "잘못된 요청"}, status_code=400)
    path = str(body.get("path", "")).strip()
    text = str(body.get("text", "")).strip()[:500]
    if not path: return JSONResponse({"error": "경로 필요"}, status_code=400)
    def _update_memo():
        memos = load_memos()
        if text: memos[path] = {"text": text, "author": sess["name"], "time": now_str()}
        else: memos.pop(path, None)
        save_memos(memos)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _update_memo)
    return JSONResponse({"ok": True})

@app.get("/search")
async def search_files(q: str = Query(...), token: str = Query(None)):
    if not get_session(token): return JSONResponse({"error": "인증 필요"}, status_code=401)
    q = q.strip()
    if not q: return JSONResponse({"results": []})
    q_lower = q.lower()
    def _do_search():
        found = []
        try:
            for p in UPLOAD_DIR.rglob("*"):
                if not p.is_file(): continue
                if q_lower in p.name.lower():
                    try:
                        stat = p.stat()
                        rel = str(p.relative_to(UPLOAD_DIR))
                        parent_rel = str(p.parent.relative_to(UPLOAD_DIR)) if p.parent != UPLOAD_DIR else ""
                        found.append({"name": p.name, "path": rel, "size": stat.st_size,
                                      "mtime": stat.st_mtime, "dir": parent_rel})
                        if len(found) >= 500: break
                    except OSError: pass
        except Exception as e: logger.error(f"Search error: {e}"); raise
        found.sort(key=lambda x: x["name"].lower())
        return found
    loop = asyncio.get_running_loop()
    try: results = await loop.run_in_executor(None, _do_search)
    except Exception: return JSONResponse({"error": "검색 오류"}, status_code=500)
    return JSONResponse({"results": results})

@app.get("/preview/{path:path}")
async def preview_file(path: str, token: str = Query(None)):
    if not get_session(token): return HTMLResponse("인증이 필요합니다", status_code=401)
    try: file_path = safe_path(UPLOAD_DIR, path)
    except ValueError: return HTMLResponse("허용되지 않은 경로", status_code=403)
    if not file_path.is_file(): return HTMLResponse("파일 없음", status_code=404)
    media_type, _ = mimetypes.guess_type(str(file_path))
    return FileResponse(file_path, media_type=media_type or "application/octet-stream")

@app.get("/download/{path:path}")
async def download_file(path: str, token: str = Query(None), request: Request = None):
    sess = get_session(token)
    if not sess: return HTMLResponse("인증이 필요합니다", status_code=401)
    try: file_path = safe_path(UPLOAD_DIR, path)
    except ValueError: return HTMLResponse("허용되지 않은 경로", status_code=403)
    if not file_path.is_file(): return HTMLResponse("파일 없음", status_code=404)
    size = file_path.stat().st_size
    filename = file_path.name
    ip = request.client.host if request and request.client else "unknown"
    write_log("download", path, filename, size, ip=ip, user=sess["name"])
    media_type, _ = mimetypes.guess_type(str(file_path))
    return FileResponse(file_path, media_type=media_type or "application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{url_quote(filename)}"})

@app.get("/zip/{path:path}")
async def zip_folder(path: str, token: str = Query(None)):
    if not get_session(token): return HTMLResponse("인증이 필요합니다", status_code=401)
    try: folder_path = safe_path(UPLOAD_DIR, path)
    except ValueError: return HTMLResponse("허용되지 않은 경로", status_code=403)
    if not folder_path.is_dir(): return HTMLResponse("폴더가 아닙니다", status_code=400)
    def _make_zip():
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for f in sorted(folder_path.rglob("*")):
                if f.is_file():
                    try: zf.write(f, f.relative_to(folder_path))
                    except OSError: pass
        return buf.getvalue()
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(None, _make_zip)
    zip_name = folder_path.name + ".zip"
    return Response(content=data, media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{url_quote(zip_name)}"})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str = Query(None)):
    sess = get_session(token)
    if not sess:
        await websocket.accept(); await websocket.close(code=4001); return
    await websocket.accept()
    chat_clients.add(websocket)
    client_name = sess["name"]
    client_ip   = websocket.client.host if websocket.client else "unknown"
    ws_users[websocket] = client_name
    now = datetime.datetime.now()
    await broadcast_chat({"type": "chat_system",
        "text": f"{client_name}님이 입장했습니다  ·  현재 {len(chat_clients)}명 접속 중",
        "date": now.strftime("%Y-%m-%d"), "time": now.strftime("%H:%M:%S")})
    await broadcast_online_users()
    try:
        while True:
            try: msg = await websocket.receive()
            except WebSocketDisconnect: break
            if msg.get("type") == "websocket.disconnect": break
            raw = msg.get("text")
            if raw is None: continue
            try:
                cmd = json.loads(raw)
                if not isinstance(cmd, dict): raise ValueError
            except (json.JSONDecodeError, ValueError):
                await _err(websocket, "잘못된 요청 형식"); continue
            t = cmd.get("type", "")
            try:
                if   t == "tree":         await handle_tree(websocket)
                elif t == "list":         await handle_list(websocket, cmd.get("path", ""))
                elif t == "mkdir":        await handle_mkdir(websocket, cmd, client_ip, client_name)
                elif t == "upload":       await handle_upload(websocket, cmd, client_ip, client_name)
                elif t == "download":     await handle_download(websocket, cmd, client_ip, client_name)
                elif t == "delete":       await handle_delete(websocket, cmd, client_ip, client_name)
                elif t == "rename":       await handle_rename(websocket, cmd)
                elif t == "move":         await handle_move(websocket, cmd, client_ip, client_name)
                elif t == "get_logs":     await handle_get_logs(websocket, cmd.get("limit", 300))
                elif t == "save":         await handle_save(websocket, cmd, client_ip, client_name)
                elif t == "copy":         await handle_copy(websocket, cmd, client_ip, client_name)
                elif t == "trash_list":   await handle_trash_list(websocket)
                elif t == "restore":      await handle_restore(websocket, cmd, client_ip, client_name)
                elif t == "trash_delete": await handle_trash_delete(websocket, cmd)
                elif t == "trash_clear":  await handle_trash_clear(websocket)
                elif t == "chat_send":    await handle_chat_send(websocket, cmd, client_ip, client_name)
                elif t == "chat_history": await handle_chat_history(websocket, cmd.get("limit", 100))
                elif t == "ping":         await websocket.send_text('{"type":"pong"}')
                else: await _err(websocket, f"알 수 없는 명령: {t!r}")
            except WebSocketDisconnect: break
            except asyncio.CancelledError: raise
            except Exception as e:
                logger.exception(f"Handler error [{t!r}] from {client_name}: {e}")
                await _err(websocket, "서버 내부 오류가 발생했습니다")
    except asyncio.CancelledError: pass
    finally:
        chat_clients.discard(websocket); ws_users.pop(websocket, None)
        now = datetime.datetime.now()
        await broadcast_chat({"type": "chat_system",
            "text": f"{client_name}님이 퇴장했습니다  ·  현재 {len(chat_clients)}명 접속 중",
            "date": now.strftime("%Y-%m-%d"), "time": now.strftime("%H:%M:%S")})
        await broadcast_online_users()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    base = Path(__file__).parent
    ssl_key  = base / "key.pem"
    ssl_cert = base / "cert.pem"
    use_ssl  = (not args.http) and ssl_key.exists() and ssl_cert.exists()
    uvicorn.run("server:app", host="0.0.0.0", port=args.port, reload=False,
        ws_ping_interval=30, ws_ping_timeout=10,
        ssl_keyfile=str(ssl_key) if use_ssl else None,
        ssl_certfile=str(ssl_cert) if use_ssl else None)
