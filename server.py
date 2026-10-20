
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
MAX_EDIT_SIZE     = 5 * 1024 * 1024           # 5 MB (인라인 편집 제한)
MAX_FILENAME      = 255
SESSION_TTL       = datetime.timedelta(hours=24)
RATE_LIMIT_MAX    = 10
RATE_LIMIT_WINDOW = datetime.timedelta(minutes=5)

# 전역 상태 (asyncio 단일 스레드 — 락 불필요)
chat_clients:    set["WebSocket"]       = set()
ws_users:        dict["WebSocket", str] = {}
sessions:        dict[str, dict]        = {}
_login_attempts: dict[str, list]        = {}   # ip → [datetime, ...]
_log_queue:      "asyncio.Queue | None" = None
_chat_queue:     "asyncio.Queue | None" = None

@asynccontextmanager
async def lifespan(app):
    global _log_queue, _chat_queue

    _log_queue  = asyncio.Queue()
    _chat_queue = asyncio.Queue()

    async def _file_writer(queue: asyncio.Queue, path: Path):
        while True:
            entry = await queue.get()
            if entry is None:
                queue.task_done()
                break
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
            if expired:
                logger.info(f"만료 세션 {len(expired)}개 정리")
            # rate limit 기록도 주기적으로 정리
            cutoff = now - RATE_LIMIT_WINDOW
            for ip in list(_login_attempts.keys()):
                _login_attempts[ip] = [t for t in _login_attempts[ip] if t > cutoff]
                if not _login_attempts[ip]:
                    del _login_attempts[ip]

    log_task    = asyncio.create_task(_file_writer(_log_queue,  LOG_FILE))
    chat_task   = asyncio.create_task(_file_writer(_chat_queue, CHAT_LOG))
    cleaner_task = asyncio.create_task(_session_cleaner())
    yield
    await _log_queue.put(None)
    await _chat_queue.put(None)
    await log_task
    await chat_task
    cleaner_task.cancel()
    try:
        await cleaner_task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)

# ── OpenSlide / WSI ───────────────────────────────────────
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
_wsi_tls          = threading.local()   # per-thread slide cache

def _wsi_get_dz(file_path_str: str) -> "DeepZoomGenerator":
    """Thread-local DeepZoomGenerator cache — each worker thread keeps its own handles."""
    if not hasattr(_wsi_tls, "cache"):
        _wsi_tls.cache = {}
    cache  = _wsi_tls.cache
    mtime  = Path(file_path_str).stat().st_mtime
    entry  = cache.get(file_path_str)
    if entry and entry[0] == mtime:
        return entry[1]
    slide  = openslide.OpenSlide(file_path_str)
    dz     = DeepZoomGenerator(slide, tile_size=_WSI_TILE_SIZE, overlap=_WSI_TILE_OVERLAP)
    cache[file_path_str] = (mtime, dz)
    return dz


# ── Helpers ──────────────────────────────────────────────
def now_str() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def fmt_size(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.0f} {unit}"
        n //= 1024
    return f"{n:.1f} TB"

def _unique_path(dest_dir: Path, filename: str) -> Path:
    """이미 존재하는 파일이면 stem(N).suffix 형태로 번호를 붙여 반환."""
    dest = dest_dir / filename
    if not dest.exists():
        return dest
    stem   = Path(filename).stem
    suffix = Path(filename).suffix
    n = 1
    while True:
        candidate = dest_dir / f"{stem}({n}){suffix}"
        if not candidate.exists():
            return candidate
        n += 1

def safe_path(base: Path, rel: str) -> Path:
    """경로 순회 공격 방지."""
    target = (base / rel).resolve()
    if not str(target).startswith(str(base.resolve())):
        raise ValueError("허용되지 않은 경로입니다")
    return target

def write_log(action: str, path: str, name: str,
              size: int = None, ip: str = None, user: str = None):
    entry = {"time": now_str(), "action": action, "name": name, "path": path}
    if size is not None: entry["size"] = size
    if ip:   entry["ip"]   = ip
    if user: entry["user"] = user
    if _log_queue is not None:
        _log_queue.put_nowait(entry)
    else:
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except OSError as e:
            logger.error(f"Log write failed: {e}")

def write_chat_log(entry: dict):
    if _chat_queue is not None:
        _chat_queue.put_nowait(entry)
    else:
        try:
            with open(CHAT_LOG, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except OSError as e:
            logger.error(f"Chat log write failed: {e}")

def dir_tree(path: Path, base: Path) -> dict:
    rel = str(path.relative_to(base)) if path != base else ""
    node = {"name": path.name or "홈", "path": rel, "children": []}
    try:
        for child in sorted(path.iterdir()):
            if child.is_dir() and not child.name.startswith('.'):
                node["children"].append(dir_tree(child, base))
    except PermissionError:
        pass
    return node

async def _err(ws: WebSocket, msg: str):
    """연결이 끊겼어도 안전하게 에러 전송."""
    try:
        await ws.send_text(json.dumps({"type": "error", "msg": msg}))
    except Exception:
        pass

async def broadcast_chat(message: dict):
    global chat_clients
    if not chat_clients:
        return
    text = json.dumps(message, ensure_ascii=False)
    dead = set()
    for client in list(chat_clients):
        try:
            await client.send_text(text)
        except Exception:
            dead.add(client)
    chat_clients -= dead

async def broadcast_online_users():
    users = list(ws_users.values())
    await broadcast_chat({"type": "online_users", "users": users})


# ── Session ──────────────────────────────────────────────
def create_session(name: str, ip: str) -> str:
    token = secrets.token_urlsafe(32)
    sessions[token] = {
        "name":    name,
        "ip":      ip,
        "created": datetime.datetime.now(),
    }
    return token

def _check_rate_limit(ip: str) -> bool:
    """5분 내 10회 초과 시 False 반환."""
    now = datetime.datetime.now()
    attempts = _login_attempts.get(ip, [])
    attempts = [t for t in attempts if now - t < RATE_LIMIT_WINDOW]
    if len(attempts) >= RATE_LIMIT_MAX:
        _login_attempts[ip] = attempts
        return False
    attempts.append(now)
    _login_attempts[ip] = attempts
    return True

def get_session(token: str | None) -> dict | None:
    if not token:
        return None
    sess = sessions.get(token)
    if not sess:
        return None
    if datetime.datetime.now() - sess["created"] > SESSION_TTL:
        sessions.pop(token, None)
        return None
    return sess


# ── HTTP Routes ───────────────────────────────────────────
@app.get("/")
async def index():
    try:
        with open(Path(__file__).parent / "index.html", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>index.html not found</h1>", status_code=500)

@app.get("/manifest.json")
async def manifest():
    p = Path(__file__).parent / "manifest.json"
    if not p.exists():
        return JSONResponse({"error": "not found"}, status_code=404)
    return HTMLResponse(p.read_text(encoding="utf-8"), media_type="application/manifest+json")

@app.get("/sw.js")
async def service_worker():
    p = Path(__file__).parent / "sw.js"
    if not p.exists():
        return HTMLResponse("// sw not found", status_code=404, media_type="application/javascript")
    return HTMLResponse(p.read_text(encoding="utf-8"), media_type="application/javascript",
                        headers={"Service-Worker-Allowed": "/"})

@app.get("/icon.svg")
async def icon():
    p = Path(__file__).parent / "icon.svg"
    if not p.exists():
        return HTMLResponse("", status_code=404)
    return HTMLResponse(p.read_text(encoding="utf-8"), media_type="image/svg+xml")

@app.post("/auth/login")
async def auth_login(request: Request):
    ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(ip):
        logger.warning(f"Rate limit exceeded: ip={ip}")
        return JSONResponse(
            {"error": "로그인 시도가 너무 많습니다. 잠시 후 다시 시도해주세요."},
            status_code=429,
        )

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "잘못된 요청 형식"}, status_code=400)

    name = str(body.get("name", "")).strip()[:30]
    if not name:
        return JSONResponse({"error": "이름을 입력해주세요"}, status_code=400)

    token = create_session(name, ip)
    logger.info(f"LOGIN  name={name!r} ip={ip}")
    write_log("login", "", name, ip=ip, user=name)
    return JSONResponse({"token": token, "name": name})

@app.get("/auth/verify")
async def auth_verify(token: str = Query(None)):
    sess = get_session(token)
    if not sess:
        return JSONResponse({"valid": False}, status_code=401)
    return JSONResponse({"valid": True, "name": sess["name"]})

@app.delete("/auth/logout")
async def auth_logout(token: str = Query(None)):
    if token:
        name = sessions.get(token, {}).get("name", "?")
        sessions.pop(token, None)
        logger.info(f"LOGOUT name={name!r}")
    return JSONResponse({"ok": True})


# ── 메모 ──────────────────────────────────────────────────
def load_memos() -> dict:
    if MEMO_FILE.exists():
        try:
            return json.loads(MEMO_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_memos(memos: dict):
    try:
        MEMO_FILE.write_text(json.dumps(memos, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError as e:
        logger.error(f"Memo save error: {e}")

@app.get("/memos")
async def get_memos(token: str = Query(None)):
    if not get_session(token):
        return JSONResponse({"error": "인증 필요"}, status_code=401)
    loop = asyncio.get_running_loop()
    memos = await loop.run_in_executor(None, load_memos)
    return JSONResponse(memos)

@app.post("/memos")
async def set_memo(request: Request, token: str = Query(None)):
    sess = get_session(token)
    if not sess:
        return JSONResponse({"error": "인증 필요"}, status_code=401)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "잘못된 요청"}, status_code=400)
    path = str(body.get("path", "")).strip()
    text = str(body.get("text", "")).strip()[:500]
    if not path:
        return JSONResponse({"error": "경로 필요"}, status_code=400)

    def _update_memo():
        memos = load_memos()
        if text:
            memos[path] = {"text": text, "author": sess["name"], "time": now_str()}
        else:
            memos.pop(path, None)
        save_memos(memos)

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _update_memo)
    return JSONResponse({"ok": True})


@app.get("/search")
async def search_files(q: str = Query(...), token: str = Query(None)):
    if not get_session(token):
        return JSONResponse({"error": "인증 필요"}, status_code=401)
    q = q.strip()
    if not q:
        return JSONResponse({"results": []})

    q_lower = q.lower()

    def _do_search() -> list:
        found = []
        try:
            for p in UPLOAD_DIR.rglob("*"):
                if not p.is_file():
                    continue
                if q_lower in p.name.lower():
                    try:
                        stat = p.stat()
                        rel = str(p.relative_to(UPLOAD_DIR))
                        parent_rel = str(p.parent.relative_to(UPLOAD_DIR)) if p.parent != UPLOAD_DIR else ""
                        found.append({
                            "name":  p.name,
                            "path":  rel,
                            "size":  stat.st_size,
                            "mtime": stat.st_mtime,
                            "dir":   parent_rel,
                        })
                        if len(found) >= 500:
                            break
                    except OSError:
                        pass
        except Exception as e:
            logger.error(f"Search error: {e}")
            raise
        found.sort(key=lambda x: x["name"].lower())
        return found

    loop = asyncio.get_running_loop()
    try:
        results = await loop.run_in_executor(None, _do_search)
    except Exception:
        return JSONResponse({"error": "검색 오류"}, status_code=500)

    return JSONResponse({"results": results})


@app.get("/preview/{path:path}")
async def preview_file(path: str, token: str = Query(None)):
    if not get_session(token):
        return HTMLResponse("인증이 필요합니다", status_code=401)
    try:
        file_path = safe_path(UPLOAD_DIR, path)
    except ValueError:
        return HTMLResponse("허용되지 않은 경로", status_code=403)
    if not file_path.is_file():
        return HTMLResponse("파일 없음", status_code=404)
    try:
        media_type, _ = mimetypes.guess_type(str(file_path))
        return FileResponse(file_path, media_type=media_type or "application/octet-stream")
    except Exception as e:
        logger.error(f"Preview serve error: {e}")
        return HTMLResponse("파일 전송 오류", status_code=500)


@app.get("/download/{path:path}")
async def download_file(path: str, token: str = Query(None), request: Request = None):
    sess = get_session(token)
    if not sess:
        return HTMLResponse("인증이 필요합니다", status_code=401)
    try:
        file_path = safe_path(UPLOAD_DIR, path)
    except ValueError:
        return HTMLResponse("허용되지 않은 경로", status_code=403)
    if not file_path.is_file():
        return HTMLResponse("파일 없음", status_code=404)
    try:
        size = file_path.stat().st_size
        filename = file_path.name
        ip = request.client.host if request and request.client else "unknown"
        write_log("download", path, filename, size, ip=ip, user=sess["name"])
        logger.info(f"DOWNLOAD {filename} ({fmt_size(size)}) by {sess['name']}")
        media_type, _ = mimetypes.guess_type(str(file_path))
        return FileResponse(
            file_path,
            media_type=media_type or "application/octet-stream",
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{url_quote(filename)}"
            },
        )
    except Exception as e:
        logger.error(f"Download serve error: {e}")
        return HTMLResponse("파일 전송 오류", status_code=500)


@app.get("/zip/{path:path}")
async def zip_folder(path: str, token: str = Query(None)):
    if not get_session(token):
        return HTMLResponse("인증이 필요합니다", status_code=401)
    try:
        folder_path = safe_path(UPLOAD_DIR, path)
    except ValueError:
        return HTMLResponse("허용되지 않은 경로", status_code=403)
    if not folder_path.is_dir():
        return HTMLResponse("폴더가 아닙니다", status_code=400)

    def _make_zip():
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for f in sorted(folder_path.rglob("*")):
                if f.is_file():
                    try:
                        zf.write(f, f.relative_to(folder_path))
                    except OSError:
                        pass
        return buf.getvalue()

    loop = asyncio.get_running_loop()
    try:
        data = await loop.run_in_executor(None, _make_zip)
    except Exception as e:
        logger.error(f"ZIP error [{path}]: {e}")
        return HTMLResponse("ZIP 생성 오류", status_code=500)

    zip_name = folder_path.name + ".zip"
    return Response(
        content=data,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{url_quote(zip_name)}"},
    )


@app.get("/csv")
async def preview_csv_paged(
    path:   str = Query(...),
    offset: int = Query(0,   ge=0),
    limit:  int = Query(200, ge=1, le=1000),
    token:  str = Query(None),
):
    if not get_session(token):
        return JSONResponse({"error": "인증 필요"}, status_code=401)
    try:
        file_path = safe_path(UPLOAD_DIR, path)
    except ValueError:
        return JSONResponse({"error": "허용되지 않은 경로"}, status_code=403)
    if not file_path.is_file():
        return JSONResponse({"error": "파일 없음"}, status_code=404)

    ext = file_path.suffix.lower()

    if ext == ".parquet":
        def _read_parquet():
            try:
                import pandas as pd
            except ImportError:
                raise RuntimeError("pandas_missing")
            df = pd.read_parquet(file_path)
            header = list(df.columns.astype(str))
            total  = len(df)
            chunk  = df.iloc[offset: offset + limit + 1].astype(str)
            has_more = len(chunk) > limit
            rows = chunk.iloc[:limit].values.tolist()
            return header, rows, has_more, total

        loop = asyncio.get_running_loop()
        try:
            header, rows, has_more, total = await loop.run_in_executor(None, _read_parquet)
        except RuntimeError as e:
            if "pandas_missing" in str(e):
                return JSONResponse({"error": "pandas가 설치되지 않았습니다"}, status_code=501)
            return JSONResponse({"error": str(e)}, status_code=500)
        except Exception as e:
            logger.error(f"Parquet read error [{path}]: {e}")
            return JSONResponse({"error": f"파일 읽기 오류: {e}"}, status_code=500)
    else:
        def _read_csv():
            with open(file_path, "r", encoding="utf-8", errors="replace", newline="") as f:
                reader = csv.reader(f)
                try:
                    header = next(reader)
                except StopIteration:
                    return [], [], False
                for _ in itertools.islice(reader, offset):
                    pass
                chunk = list(itertools.islice(reader, limit + 1))
            has_more = len(chunk) > limit
            return header, chunk[:limit], has_more

        loop = asyncio.get_running_loop()
        try:
            header, rows, has_more = await loop.run_in_executor(None, _read_csv)
        except Exception as e:
            logger.error(f"CSV read error [{path}]: {e}")
            return JSONResponse({"error": f"파일 읽기 오류: {e}"}, status_code=500)
        total = None

    return JSONResponse({
        "headers":  header,
        "rows":     rows,
        "offset":   offset,
        "limit":    limit,
        "has_more": has_more,
        "total":    total if ext == ".parquet" else None,
    })


@app.post("/zip-batch")
async def zip_batch(request: Request, token: str = Query(None)):
    if not get_session(token):
        return HTMLResponse("인증이 필요합니다", status_code=401)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "잘못된 요청"}, status_code=400)

    raw_paths = body.get("paths", [])
    if not isinstance(raw_paths, list) or not raw_paths:
        return JSONResponse({"error": "paths가 필요합니다"}, status_code=400)
    if len(raw_paths) > 500:
        return JSONResponse({"error": "최대 500개까지 가능합니다"}, status_code=400)

    valid = []
    for p in raw_paths:
        try:
            fp = safe_path(UPLOAD_DIR, str(p).strip())
            if fp.exists():
                valid.append(fp)
        except ValueError:
            pass

    if not valid:
        return JSONResponse({"error": "유효한 파일이 없습니다"}, status_code=400)

    def _make_zip():
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for fp in valid:
                if fp.is_file():
                    try:
                        zf.write(fp, fp.name)
                    except OSError:
                        pass
                elif fp.is_dir():
                    for f in sorted(fp.rglob("*")):
                        if f.is_file():
                            try:
                                zf.write(f, f.relative_to(fp.parent))
                            except OSError:
                                pass
        return buf.getvalue()

    loop = asyncio.get_running_loop()
    try:
        data = await loop.run_in_executor(None, _make_zip)
    except Exception as e:
        logger.error(f"ZIP batch error: {e}")
        return HTMLResponse("ZIP 생성 오류", status_code=500)

    return Response(
        content=data,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename*=UTF-8''batch.zip"},
    )


# ── WSI (Whole Slide Image) ───────────────────────────────
@app.get("/wsi/dzi")
async def wsi_dzi(path: str = Query(...), token: str = Query(None)):
    if not OPENSLIDE_OK:
        return JSONResponse({"error": "openslide가 설치되지 않았습니다"}, status_code=501)
    if not get_session(token):
        return HTMLResponse("인증 필요", status_code=401)
    try:
        file_path = safe_path(UPLOAD_DIR, path)
    except ValueError:
        return HTMLResponse("허용되지 않은 경로", status_code=403)
    if not file_path.is_file():
        return HTMLResponse("파일 없음", status_code=404)

    loop = asyncio.get_running_loop()
    try:
        def _get_info():
            dz = _wsi_get_dz(str(file_path))
            w, h = dz.level_dimensions[-1]
            return w, h
        w, h = await loop.run_in_executor(_wsi_executor, _get_info)
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Image xmlns="http://schemas.microsoft.com/deepzoom/2008"'
            f' Format="jpeg" Overlap="{_WSI_TILE_OVERLAP}" TileSize="{_WSI_TILE_SIZE}">'
            f'<Size Width="{w}" Height="{h}"/></Image>'
        )
        return Response(content=xml, media_type="application/xml",
                        headers={"Cache-Control": "no-cache"})
    except openslide.OpenSlideError as e:
        return HTMLResponse(f"슬라이드 열기 실패: {e}", status_code=422)
    except Exception as e:
        logger.error(f"WSI DZI error [{path}]: {e}")
        return HTMLResponse("슬라이드 처리 오류", status_code=500)


@app.get("/wsi/tile")
async def wsi_tile(
    path:  str = Query(...),
    level: int = Query(...),
    x:     int = Query(...),
    y:     int = Query(...),
    token: str = Query(None),
):
    if not OPENSLIDE_OK:
        return JSONResponse({"error": "openslide 없음"}, status_code=501)
    if not get_session(token):
        return HTMLResponse("인증 필요", status_code=401)
    try:
        file_path = safe_path(UPLOAD_DIR, path)
    except ValueError:
        return HTMLResponse("허용되지 않은 경로", status_code=403)
    if not file_path.is_file():
        return HTMLResponse("파일 없음", status_code=404)

    loop = asyncio.get_running_loop()
    try:
        def _read_tile():
            dz   = _wsi_get_dz(str(file_path))
            tile = dz.get_tile(level, (x, y))
            if tile.mode != "RGB":
                tile = tile.convert("RGB")
            buf = io.BytesIO()
            tile.save(buf, format="JPEG", quality=85)
            return buf.getvalue()

        data = await loop.run_in_executor(_wsi_executor, _read_tile)
        return Response(
            content=data,
            media_type="image/jpeg",
            headers={"Cache-Control": "max-age=86400", "X-Content-Type-Options": "nosniff"},
        )
    except (ValueError, IndexError):
        return HTMLResponse("타일 범위 초과", status_code=404)
    except openslide.OpenSlideError as e:
        logger.warning(f"WSI tile OpenSlide error [{path}] {level}/{x}/{y}: {e}")
        return HTMLResponse(f"타일 오류: {e}", status_code=422)
    except Exception as e:
        logger.error(f"WSI tile error [{path}] {level}/{x}/{y}: {e}")
        return HTMLResponse("타일 처리 오류", status_code=500)


# ── WebSocket ─────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str = Query(None)):
    # 인증 검사 — accept 후 4001로 종료해야 클라이언트가 코드 수신 가능
    sess = get_session(token)
    if not sess:
        await websocket.accept()
        await websocket.close(code=4001)
        return

    await websocket.accept()
    chat_clients.add(websocket)
    client_name = sess["name"]
    client_ip   = websocket.client.host if websocket.client else "unknown"
    ws_users[websocket] = client_name

    now = datetime.datetime.now()
    await broadcast_chat({
        "type": "chat_system",
        "text": f"{client_name}님이 입장했습니다  ·  현재 {len(chat_clients)}명 접속 중",
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
    })
    await broadcast_online_users()

    try:
        while True:
            # ── 수신 ──
            try:
                msg = await websocket.receive()
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.warning(f"WS recv error [{client_name}]: {e}")
                break

            if msg.get("type") == "websocket.disconnect":
                break

            # 바이너리 프레임은 업로드 실패 후 잔류 청크일 수 있으므로 조용히 무시
            raw = msg.get("text")
            if raw is None:
                logger.debug(f"Stray binary frame from {client_name}, ignoring")
                continue

            # ── JSON 파싱 ──
            try:
                cmd = json.loads(raw)
                if not isinstance(cmd, dict):
                    raise ValueError
            except (json.JSONDecodeError, ValueError):
                await _err(websocket, "잘못된 요청 형식 (JSON dict 필요)")
                continue

            t = cmd.get("type", "")

            # ── 명령 디스패치 ──
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
                else:
                    await _err(websocket, f"알 수 없는 명령: {t!r}")
            except WebSocketDisconnect:
                break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(f"Handler error [{t!r}] from {client_name}: {e}")
                await _err(websocket, "서버 내부 오류가 발생했습니다")

    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.exception(f"WS main loop error [{client_name}]: {e}")
    finally:
        chat_clients.discard(websocket)
        ws_users.pop(websocket, None)
        now = datetime.datetime.now()
        await broadcast_chat({
            "type": "chat_system",
            "text": f"{client_name}님이 퇴장했습니다  ·  현재 {len(chat_clients)}명 접속 중",
            "date": now.strftime("%Y-%m-%d"),
            "time": now.strftime("%H:%M:%S"),
        })
        await broadcast_online_users()


# ── File Handlers ─────────────────────────────────────────
async def handle_tree(ws: WebSocket):
    try:
        tree = dir_tree(UPLOAD_DIR, UPLOAD_DIR)
        await ws.send_text(json.dumps({"type": "tree", "tree": tree}))
    except Exception as e:
        logger.error(f"handle_tree: {e}")
        await _err(ws, "폴더 트리 로드 실패")


async def handle_list(ws: WebSocket, rel: str):
    try:
        folder = safe_path(UPLOAD_DIR, rel) if rel else UPLOAD_DIR
    except ValueError as e:
        await _err(ws, str(e)); return

    if not folder.exists():
        await _err(ws, "폴더가 존재하지 않습니다"); return
    if not folder.is_dir():
        await _err(ws, "경로가 폴더가 아닙니다"); return

    try:
        items = []
        for p in sorted(folder.iterdir(), key=lambda x: (x.is_file(), x.name.lower())):
            if p.name.startswith('.'):
                continue
            try:
                stat = p.stat()
                items.append({
                    "name":   p.name,
                    "path":   str(p.relative_to(UPLOAD_DIR)),
                    "is_dir": p.is_dir(),
                    "size":   stat.st_size if p.is_file() else None,
                    "mtime":  stat.st_mtime,
                })
            except OSError:
                pass  # 읽기 불가 항목 건너뜀
        await ws.send_text(json.dumps({"type": "list", "path": rel, "items": items}))
    except PermissionError:
        await _err(ws, "폴더 읽기 권한이 없습니다")
    except Exception as e:
        logger.error(f"handle_list: {e}")
        await _err(ws, "폴더 목록 로드 실패")


async def handle_mkdir(ws: WebSocket, cmd: dict, ip: str = "", user: str = ""):
    path_str = str(cmd.get("path", "")).strip()
    if not path_str:
        await _err(ws, "폴더 이름이 없습니다"); return
    try:
        folder = safe_path(UPLOAD_DIR, path_str)
    except ValueError as e:
        await _err(ws, str(e)); return

    try:
        folder.mkdir(parents=True, exist_ok=True)
        write_log("mkdir", path_str, folder.name, ip=ip, user=user)
        await ws.send_text(json.dumps({"type": "done", "action": "mkdir", "path": path_str}))
        await handle_tree(ws)
    except PermissionError:
        await _err(ws, "폴더 생성 권한이 없습니다")
    except OSError as e:
        await _err(ws, f"폴더 생성 실패: {e.strerror}")


async def handle_upload(ws: WebSocket, cmd: dict, ip: str, user: str):
    # ── 입력 검증 ──
    try:
        dest_dir = safe_path(UPLOAD_DIR, str(cmd.get("dir", "")))
    except ValueError as e:
        await _err(ws, str(e)); return

    raw_name = str(cmd.get("name", "")).strip()
    filename = Path(raw_name).name
    if not filename:
        await _err(ws, "파일 이름이 올바르지 않습니다"); return
    if len(filename) > MAX_FILENAME:
        await _err(ws, f"파일 이름이 너무 깁니다 (최대 {MAX_FILENAME}자)"); return

    total_size = cmd.get("size")
    if not isinstance(total_size, (int, float)) or total_size < 0:
        await _err(ws, "파일 크기 정보가 올바르지 않습니다"); return
    total_size = int(total_size)
    if total_size > MAX_UPLOAD:
        await _err(ws, f"파일이 너무 큽니다 (최대 {fmt_size(MAX_UPLOAD)})"); return

    # ── 디렉토리 준비 ──
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        await _err(ws, f"폴더 생성 실패: {e.strerror}"); return

    dest       = _unique_path(dest_dir, filename)
    final_name = dest.name
    renamed    = final_name != filename
    tmp        = dest_dir / f".{final_name}.tmp"  # 임시 파일에 먼저 기록 (원자적 저장)
    received   = 0
    sha        = hashlib.sha256()

    # ── 수신 ──
    try:
        async with aiofiles.open(tmp, "wb") as f:
            while received < total_size:
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=60)
                except asyncio.TimeoutError:
                    logger.warning(f"Upload timeout: {filename}")
                    tmp.unlink(missing_ok=True)
                    await _err(ws, "업로드 시간 초과 (60초)")
                    return
                except WebSocketDisconnect:
                    logger.info(f"Upload disconnected mid-way: {filename}")
                    tmp.unlink(missing_ok=True)
                    raise

                if msg.get("type") == "websocket.disconnect":
                    logger.info(f"Upload disconnected (disconnect frame): {filename}")
                    tmp.unlink(missing_ok=True)
                    raise WebSocketDisconnect()

                if "bytes" not in msg:
                    # 업로드 중 텍스트 프레임이 끼어든 경우 — 업로드 중단
                    logger.warning(f"Text frame during upload [{filename}]: {str(msg.get('text',''))[:80]}")
                    tmp.unlink(missing_ok=True)
                    await _err(ws, "업로드 중 다른 명령이 수신되어 취소되었습니다. 다시 시도해주세요.")
                    return

                chunk = msg["bytes"]

                await f.write(chunk)
                sha.update(chunk)
                received += len(chunk)

                try:
                    await ws.send_text(json.dumps({
                        "type":  "progress",
                        "value": round(received / total_size * 100, 1),
                        "name":  final_name,
                    }))
                except Exception:
                    pass  # 진행률 전송 실패는 치명적이지 않음

    except WebSocketDisconnect:
        raise
    except OSError as e:
        tmp.unlink(missing_ok=True)
        if e.errno == 28:
            await _err(ws, "디스크 공간이 부족합니다")
        else:
            logger.error(f"Upload write error [{filename}]: {e}")
            await _err(ws, f"파일 저장 실패: {e.strerror}")
        return
    except Exception as e:
        tmp.unlink(missing_ok=True)
        logger.exception(f"Upload unexpected error [{filename}]: {e}")
        raise

    # ── 체크섬 검증 ──
    actual   = sha.hexdigest()
    expected = cmd.get("sha256")
    if expected and expected != actual:
        tmp.unlink(missing_ok=True)
        await _err(ws, "파일 무결성 검사 실패 (체크섬 불일치)")
        return

    # ── 원자적 이동 ──
    try:
        tmp.rename(dest)
    except OSError as e:
        tmp.unlink(missing_ok=True)
        await _err(ws, f"파일 저장 실패: {e.strerror}")
        return

    rel_path = str(dest.relative_to(UPLOAD_DIR))
    write_log("upload", rel_path, final_name, received, ip, user)
    logger.info(f"UPLOAD {final_name} ({fmt_size(received)}) by {user}"
                + (f" [renamed from {filename}]" if renamed else ""))

    try:
        await ws.send_text(json.dumps({
            "type":    "done",      "action":  "upload",
            "name":    final_name,  "path":    rel_path,
            "size":    received,    "sha256":  actual,
            "renamed": renamed,     "original": filename if renamed else None,
        }))
    except Exception:
        pass


async def handle_download(ws: WebSocket, cmd: dict, ip: str, user: str):
    path_str = str(cmd.get("path", "")).strip()
    if not path_str:
        await _err(ws, "파일 경로가 없습니다"); return
    try:
        path = safe_path(UPLOAD_DIR, path_str)
    except ValueError as e:
        await _err(ws, str(e)); return

    if not path.exists():
        await _err(ws, "파일이 존재하지 않습니다"); return
    if not path.is_file():
        await _err(ws, "경로가 파일이 아닙니다"); return

    try:
        size = path.stat().st_size
    except OSError as e:
        await _err(ws, f"파일 정보 읽기 실패: {e.strerror}"); return

    sha = hashlib.sha256()
    try:
        await ws.send_text(json.dumps({"type": "meta", "name": path.name, "size": size}))
        async with aiofiles.open(path, "rb") as f:
            while chunk := await f.read(CHUNK_SIZE):
                sha.update(chunk)
                await ws.send_bytes(chunk)
    except WebSocketDisconnect:
        raise
    except PermissionError:
        await _err(ws, "파일 읽기 권한이 없습니다"); return
    except OSError as e:
        logger.error(f"Download read error [{path.name}]: {e}")
        await _err(ws, f"파일 전송 오류: {e.strerror}"); return

    write_log("download", path_str, path.name, size, ip, user)
    logger.info(f"DOWNLOAD {path.name} ({fmt_size(size)}) by {user}")

    try:
        await ws.send_text(json.dumps({
            "type":   "done",      "action": "download",
            "name":   path.name,   "sha256": sha.hexdigest(),
        }))
    except Exception:
        pass


async def handle_delete(ws: WebSocket, cmd: dict, ip: str = "", user: str = ""):
    path_str = str(cmd.get("path", "")).strip()
    if not path_str or path_str.startswith('.'):
        await _err(ws, "허용되지 않은 경로입니다"); return
    try:
        target = safe_path(UPLOAD_DIR, path_str)
    except ValueError as e:
        await _err(ws, str(e)); return

    if not target.exists():
        await _err(ws, "파일 또는 폴더가 존재하지 않습니다"); return

    try:
        is_dir = target.is_dir()
        uid = f"{int(datetime.datetime.now().timestamp()*1000)}_{secrets.token_hex(4)}"
        trash_target = TRASH_DIR / uid

        def _do_trash():
            size = (sum(f.stat().st_size for f in target.rglob('*') if f.is_file())
                    if is_dir else target.stat().st_size)
            meta = {
                "id": uid, "original_path": path_str,
                "name": target.name, "deleted_at": now_str(),
                "is_dir": is_dir, "size": size, "deleted_by": user,
            }
            shutil.move(str(target), str(trash_target))
            (TRASH_INFO / f"{uid}.json").write_text(
                json.dumps(meta, ensure_ascii=False), encoding="utf-8"
            )
            return size

        loop = asyncio.get_running_loop()
        size = await loop.run_in_executor(None, _do_trash)
        write_log("trash", path_str, target.name, size=size, ip=ip, user=user)
        await ws.send_text(json.dumps({"type": "done", "action": "delete", "path": path_str}))
        await handle_tree(ws)
    except PermissionError:
        await _err(ws, "삭제 권한이 없습니다")
    except OSError as e:
        logger.error(f"Trash error [{path_str}]: {e}")
        await _err(ws, f"삭제 실패: {e.strerror}")


async def handle_copy(ws: WebSocket, cmd: dict, ip: str = "", user: str = ""):
    path_str = str(cmd.get("path", "")).strip()
    if not path_str or path_str.startswith('.'):
        await _err(ws, "허용되지 않은 경로입니다"); return
    try:
        src = safe_path(UPLOAD_DIR, path_str)
    except ValueError as e:
        await _err(ws, str(e)); return
    if not src.exists():
        await _err(ws, "파일/폴더가 존재하지 않습니다"); return

    copy_name = (f"{src.name}(복사본)" if src.is_dir()
                 else f"{src.stem}(복사본){src.suffix}")
    dest = _unique_path(src.parent, copy_name)

    def _do_copy():
        if src.is_dir():
            shutil.copytree(str(src), str(dest))
        else:
            shutil.copy2(str(src), str(dest))

    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, _do_copy)
    except PermissionError:
        await _err(ws, "복사 권한이 없습니다"); return
    except OSError as e:
        logger.error(f"Copy error [{path_str}]: {e}")
        await _err(ws, f"복사 실패: {e.strerror}"); return

    new_path = str(dest.relative_to(UPLOAD_DIR))
    size = dest.stat().st_size if dest.is_file() else None
    write_log("copy", new_path, dest.name, size=size, ip=ip, user=user)
    logger.info(f"COPY {src.name} → {dest.name} by {user}")
    await ws.send_text(json.dumps({"type": "done", "action": "copy", "path": new_path, "name": dest.name}))
    await handle_tree(ws)


async def handle_save(ws: WebSocket, cmd: dict, ip: str = "", user: str = ""):
    path_str = str(cmd.get("path", "")).strip()
    content  = cmd.get("content", "")
    if not path_str or path_str.startswith('.'):
        await _err(ws, "허용되지 않은 경로입니다"); return
    if not isinstance(content, str):
        await _err(ws, "내용이 올바르지 않습니다"); return
    if len(content.encode("utf-8")) > MAX_EDIT_SIZE:
        await _err(ws, "내용이 너무 큽니다 (최대 5 MB)"); return
    try:
        file_path = safe_path(UPLOAD_DIR, path_str)
    except ValueError as e:
        await _err(ws, str(e)); return
    if not file_path.is_file():
        await _err(ws, "파일이 존재하지 않습니다"); return
    try:
        async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
            await f.write(content)
        size = len(content.encode("utf-8"))
        write_log("save", path_str, file_path.name, size=size, ip=ip, user=user)
        logger.info(f"SAVE {file_path.name} ({fmt_size(size)}) by {user}")
        await ws.send_text(json.dumps({"type": "done", "action": "save", "path": path_str}))
    except PermissionError:
        await _err(ws, "저장 권한이 없습니다")
    except OSError as e:
        logger.error(f"Save error [{path_str}]: {e}")
        await _err(ws, f"저장 실패: {e.strerror}")


async def handle_trash_list(ws: WebSocket):
    def _read():
        items = []
        for info_file in sorted(TRASH_INFO.glob("*.json"), reverse=True):
            try:
                meta = json.loads(info_file.read_text(encoding="utf-8"))
                if (TRASH_DIR / meta["id"]).exists():
                    items.append(meta)
                else:
                    info_file.unlink(missing_ok=True)
            except (json.JSONDecodeError, OSError, KeyError):
                pass
        return items

    loop = asyncio.get_running_loop()
    items = await loop.run_in_executor(None, _read)
    try:
        await ws.send_text(json.dumps({"type": "trash_items", "items": items}))
    except Exception:
        pass


async def handle_restore(ws: WebSocket, cmd: dict, ip: str = "", user: str = ""):
    uid = str(cmd.get("id", "")).strip()
    if not uid:
        await _err(ws, "복원 ID가 없습니다"); return
    info_file    = TRASH_INFO / f"{uid}.json"
    trash_target = TRASH_DIR  / uid
    if not info_file.exists() or not trash_target.exists():
        await _err(ws, "휴지통에서 항목을 찾을 수 없습니다"); return
    try:
        meta = json.loads(info_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        await _err(ws, "복원 정보 읽기 실패"); return
    try:
        dest = safe_path(UPLOAD_DIR, meta["original_path"])
    except (ValueError, KeyError):
        await _err(ws, "복원 경로가 올바르지 않습니다"); return

    def _do_restore():
        dest.parent.mkdir(parents=True, exist_ok=True)
        final = _unique_path(dest.parent, dest.name)
        shutil.move(str(trash_target), str(final))
        info_file.unlink(missing_ok=True)
        return str(final.relative_to(UPLOAD_DIR)), final.name

    loop = asyncio.get_running_loop()
    try:
        new_path, new_name = await loop.run_in_executor(None, _do_restore)
    except PermissionError:
        await _err(ws, "복원 권한이 없습니다"); return
    except OSError as e:
        logger.error(f"Restore error [{uid}]: {e}")
        await _err(ws, f"복원 실패: {e.strerror}"); return

    write_log("restore", new_path, new_name, ip=ip, user=user)
    logger.info(f"RESTORE {new_name} by {user}")
    await ws.send_text(json.dumps({"type": "done", "action": "restore", "id": uid, "path": new_path, "name": new_name}))
    await handle_tree(ws)
    await handle_trash_list(ws)


async def handle_trash_delete(ws: WebSocket, cmd: dict):
    uid = str(cmd.get("id", "")).strip()
    if not uid:
        await _err(ws, "ID가 없습니다"); return
    trash_target = TRASH_DIR  / uid
    info_file    = TRASH_INFO / f"{uid}.json"

    def _do_purge():
        if trash_target.is_dir():
            shutil.rmtree(str(trash_target), ignore_errors=True)
        elif trash_target.exists():
            trash_target.unlink(missing_ok=True)
        info_file.unlink(missing_ok=True)

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _do_purge)
    await ws.send_text(json.dumps({"type": "done", "action": "trash_delete", "id": uid}))
    await handle_trash_list(ws)


async def handle_trash_clear(ws: WebSocket):
    def _do_clear():
        for item in list(TRASH_DIR.iterdir()):
            if item.name == ".info":
                continue
            if item.is_dir():
                shutil.rmtree(str(item), ignore_errors=True)
            else:
                item.unlink(missing_ok=True)
        for f in TRASH_INFO.glob("*.json"):
            f.unlink(missing_ok=True)

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _do_clear)
    await ws.send_text(json.dumps({"type": "done", "action": "trash_clear"}))
    await handle_trash_list(ws)


async def handle_move(ws: WebSocket, cmd: dict, ip: str = "", user: str = ""):
    path_str = str(cmd.get("path", "")).strip()
    dest_str  = str(cmd.get("dest", "")).strip()
    if not path_str or path_str.startswith('.'):
        await _err(ws, "허용되지 않은 경로입니다"); return
    try:
        src      = safe_path(UPLOAD_DIR, path_str)
        dest_dir = safe_path(UPLOAD_DIR, dest_str) if dest_str else UPLOAD_DIR
    except ValueError as e:
        await _err(ws, str(e)); return
    if not src.exists():
        await _err(ws, "파일/폴더가 존재하지 않습니다"); return
    if not dest_dir.is_dir():
        await _err(ws, "대상 폴더가 존재하지 않습니다"); return
    if src.parent.resolve() == dest_dir.resolve():
        await _err(ws, "이미 같은 폴더에 있습니다"); return
    if src.is_dir():
        try:
            dest_dir.resolve().relative_to(src.resolve())
            await _err(ws, "폴더를 자신의 하위 폴더로 이동할 수 없습니다"); return
        except ValueError:
            pass

    dest = _unique_path(dest_dir, src.name)

    def _do_move():
        shutil.move(str(src), str(dest))

    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, _do_move)
    except PermissionError:
        await _err(ws, "이동 권한이 없습니다"); return
    except OSError as e:
        logger.error(f"Move error [{path_str}]: {e}")
        await _err(ws, f"이동 실패: {e.strerror}"); return

    new_path = str(dest.relative_to(UPLOAD_DIR))
    write_log("move", new_path, dest.name, ip=ip, user=user)
    logger.info(f"MOVE {src.name} → {dest_str or '홈'} by {user}")
    await ws.send_text(json.dumps({
        "type": "done", "action": "move",
        "path": new_path, "name": dest.name,
        "renamed": dest.name != src.name,
    }))
    await handle_tree(ws)


async def handle_rename(ws: WebSocket, cmd: dict):
    src_str = str(cmd.get("path", "")).strip()
    dst_str = str(cmd.get("new_path", "")).strip()
    if not src_str or not dst_str:
        await _err(ws, "경로가 없습니다"); return
    try:
        src = safe_path(UPLOAD_DIR, src_str)
        dst = safe_path(UPLOAD_DIR, dst_str)
    except ValueError as e:
        await _err(ws, str(e)); return

    if not src.exists():
        await _err(ws, "원본 파일/폴더가 존재하지 않습니다"); return
    if dst.exists():
        await _err(ws, "같은 이름의 파일/폴더가 이미 있습니다"); return

    try:
        src.rename(dst)
        await ws.send_text(json.dumps({"type": "done", "action": "rename", "path": dst_str}))
        await handle_tree(ws)
    except PermissionError:
        await _err(ws, "이름 변경 권한이 없습니다")
    except OSError as e:
        logger.error(f"Rename error [{src_str} → {dst_str}]: {e}")
        await _err(ws, f"이름 변경 실패: {e.strerror}")


async def handle_get_logs(ws: WebSocket, limit: int = 300):
    try:
        limit = max(1, min(int(limit), 1000))
    except (TypeError, ValueError):
        limit = 300

    def _read_logs():
        entries = []
        if not LOG_FILE.exists():
            return entries
        try:
            lines = LOG_FILE.read_text(encoding="utf-8").strip().splitlines()
            for line in reversed(lines[-limit:]):
                if not line.strip():
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        except OSError as e:
            logger.error(f"Log read error: {e}")
        return entries

    loop = asyncio.get_running_loop()
    entries = await loop.run_in_executor(None, _read_logs)
    try:
        await ws.send_text(json.dumps({"type": "logs", "entries": entries}))
    except Exception:
        pass


# ── Chat Handlers ─────────────────────────────────────────
async def handle_chat_send(ws: WebSocket, cmd: dict, ip: str, user: str):
    text = str(cmd.get("text", "")).strip()
    if not text:
        return
    if len(text) > 2000:
        await _err(ws, "메시지가 너무 깁니다 (최대 2000자)"); return

    now = datetime.datetime.now()
    msg = {
        "type": "chat_message",
        "user": user,   # 세션 이름 사용 (클라이언트 제공값 무시)
        "text": text,
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "ip":   ip,
    }
    write_chat_log({k: v for k, v in msg.items() if k != "type"})
    await broadcast_chat(msg)


async def handle_chat_history(ws: WebSocket, limit: int = 100):
    try:
        limit = max(1, min(int(limit), 500))
    except (TypeError, ValueError):
        limit = 100

    def _read_chat_history():
        messages = []
        if not CHAT_LOG.exists():
            return messages
        try:
            lines = CHAT_LOG.read_text(encoding="utf-8").strip().splitlines()
            for line in lines[-limit:]:
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                    entry["type"] = "chat_message"
                    messages.append(entry)
                except json.JSONDecodeError:
                    pass
        except OSError as e:
            logger.error(f"Chat history read error: {e}")
        return messages

    loop = asyncio.get_running_loop()
    messages = await loop.run_in_executor(None, _read_chat_history)
    try:
        await ws.send_text(json.dumps({"type": "chat_history", "messages": messages}))
    except Exception:
        pass


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true",
                        help="HTTP 모드 강제 (리버스 프록시/터널 뒤에서 사용)")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    base = Path(__file__).parent
    ssl_key  = base / "key.pem"
    ssl_cert = base / "cert.pem"
    use_ssl  = (not args.http) and ssl_key.exists() and ssl_cert.exists()

    if use_ssl:
        logger.info("HTTPS 모드 — cert.pem / key.pem 사용")
    elif args.http:
        logger.info("HTTP 모드 (--http 플래그) — 터널/프록시가 TLS 처리")
    else:
        logger.warning("cert.pem / key.pem 없음 — HTTP 모드로 시작")

    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=args.port,
        reload=False,
        ws_ping_interval=30,
        ws_ping_timeout=10,
        ssl_keyfile  = str(ssl_key)  if use_ssl else None,
        ssl_certfile = str(ssl_cert) if use_ssl else None,
    )
