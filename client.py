#!/usr/bin/env python3
"""
사용법:
  python client.py upload <파일경로>          # 업로드
  python client.py download <파일명>          # 다운로드
  python client.py list                       # 목록 조회
  python client.py delete <파일명>            # 삭제

  --host  서버 주소 (기본: localhost)
  --port  포트 (기본: 8765)
"""
import argparse
import asyncio
import hashlib
import json
import os
import sys
from pathlib import Path

import websockets

CHUNK_SIZE = 64 * 1024


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            h.update(chunk)
    return h.hexdigest()


async def upload(ws, path: Path):
    size = path.stat().st_size
    print(f"업로드: {path.name} ({size:,} bytes) — SHA256 계산 중...")
    checksum = sha256_file(path)

    await ws.send(json.dumps({"type": "upload", "name": path.name, "size": size, "sha256": checksum}))

    sent = 0
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            await ws.send(chunk)
            sent += len(chunk)
            pct = sent / size * 100
            print(f"\r  {pct:.1f}%  {sent:,}/{size:,} bytes", end="", flush=True)
            msg = json.loads(await ws.recv())
            if msg["type"] == "error":
                print(f"\n오류: {msg['msg']}")
                return

    print()
    result = json.loads(await ws.recv())
    if result["type"] == "done":
        print(f"완료: {result['name']}  sha256={result['sha256']}")
    else:
        print(f"실패: {result}")


async def download(ws, name: str, out_dir: Path = Path(".")):
    await ws.send(json.dumps({"type": "download", "name": name}))

    meta = json.loads(await ws.recv())
    if meta["type"] == "error":
        print(f"오류: {meta['msg']}")
        return

    total = meta["size"]
    dest = out_dir / name
    received = 0
    sha = hashlib.sha256()

    with open(dest, "wb") as f:
        while True:
            data = await ws.recv()
            if isinstance(data, str):
                msg = json.loads(data)
                if msg["type"] == "done":
                    print(f"\n완료: {dest}  sha256={msg['sha256']}")
                    break
                elif msg["type"] == "error":
                    print(f"\n오류: {msg['msg']}")
                    dest.unlink(missing_ok=True)
                    break
            else:
                f.write(data)
                sha.update(data)
                received += len(data)
                pct = received / total * 100 if total else 0
                print(f"\r  {pct:.1f}%  {received:,}/{total:,} bytes", end="", flush=True)


async def list_files(ws):
    await ws.send(json.dumps({"type": "list"}))
    msg = json.loads(await ws.recv())
    files = msg.get("files", [])
    if not files:
        print("파일 없음")
        return
    print(f"{'파일명':<40} {'크기':>12}")
    print("-" * 54)
    for f in files:
        print(f"{f['name']:<40} {f['size']:>12,}")


async def delete(ws, name: str):
    await ws.send(json.dumps({"type": "delete", "name": name}))
    msg = json.loads(await ws.recv())
    if msg["type"] == "done":
        print(f"삭제됨: {msg['name']}")
    else:
        print(f"오류: {msg['msg']}")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["upload", "download", "list", "delete"])
    parser.add_argument("target", nargs="?")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    url = f"ws://{args.host}:{args.port}/ws"

    async with websockets.connect(url) as ws:
        if args.action == "upload":
            if not args.target:
                sys.exit("파일 경로를 지정하세요")
            await upload(ws, Path(args.target))
        elif args.action == "download":
            if not args.target:
                sys.exit("파일명을 지정하세요")
            await download(ws, args.target)
        elif args.action == "list":
            await list_files(ws)
        elif args.action == "delete":
            if not args.target:
                sys.exit("파일명을 지정하세요")
            await delete(ws, args.target)


if __name__ == "__main__":
    asyncio.run(main())
