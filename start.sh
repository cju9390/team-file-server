#!/bin/bash
# 파일 서버 + Cloudflare 터널 시작 스크립트
cd "$(dirname "$0")"

PORT=8765

if command -v cloudflared &>/dev/null; then
    CF="cloudflared"
elif [ -f ./cloudflared ]; then
    CF="./cloudflared"
else
    echo "✗ cloudflared가 없습니다."
    echo "  curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o cloudflared && chmod +x cloudflared"
    exit 1
fi

cleanup() {
    echo "► 종료 중..."
    [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null
    [ -n "$TUNNEL_PID" ] && kill "$TUNNEL_PID" 2>/dev/null
    jobs -p | xargs -r kill 2>/dev/null
    exit 0
}
trap cleanup SIGINT SIGTERM EXIT

if lsof -ti tcp:$PORT &>/dev/null; then
    echo "► 포트 $PORT 사용 중인 프로세스 종료..."
    lsof -ti tcp:$PORT | xargs kill -9 2>/dev/null
    sleep 1
fi

echo "► 파일 서버 시작 (포트 $PORT, HTTP 모드)..."
python3 server.py --http --port $PORT &>/tmp/fileserver.log &
SERVER_PID=$!
sleep 2

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "✗ 서버 시작 실패. 로그:"
    cat /tmp/fileserver.log; exit 1
fi
echo "  서버 실행 중 (PID: $SERVER_PID)"

echo ""
echo "► Cloudflare 터널 연결 중..."

"$CF" tunnel --url "http://localhost:$PORT" 2>&1 | tee /tmp/cf_tunnel.log | \
while IFS= read -r line; do
    if echo "$line" | grep -q "trycloudflare.com"; then
        URL=$(echo "$line" | grep -oE 'https://[a-zA-Z0-9-]+\.trycloudflare\.com')
        if [ -n "$URL" ]; then
            echo ""
            echo "┌──────────────────────────────────────────────────────┐"
            echo "│                  접속 정보                            │"
            echo "├──────────────────────────────────────────────────────┤"
            printf "│  공개 URL : %-41s│\n" "$URL"
            printf "│  LAN  URL : %-41s│\n" "http://$(hostname -I | awk '{print $1}'):$PORT"
            echo "└──────────────────────────────────────────────────────┘"
        fi
    else
        if echo "$line" | grep -qiE "ERR|error|warn|failed|panic"; then
            echo "$line"
        fi
    fi
done

echo "터널이 종료되었습니다."
