#!/bin/bash
# 파일 서버 + Cloudflare 터널 시작 스크립트
cd "$(dirname "$0")"

PORT=8765

# ── cloudflared 경로 확인 ────────────────────────────────
if command -v cloudflared &>/dev/null; then
    CF="cloudflared"
elif [ -f ./cloudflared ]; then
    CF="./cloudflared"
else
    echo "✗ cloudflared가 없습니다."
    echo "  다음 명령으로 다운로드하세요:"
    echo "  curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o cloudflared && chmod +x cloudflared"
    exit 1
fi

# ── 정리 함수 ────────────────────────────────────────────
cleanup() {
    echo ""
    echo "► 종료 중..."
    [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null
    [ -n "$TUNNEL_PID" ] && kill "$TUNNEL_PID" 2>/dev/null
    # 자식 프로세스 포함 정리
    jobs -p | xargs -r kill 2>/dev/null
    exit 0
}
trap cleanup SIGINT SIGTERM EXIT

# ── 기존 프로세스 정리 ────────────────────────────────────
if lsof -ti tcp:$PORT &>/dev/null; then
    echo "► 포트 $PORT 사용 중인 프로세스 종료..."
    lsof -ti tcp:$PORT | xargs kill -9 2>/dev/null
    sleep 1
fi

# ── 파일 서버 시작 (HTTP 모드) ────────────────────────────
echo "► 파일 서버 시작 (포트 $PORT, HTTP 모드)..."
python3 server.py --http --port $PORT &>/tmp/fileserver.log &
SERVER_PID=$!
sleep 2

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "✗ 서버 시작 실패. 로그:"
    cat /tmp/fileserver.log
    exit 1
fi
echo "  서버 실행 중 (PID: $SERVER_PID)"

# ── Cloudflare 터널 시작 ──────────────────────────────────
echo ""
echo "► Cloudflare 터널 연결 중..."
echo "  (URL이 아래에 표시됩니다 — 잠시 기다리세요)"
echo ""

# 터널 로그를 파일에도 저장하면서 터미널에 출력
"$CF" tunnel --url "http://localhost:$PORT" 2>&1 | tee /tmp/cf_tunnel.log | \
while IFS= read -r line; do
    # URL 감지 시 강조 출력
    if echo "$line" | grep -q "trycloudflare.com"; then
        URL=$(echo "$line" | grep -oE 'https://[a-zA-Z0-9-]+\.trycloudflare\.com')
        if [ -n "$URL" ]; then
            echo ""
            echo "┌──────────────────────────────────────────────────────┐"
            echo "│                  접속 정보                            │"
            echo "├──────────────────────────────────────────────────────┤"
            printf "│  공개 URL : %-41s│\n" "$URL"
            printf "│  LAN  URL : %-41s│\n" "http://$(hostname -I | awk '{print $1}'):$PORT"
            echo "├──────────────────────────────────────────────────────┤"
            echo "│  PWA 설치 : 브라우저에서 위 URL 접속 →               │"
            echo "│            주소창 옆 설치 아이콘 또는 '앱 설치' 버튼  │"
            echo "└──────────────────────────────────────────────────────┘"
            echo ""
        fi
    else
        # 에러/경고만 출력 (불필요한 로그 제거)
        if echo "$line" | grep -qiE "ERR|error|warn|failed|panic"; then
            echo "$line"
        fi
    fi
done

# while 루프 종료 = 터널 종료
echo "터널이 종료되었습니다."
