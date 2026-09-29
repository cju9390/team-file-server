# 팀 파일 서버 (FastAPI + WebSocket)

팀 프로젝트에서 대용량 파일(병리 WSI, 모델 체크포인트, 데이터셋)을 주고받기 위해 직접 만든 파일 공유 서버입니다. 로컬에서 FastAPI 서버를 띄우고 Cloudflare Tunnel로 외부에 공개해, 클라우드 스토리지 없이 **운영 비용 0원**으로 사용했습니다.

| 항목 | 내용 |
|---|---|
| 기간 | 2026.05 ~ 2026.06 |
| 형태 | 개인 프로젝트 (1인 개발, 팀 내부 사용) |
| 역할 | 전체 설계·개발 — FastAPI 백엔드, 순수 JS SPA, WebSocket 전송, Cloudflare Tunnel, PWA, TLS |

## 주요 기능

**파일 관리**
- 업로드/다운로드 최대 2GB, SHA-256 체크섬 무결성 검증
- 폴더 생성·이름 변경·이동·복사, 다중 선택 배치 작업, 폴더 ZIP 다운로드
- 휴지통 — 삭제 시 휴지통으로 이동 후 복원 가능

**미리보기**
- 이미지, PDF, 코드(highlight.js), Markdown, CSV/Parquet 테이블, Jupyter Notebook
- **WSI 병리 슬라이드**(svs, ndpi) 딥줌 뷰어 — OpenSeadragon + openslide
- 텍스트 파일 인라인 편집

**협업**
- WebSocket 실시간 채팅, 파일 메모, 접근 로그, 파일명 검색, 즐겨찾기, 다크모드

**보안**
- 이름 입력 → 세션 토큰 발급(24시간 TTL)
- 로그인 Rate Limiting (5분 내 10회 초과 시 차단)
- 경로 순회(Path Traversal) 방어
- TLS(HTTPS) 모드 지원

## 구현 포인트

- **WebSocket 파일 전송** — HTTP multipart 대신 바이너리 프레임을 청크 단위로 스트리밍해 진행률을 실시간 표시
- **원자적 저장** — 임시 파일(`.tmp`)에 먼저 쓰고 `rename()`으로 이동해 전송 중단 시 깨진 파일이 남지 않게 함
- **프레임워크 없는 SPA** — 상태 관리와 라우팅(History API)을 순수 JS로 직접 구현
- **Cloudflare Tunnel** — 포트 포워딩·공인 IP 없이 외부 공개, `start.sh`가 터널 URL을 자동 추출
- **PWA** — manifest + Service Worker로 모바일 설치 지원

## 실행

```bash
pip install fastapi uvicorn aiofiles websockets
pip install openslide-python pandas pyarrow   # 선택: WSI 뷰어, Parquet 미리보기

bash start.sh                          # 서버 + Cloudflare Tunnel 동시 시작 (권장)
python server.py --port 8765           # HTTPS (cert.pem/key.pem 있을 때)
python server.py --http --port 8765    # HTTP (터널/프록시 뒤에서)

python client.py upload 파일.txt       # CLI 클라이언트
python client.py list
```

## 파일 구조

```
server.py       FastAPI 서버 (백엔드 전체)
client.py       CLI 클라이언트 (업로드/다운로드/목록/삭제)
index.html      웹 프론트엔드 SPA
sw.js           Service Worker
manifest.json   PWA 메타데이터
icon.svg        앱 아이콘
start.sh        서버 + Cloudflare Tunnel 시작 스크립트
```

실행 시 `uploads/`(저장소), `access.log`, `chat.log`가 생성되며 레포에는 포함하지 않습니다. TLS 인증서(`cert.pem`, `key.pem`)도 직접 생성해야 합니다.
