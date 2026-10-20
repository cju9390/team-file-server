const CACHE_NAME = 'fileserver-v1';

// 앱 셸 — 오프라인에서도 화면을 열 수 있도록 캐시
const SHELL_URLS = [
  '/',
  '/manifest.json',
  '/icon.svg',
  'https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js',
  'https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github-dark.min.css',
  'https://cdn.jsdelivr.net/npm/marked@9/marked.min.js',
];

// ── Install: 셸 사전 캐시 ─────────────────────────────────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(SHELL_URLS))
      .then(() => self.skipWaiting())
  );
});

// ── Activate: 이전 버전 캐시 정리 ────────────────────────
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys()
      .then(keys => Promise.all(
        keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))
      ))
      .then(() => self.clients.claim())
  );
});

// ── Fetch: 요청 가로채기 ─────────────────────────────────
self.addEventListener('fetch', event => {
  const url = event.request.url;

  // WebSocket, 인증, API, 파일 프리뷰 → 항상 네트워크
  if (
    url.includes('/ws') ||
    url.includes('/auth/') ||
    url.includes('/memos') ||
    url.includes('/preview/')
  ) {
    return; // SW 미처리 → 브라우저가 직접 처리
  }

  // GET 요청만 캐시 대상
  if (event.request.method !== 'GET') return;

  // 폰트 CDN → 캐시 우선 (네트워크 비용 절감)
  if (url.includes('fonts.googleapis.com') || url.includes('fonts.gstatic.com')) {
    event.respondWith(
      caches.match(event.request).then(cached =>
        cached || fetch(event.request).then(res => {
          const clone = res.clone();
          caches.open(CACHE_NAME).then(c => c.put(event.request, clone));
          return res;
        })
      )
    );
    return;
  }

  // 그 외 → 네트워크 우선, 실패 시 캐시, 캐시도 없으면 셸(/)
  event.respondWith(
    fetch(event.request)
      .then(res => {
        if (res.ok) {
          const clone = res.clone();
          caches.open(CACHE_NAME).then(c => c.put(event.request, clone));
        }
        return res;
      })
      .catch(() =>
        caches.match(event.request)
          .then(cached => cached || caches.match('/'))
      )
  );
});
