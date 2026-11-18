// «Құлаған мысалдар» — сыныптар арасындағы ауызша есеп жарысы, QR арқылы кіру.
// Іске қосу: node server.js   (әдепкі порт 3000)
// Айнымалылар: PORT, ADMIN_PIN (әдепкі 1234), PUBLIC_URL (QR үшін сайт мекенжайы, Render-де өзі анықталады),
//              DATABASE_URL (Postgres, мысалы Neon; болмаса нәтижелер data/results.json файлына жазылады)

const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const QRCode = require('qrcode');

const PORT = Number(process.env.PORT) || 3000;
// интернетте (Render) әдепкі PIN жоқ: ADMIN_PIN орнатылмаса, басқару беті жабық
const ADMIN_PIN = process.env.ADMIN_PIN || (process.env.RENDER ? '' : '1234');
const PUBLIC_URL = (process.env.PUBLIC_URL || process.env.RENDER_EXTERNAL_URL || '').replace(/\/$/, '');
const DATABASE_URL = process.env.DATABASE_URL || '';
const DATA_FILE = path.join(__dirname, 'data', 'results.json');
const TOP_PER_CLASS = 10; // сыныпқа 10 үздік оқушының нәтижесі есептеледі

// ---------- сақтау ----------
// Барлық нәтиже жадта (results) тұрады, рейтинг солардан есептеледі.
// Әр өзгеріс қосымша Postgres-ке (DATABASE_URL болса) немесе JSON файлға жазылады.
let results = [];
const store = DATABASE_URL ? pgStore(DATABASE_URL) : fileStore(DATA_FILE);

function fileStore(file) {
  let timer = null;
  const flush = () => { clearTimeout(timer); timer = setTimeout(() => fs.writeFile(file, JSON.stringify(results), () => {}), 300); };
  return {
    kind: 'файл ' + path.relative(__dirname, file),
    async load() {
      fs.mkdirSync(path.dirname(file), { recursive: true });
      try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return []; }
    },
    async add() { flush(); },
    async remove() { flush(); },
    async clear() { flush(); },
  };
}

function pgStore(url) {
  const { Pool } = require('pg');
  const u = new URL(url);
  const local = /^(localhost|127\.0\.0\.1)$/.test(u.hostname) || u.searchParams.get('sslmode') === 'disable';
  u.searchParams.delete('sslmode'); u.searchParams.delete('channel_binding');
  const pool = new Pool({ connectionString: u.toString(), ssl: local ? false : { rejectUnauthorized: false }, max: 3, idleTimeoutMillis: 30_000 });
  pool.on('error', e => console.error('База қатесі:', e.message));
  const COLS = ['id', 'game', 'name', 'cls', 'score', 'correct', 'wrong', 'secs', 't'];
  return {
    kind: 'Postgres (' + u.hostname + ')',
    async load() {
      await pool.query(`CREATE TABLE IF NOT EXISTS results (
        id text PRIMARY KEY, game text NOT NULL, name text NOT NULL, cls text NOT NULL,
        score integer NOT NULL, correct integer NOT NULL DEFAULT 0, wrong integer NOT NULL DEFAULT 0,
        secs integer NOT NULL DEFAULT 0, t bigint NOT NULL)`);
      const { rows } = await pool.query('SELECT * FROM results ORDER BY t');
      return rows.map(r => ({ ...r, t: Number(r.t) }));
    },
    async add(r) {
      await pool.query(`INSERT INTO results (${COLS}) VALUES (${COLS.map((_, i) => '$' + (i + 1))}) ON CONFLICT (id) DO NOTHING`,
        COLS.map(c => r[c] ?? (c === 'secs' || c === 'wrong' || c === 'correct' ? 0 : null)));
    },
    async remove(ids) { if (ids.length) await pool.query('DELETE FROM results WHERE id = ANY($1)', [ids]); },
    async clear() { await pool.query('DELETE FROM results'); },
  };
}

// басталған ойындар: id -> {started, name, cls}
const games = new Map();
setInterval(() => {
  const now = Date.now();
  for (const [id, g] of games) if (now - g.started > 5 * 60_000) games.delete(id);
}, 60_000);

// ---------- рейтинг ----------
function periodStart(period) {
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  if (period === 'week') d.setDate(d.getDate() - ((d.getDay() + 6) % 7)); // с понедельника
  if (period === 'all') return 0;
  return d.getTime();
}
function normClass(s) {
  return String(s || '').toUpperCase().replace(/\s+/g, '').replace(/[^0-9А-ЯЁӘҒҚҢӨҰҮҺІA-Z\-]/g, '').slice(0, 5);
}
function normName(s) {
  return String(s || '').replace(/\s+/g, ' ').trim().slice(0, 24);
}
function leaderboard(period = 'day') {
  const from = periodStart(period);
  const best = new Map(); // ключ "класс|имя" -> лучший результат
  let games = 0;
  for (const r of results) {
    if (r.t < from || r.game !== 'fall') continue;
    games++;
    const key = r.cls + '|' + r.name.toLowerCase();
    const cur = best.get(key);
    if (!cur || r.score > cur.score) best.set(key, r);
  }
  const players = [...best.values()].sort((a, b) => b.score - a.score || a.t - b.t);
  const classes = new Map();
  for (const p of players) {
    if (!classes.has(p.cls)) classes.set(p.cls, { cls: p.cls, scores: [] });
    classes.get(p.cls).scores.push(p.score);
  }
  const classList = [...classes.values()].map(c => ({
    cls: c.cls,
    players: c.scores.length,
    total: c.scores.slice(0, TOP_PER_CLASS).reduce((s, x) => s + x, 0),
    best: c.scores[0],
  })).sort((a, b) => b.total - a.total);
  return {
    period, games, topPerClass: TOP_PER_CLASS,
    players: players.slice(0, 30).map(p => ({ name: p.name, cls: p.cls, score: p.score, correct: p.correct })),
    classes: classList,
  };
}

// ---------- живые обновления (SSE) ----------
const streams = new Set();
function broadcast() {
  for (const s of streams) {
    s.res.write(`data: ${JSON.stringify(leaderboard(s.period))}\n\n`);
  }
}
setInterval(() => { for (const s of streams) s.res.write(': ping\n\n'); }, 25_000);

// ---------- HTTP ----------
function lanAddress() {
  for (const list of Object.values(os.networkInterfaces())) {
    for (const a of list || []) if (a.family === 'IPv4' && !a.internal) return a.address;
  }
  return 'localhost';
}
function joinUrl(req) {
  if (PUBLIC_URL) return PUBLIC_URL + '/';
  let host = req.headers['x-forwarded-host'] || req.headers.host || `localhost:${PORT}`;
  if (/^(localhost|127\.0\.0\.1)/.test(host)) host = host.replace(/^[^:]+/, lanAddress());
  const proto = req.headers['x-forwarded-proto'] || 'http';
  return `${proto}://${host}/`;
}
function sendJson(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(obj));
}
function readBody(req) {
  return new Promise((resolve) => {
    let data = '';
    req.on('data', c => { data += c; if (data.length > 1e5) req.destroy(); });
    req.on('end', () => { try { resolve(JSON.parse(data || '{}')); } catch { resolve({}); } });
  });
}
const STATIC = { '/': 'fall.html', '/fall': 'fall.html', '/screen': 'screen.html', '/admin': 'admin.html' };

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const p = url.pathname;

  if (req.method === 'GET' && STATIC[p]) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    return fs.createReadStream(path.join(__dirname, 'public', STATIC[p])).pipe(res);
  }

  if (p === '/api/qr.svg') {
    const svg = await QRCode.toString(joinUrl(req), { type: 'svg', margin: 1, color: { dark: '#10261c', light: '#ffffff' } });
    res.writeHead(200, { 'Content-Type': 'image/svg+xml', 'Cache-Control': 'no-store' });
    return res.end(svg);
  }
  if (p === '/api/join-url') return sendJson(res, 200, { url: joinUrl(req) });

  if (p === '/api/leaderboard') return sendJson(res, 200, leaderboard(url.searchParams.get('period') || 'day'));

  if (p === '/api/stream') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', Connection: 'keep-alive' });
    const s = { res, period: url.searchParams.get('period') || 'day' };
    streams.add(s);
    res.write(`data: ${JSON.stringify(leaderboard(s.period))}\n\n`);
    req.on('close', () => streams.delete(s));
    return;
  }

  // ----- «Құлаған мысалдар» -----
  if (p === '/api/fall/start' && req.method === 'POST') {
    const body = await readBody(req);
    const name = normName(body.name), cls = normClass(body.cls);
    if (name.length < 2) return sendJson(res, 400, { error: 'Атыңды жаз (кемінде 2 әріп).' });
    if (!/^\d{1,2}/.test(cls)) return sendJson(res, 400, { error: 'Сынып былай жазылады: 7А, 10Ә.' });
    const id = crypto.randomUUID();
    games.set(id, { kind: 'fall', started: Date.now(), name, cls });
    return sendJson(res, 200, { id });
  }
  if (p === '/api/fall/finish' && req.method === 'POST') {
    const body = await readBody(req);
    const g = games.get(body.id);
    if (!g || g.kind !== 'fall') return sendJson(res, 400, { error: 'Ойын табылмады. Қайтадан баста.' });
    games.delete(body.id);
    const real = (Date.now() - g.started) / 1000;
    const secs = Number(body.seconds) || 0, solved = Math.floor(Number(body.solved) || 0), score = Math.floor(Number(body.score) || 0);
    // шынайылығын тексеру: ойын уақыты, шешу жылдамдығы, бір мысалға ең көп ұпай (5 ұпай × комбо 3 × алтын 3)
    if (secs > real + 3 || secs < 0 || solved < 0 || score < 0 || solved > secs / 0.5 + 3 || score > solved * 15 + 15)
      return sendJson(res, 400, { error: 'Нәтиже күдікті болып шықты. Тағы ойна.' });
    const rec = { id: body.id, game: 'fall', name: g.name, cls: g.cls, score, correct: solved, wrong: 0, secs: Math.round(secs), t: Date.now() };
    try { await store.add(rec); }
    catch (e) { console.error('Нәтиже сақталмады:', e.message); return sendJson(res, 503, { error: 'Нәтиже сақталмады. Бір минуттан соң тағы ойна.' }); }
    results.push(rec);
    broadcast();
    const lb = leaderboard('day');
    return sendJson(res, 200, {
      score,
      place: lb.players.findIndex(x => x.name === g.name && x.cls === g.cls) + 1,
      classPlace: lb.classes.findIndex(c => c.cls === g.cls) + 1,
    });
  }

  // ----- админ -----
  if (p.startsWith('/api/admin/')) {
    const body = req.method === 'POST' ? await readBody(req) : {};
    if (!ADMIN_PIN) return sendJson(res, 403, { error: 'Сервердің баптауларында ADMIN_PIN орнатылмаған.' });
    if (body.pin !== ADMIN_PIN) return sendJson(res, 403, { error: 'PIN қате.' });
    if (p === '/api/admin/list') {
      return sendJson(res, 200, { results: results.slice(-300).reverse() });
    }
    if (p === '/api/admin/delete') {
      let gone = [];
      if (body.id) gone = results.filter(r => r.id === body.id);
      if (body.player) {
        const [cls, name = ''] = String(body.player).split('|');
        gone = results.filter(r => r.cls === cls && r.name.toLowerCase() === name.toLowerCase());
      }
      const ids = new Set(gone.map(r => r.id));
      try { await store.remove([...ids]); } catch (e) { return sendJson(res, 503, { error: 'База қолжетімсіз: ' + e.message }); }
      results = results.filter(r => !ids.has(r.id));
      broadcast();
      return sendJson(res, 200, { removed: ids.size });
    }
    if (p === '/api/admin/reset') {
      try { await store.clear(); } catch (e) { return sendJson(res, 503, { error: 'База қолжетімсіз: ' + e.message }); }
      results = []; broadcast();
      return sendJson(res, 200, { ok: true });
    }
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Табылмады');
});

store.load().then(rows => {
  results = rows;
  server.listen(PORT, () => started());
}).catch(e => {
  console.error('Нәтижелерді жүктеу мүмкін болмады:', e.message);
  console.error('DATABASE_URL дұрыс па? Мысал: postgresql://user:pass@host/dbname');
  process.exit(1);
});

function started() {
  const ip = lanAddress();
  console.log(`  Нәтижелер: ${store.kind}, жүктелді: ${results.length}`);
  if (PUBLIC_URL) {
    console.log(`\n  Құлаған мысалдар іске қосылды!\n`);
    console.log(`  QR бар табло: ${PUBLIC_URL}/screen`);
    console.log(`  Ойын:         ${PUBLIC_URL}/`);
    console.log(`  Басқару:      ${PUBLIC_URL}/admin\n`);
    return;
  }
  console.log(`\n  Құлаған мысалдар іске қосылды!\n`);
  console.log(`  QR бар табло (проекторда ашыңыз): http://${ip}:${PORT}/screen`);
  console.log(`  Оқушыларға арналған ойын:         http://${ip}:${PORT}/`);
  console.log(`  Басқару (PIN ${ADMIN_PIN === '1234' ? '1234 — ADMIN_PIN арқылы өзгертіңіз' : 'орнатылған'}): http://${ip}:${PORT}/admin\n`);
}
