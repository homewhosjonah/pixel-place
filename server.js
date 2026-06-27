'use strict';

const path = require('path');
const http = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');
const Database = require('better-sqlite3');

// ----------------------------------------------------------------------------
// Configuration
// ----------------------------------------------------------------------------
const PORT = process.env.PORT || 3000;
const GRID_SIZE = 200;                  // 200 x 200 grid
const TOTAL_PIXELS = GRID_SIZE * GRID_SIZE;
const COOLDOWN_MS = 60 * 1000;          // 60 seconds between placements per user
const DEFAULT_COLOR = '#ffffff';        // white canvas on first run
const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'canvas.db');

// Validation helpers ---------------------------------------------------------
const HEX_COLOR = /^#[0-9a-fA-F]{6}$/;

function isValidCoord(n) {
  return Number.isInteger(n) && n >= 0 && n < GRID_SIZE;
}

function normalizeColor(color) {
  if (typeof color !== 'string') return null;
  const c = color.trim().toLowerCase();
  return HEX_COLOR.test(c) ? c : null;
}

// ----------------------------------------------------------------------------
// Database setup
// ----------------------------------------------------------------------------
let db;
try {
  db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
} catch (err) {
  console.error('[fatal] Failed to open database at', DB_PATH, err);
  process.exit(1);
}

db.exec(`
  CREATE TABLE IF NOT EXISTS pixels (
    idx   INTEGER PRIMARY KEY,
    color TEXT NOT NULL
  );
`);

// In-memory mirror of the canvas for fast full-state snapshots & broadcasts.
// Index = y * GRID_SIZE + x.
const canvas = new Array(TOTAL_PIXELS).fill(DEFAULT_COLOR);

// Load persisted pixels into the in-memory canvas.
function loadCanvas() {
  const rows = db.prepare('SELECT idx, color FROM pixels').all();
  for (const row of rows) {
    if (row.idx >= 0 && row.idx < TOTAL_PIXELS) {
      canvas[row.idx] = row.color;
    }
  }
  console.log(`[db] Loaded ${rows.length} stored pixels (${TOTAL_PIXELS} total cells).`);
}

const upsertPixel = db.prepare(`
  INSERT INTO pixels (idx, color) VALUES (@idx, @color)
  ON CONFLICT(idx) DO UPDATE SET color = excluded.color
`);

function persistPixel(idx, color) {
  upsertPixel.run({ idx, color });
}

try {
  loadCanvas();
} catch (err) {
  console.error('[fatal] Failed to load canvas from database', err);
  process.exit(1);
}

// ----------------------------------------------------------------------------
// Cooldown tracking (in-memory; per user id)
// ----------------------------------------------------------------------------
const lastPlacement = new Map(); // userId -> timestamp(ms)

function cooldownRemaining(userId) {
  const last = lastPlacement.get(userId);
  if (!last) return 0;
  const remaining = COOLDOWN_MS - (Date.now() - last);
  return remaining > 0 ? remaining : 0;
}

// ----------------------------------------------------------------------------
// Express app
// ----------------------------------------------------------------------------
const app = express();

app.disable('x-powered-by');

const PUBLIC_DIR = path.join(__dirname, 'public');
app.use(express.static(PUBLIC_DIR));

app.get('/', (_req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'index.html'));
});

app.get('/healthz', (_req, res) => {
  res.json({ ok: true, online: wss ? wss.clients.size : 0 });
});

const server = http.createServer(app);

// ----------------------------------------------------------------------------
// WebSocket server (shares the HTTP port)
// ----------------------------------------------------------------------------
const wss = new WebSocketServer({ server });

function broadcast(message, { except } = {}) {
  const data = JSON.stringify(message);
  for (const client of wss.clients) {
    if (client.readyState === client.OPEN && client !== except) {
      try {
        client.send(data);
      } catch (err) {
        console.error('[ws] broadcast send failed', err);
      }
    }
  }
}

function broadcastOnlineCount() {
  broadcast({ type: 'online', count: wss.clients.size });
}

function send(ws, message) {
  if (ws.readyState === ws.OPEN) {
    try {
      ws.send(JSON.stringify(message));
    } catch (err) {
      console.error('[ws] send failed', err);
    }
  }
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.userId = null;

  // Send the full canvas snapshot to the newly connected client.
  send(ws, {
    type: 'init',
    gridSize: GRID_SIZE,
    cooldownMs: COOLDOWN_MS,
    canvas, // array of hex strings, index = y*GRID_SIZE + x
  });

  broadcastOnlineCount();

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return send(ws, { type: 'error', message: 'Malformed message' });
    }

    if (!msg || typeof msg !== 'object') return;

    switch (msg.type) {
      case 'hello': {
        // Client announces its persistent user id (from localStorage).
        if (typeof msg.userId === 'string' && msg.userId.length > 0 && msg.userId.length <= 64) {
          ws.userId = msg.userId;
          send(ws, { type: 'cooldown', remainingMs: cooldownRemaining(ws.userId) });
        }
        return;
      }

      case 'place': {
        const userId = ws.userId || (typeof msg.userId === 'string' ? msg.userId : null);
        if (!userId) {
          return send(ws, { type: 'error', message: 'Missing user id' });
        }
        ws.userId = userId;

        const x = Number(msg.x);
        const y = Number(msg.y);
        const color = normalizeColor(msg.color);

        if (!isValidCoord(x) || !isValidCoord(y)) {
          return send(ws, { type: 'error', message: 'Invalid coordinates' });
        }
        if (!color) {
          return send(ws, { type: 'error', message: 'Invalid color' });
        }

        const remaining = cooldownRemaining(userId);
        if (remaining > 0) {
          return send(ws, { type: 'cooldown', remainingMs: remaining });
        }

        const idx = y * GRID_SIZE + x;
        canvas[idx] = color;

        try {
          persistPixel(idx, color);
        } catch (err) {
          console.error('[db] Failed to persist pixel', { idx, color }, err);
          return send(ws, { type: 'error', message: 'Failed to save pixel' });
        }

        lastPlacement.set(userId, Date.now());

        // Acknowledge to the placer (starts their cooldown UI),
        // and broadcast the new pixel to everyone (including placer for consistency).
        send(ws, { type: 'cooldown', remainingMs: COOLDOWN_MS });
        broadcast({ type: 'pixel', x, y, color });
        return;
      }

      default:
        return;
    }
  });

  ws.on('error', (err) => {
    console.error('[ws] client error', err.message);
  });

  ws.on('close', () => {
    broadcastOnlineCount();
  });
});

// Heartbeat to clean up dead connections.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch { /* ignore */ }
  }
}, 30000);

wss.on('close', () => clearInterval(heartbeat));

// ----------------------------------------------------------------------------
// Startup & graceful shutdown
// ----------------------------------------------------------------------------
server.listen(PORT, () => {
  console.log(`[server] Pixel Place running at http://localhost:${PORT}`);
});

server.on('error', (err) => {
  console.error('[fatal] HTTP server error', err);
  process.exit(1);
});

function shutdown(signal) {
  console.log(`\n[server] ${signal} received, shutting down...`);
  clearInterval(heartbeat);
  for (const ws of wss.clients) {
    try { ws.close(1001, 'Server shutting down'); } catch { /* ignore */ }
  }
  server.close(() => {
    try { db.close(); } catch { /* ignore */ }
    console.log('[server] Closed cleanly.');
    process.exit(0);
  });
  // Force exit if close hangs.
  setTimeout(() => process.exit(0), 5000).unref();
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

process.on('uncaughtException', (err) => {
  console.error('[uncaughtException]', err);
});
process.on('unhandledRejection', (err) => {
  console.error('[unhandledRejection]', err);
});
