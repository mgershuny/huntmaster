/**
 * HuntMaster Multiplayer Server
 * Real-time WebSocket rooms for AR treasure hunts.
 *
 * Each hunt ID = a room. Players join, collect items, and see each
 * other's progress in real time on a live leaderboard.
 *
 * Deploy: Render free tier, Railway, Fly.io — anything that
 * supports Node.js + WebSocket.
 */

const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');

// ── Config ──────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const STATIC_DIR = path.join(__dirname, '..');
const HEARTBEAT_INTERVAL = 25_000;  // 25s between pings
const CLEANUP_INTERVAL = 60_000;    // 60s between stale-room sweeps
const ROOM_TTL = 30 * 60_000;       // rooms expire after 30 min of inactivity

// ── Room state (in-memory — ephemeral, per hunt session) ────────────
/** @type {Map<string, Room>} */
const rooms = new Map();

/**
 * @typedef {Object} Player
 * @property {string} id
 * @property {string} name
 * @property {number} score
 * @property {Set<number>} itemsFound
 * @property {import('ws').WebSocket} ws
 * @property {number} joinedAt
 * @property {number} lastSeen
 */

/**
 * @typedef {Object} Room
 * @property {string} huntId
 * @property {Map<string, Player>} players
 * @property {number} itemCount
 * @property {number} createdAt
 * @property {number} lastActivity
 */

// ── Helpers ──────────────────────────────────────────────────────────
const uid = () => Math.random().toString(36).slice(2, 10);

function roomKey(huntId) {
  return `hunt:${huntId}`;
}

function getOrCreateRoom(huntId, itemCount) {
  const key = roomKey(huntId);
  let room = rooms.get(key);
  if (!room) {
    room = {
      huntId,
      players: new Map(),
      itemCount: itemCount || 12,
      createdAt: Date.now(),
      lastActivity: Date.now(),
    };
    rooms.set(key, room);
  }
  return room;
}

function buildLeaderboard(room) {
  const entries = [];
  for (const p of room.players.values()) {
    entries.push({
      id: p.id,
      name: p.name,
      score: p.score,
      found: p.itemsFound.size,
    });
  }
  entries.sort((a, b) => b.score - a.score);
  return entries;
}

function buildPresence(room) {
  const names = [];
  for (const p of room.players.values()) {
    names.push({ id: p.id, name: p.name });
  }
  return names;
}

function broadcast(room, msg, excludePlayerId = null) {
  const data = JSON.stringify(msg);
  for (const p of room.players.values()) {
    if (p.id !== excludePlayerId && p.ws.readyState === 1) {
      p.ws.send(data);
    }
  }
}

function send(ws, msg) {
  if (ws.readyState === 1) {
    ws.send(JSON.stringify(msg));
  }
}

function removePlayer(room, playerId) {
  const player = room.players.get(playerId);
  if (!player) return;
  room.players.delete(playerId);
  room.lastActivity = Date.now();

  broadcast(room, {
    type: 'player_left',
    playerId,
    playerName: player.name,
    presence: buildPresence(room),
    leaderboard: buildLeaderboard(room),
  });

  // Clean up empty rooms
  if (room.players.size === 0) {
    rooms.delete(roomKey(room.huntId));
  }

  console.log(`[${room.huntId}] ${player.name} left (${room.players.size} players)`);
}

// ── Cleanup ──────────────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const [key, room] of rooms) {
    // Drop stale players
    for (const [pid, player] of room.players) {
      if (now - player.lastSeen > HEARTBEAT_INTERVAL * 2) {
        console.log(`[${room.huntId}] Timing out stale player ${player.name}`);
        removePlayer(room, pid);
      }
    }
    // Drop expired rooms
    if (room.players.size === 0 && now - room.lastActivity > ROOM_TTL) {
      rooms.delete(key);
      console.log(`[${room.huntId}] Room expired`);
    }
  }
}, CLEANUP_INTERVAL);

// ── HTTP + WebSocket ─────────────────────────────────────────────────
const app = express();
app.use(express.static(STATIC_DIR));
// SPA fallback — serve index.html for any non-static route
app.get('*', (req, res) => {
  if (req.path.startsWith('/.')) return res.status(404).end(); // hide dotfiles
  res.sendFile(path.join(STATIC_DIR, 'index.html'));
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  let playerId = uid();
  let currentRoom = null;

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  console.log(`[connect] ${playerId}`);

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    switch (msg.type) {

      // ── JOIN a hunt room ────────────────────────────────────
      case 'join': {
        const { huntId, playerName, itemCount } = msg;
        if (!huntId || !playerName) {
          send(ws, { type: 'error', error: 'huntId and playerName required' });
          return;
        }
        const room = getOrCreateRoom(huntId, itemCount);
        const name = String(playerName).trim().slice(0, 24) || 'Player';

        // Remove duplicate (same player reconnecting)
        for (const [pid, p] of room.players) {
          if (p.name === name && pid !== playerId) {
            removePlayer(room, pid);
            break;
          }
        }

        currentRoom = room;
        room.players.set(playerId, {
          id: playerId,
          name,
          score: 0,
          itemsFound: new Set(),
          ws,
          joinedAt: Date.now(),
          lastSeen: Date.now(),
        });
        room.lastActivity = Date.now();

        // Tell the joiner the current state
        send(ws, {
          type: 'joined',
          playerId,
          huntId,
          leaderboard: buildLeaderboard(room),
          presence: buildPresence(room),
          itemsAlreadyFound: Array.from(
            (() => {
              const all = new Set();
              for (const p of room.players.values()) {
                for (const idx of p.itemsFound) all.add(idx);
              }
              return all;
            })()
          ),
        });

        // Tell everyone else
        broadcast(room, {
          type: 'player_joined',
          playerId,
          playerName: name,
          presence: buildPresence(room),
          leaderboard: buildLeaderboard(room),
        }, playerId);

        console.log(`[${huntId}] ${name} joined (${room.players.size} players)`);
        break;
      }

      // ── COLLECT an item ─────────────────────────────────────
      case 'collect': {
        if (!currentRoom) { send(ws, { type: 'error', error: 'not in a room' }); return; }
        const { itemIdx } = msg;
        if (itemIdx == null) return;

        const player = currentRoom.players.get(playerId);
        if (!player) return;
        if (player.itemsFound.has(itemIdx)) return; // already collected

        player.itemsFound.add(itemIdx);
        player.score = player.itemsFound.size;
        player.lastSeen = Date.now();
        currentRoom.lastActivity = Date.now();

        const leaderboard = buildLeaderboard(currentRoom);

        // Tell everyone
        broadcast(currentRoom, {
          type: 'item_collected',
          itemIdx,
          playerId,
          playerName: player.name,
          leaderboard,
        });

        // If all items found, announce completion
        const allFound = Array.from({ length: currentRoom.itemCount }, (_, i) => i)
          .every(i => {
            for (const p of currentRoom.players.values()) {
              if (p.itemsFound.has(i)) return true;
            }
            return false;
          });

        if (allFound) {
          broadcast(currentRoom, {
            type: 'hunt_complete',
            leaderboard,
            message: `All ${currentRoom.itemCount} items found!`,
          });
        }

        console.log(`[${currentRoom.huntId}] ${player.name} +1 (${player.score}/${currentRoom.itemCount})`);
        break;
      }

      // ── LEAVE ───────────────────────────────────────────────
      case 'leave': {
        if (currentRoom) {
          removePlayer(currentRoom, playerId);
          currentRoom = null;
        }
        break;
      }

      // ── PING (keepalive) ────────────────────────────────────
      case 'ping': {
        const player = currentRoom?.players.get(playerId);
        if (player) player.lastSeen = Date.now();
        send(ws, { type: 'pong' });
        break;
      }
    }
  });

  ws.on('close', () => {
    if (currentRoom) {
      removePlayer(currentRoom, playerId);
    }
    console.log(`[disconnect] ${playerId}`);
  });

  ws.on('error', (err) => {
    console.error(`[error] ${playerId}:`, err.message);
  });
});

// ── Server heartbeat ─────────────────────────────────────────────────
const heartbeatInterval = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, HEARTBEAT_INTERVAL);

wss.on('close', () => clearInterval(heartbeatInterval));

// ── Start ────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`🏴‍☠️  HuntMaster server running on port ${PORT}`);
  console.log(`   WebSocket: ws://0.0.0.0:${PORT}`);
  console.log(`   Static:    http://0.0.0.0:${PORT}`);
});
