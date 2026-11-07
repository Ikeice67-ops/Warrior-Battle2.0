// ═══════════════════════════════════════════════════════════════════
//  Warrior Battle: Four Champions — Online Game Server
//  Express + Socket.IO. Serves the game page and handles rooms,
//  matchmaking, spectating, turn-sync and saved matches.
// ═══════════════════════════════════════════════════════════════════
const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;

const app = express();
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
const server = http.createServer(app);
const io = new Server(server);

// ── Storage ─────────────────────────────────────────────────────────
const rooms = new Map();            // roomCode -> room
const matchmakingQueues = new Map();// maxPlayers -> [waiting players]
const saves = new Map();            // saveId -> saved match
const DATA_DIR = path.join(__dirname, 'data');
const SAVES_FILE = path.join(DATA_DIR, 'saves.json');
fs.mkdirSync(DATA_DIR, { recursive: true });
try {
  for (const s of JSON.parse(fs.readFileSync(SAVES_FILE, 'utf8'))) saves.set(s.id, s);
} catch (e) { /* no saves yet */ }
function persistSaves() {
  try { fs.writeFileSync(SAVES_FILE, JSON.stringify([...saves.values()])); } catch (e) {}
}

// ── Helpers ─────────────────────────────────────────────────────────
const CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
function makeCode() {
  let code = '';
  for (let i = 0; i < 6; i++) code += CODE_ALPHABET[Math.floor(Math.random() * CODE_ALPHABET.length)];
  return rooms.has(code) ? makeCode() : code;
}
const makeToken = () => Math.random().toString(36).slice(2) + Date.now().toString(36);
const cleanName = n => (String(n || 'Player').trim() || 'Player').slice(0, 18);
const cleanColor = (c, fallback) => /^#[0-9a-fA-F]{6}$/.test(c) ? c : fallback;

function hostSeat(room) {
  for (let i = 0; i < room.seats.length; i++) if (room.seats[i] && room.seats[i].connected) return i;
  return -1;
}
function roomInfo(room) {
  return {
    roomCode: room.code,
    maxPlayers: room.maxPlayers,
    hostPlayerId: hostSeat(room),
    started: room.started,
    players: room.seats.map(s => s ? { playerId: s.seat, name: s.name, color: s.color, connected: s.connected } : null),
  };
}
function broadcastRoom(room) {
  io.to('room:' + room.code).emit('room:update', roomInfo(room));
}
function getSocketRoom(socket) {
  const code = socket.data.roomCode;
  return code ? rooms.get(code) : null;
}
function removeFromQueues(socket) {
  for (const [k, q] of matchmakingQueues) {
    const i = q.findIndex(e => e.socketId === socket.id);
    if (i >= 0) q.splice(i, 1);
    if (!q.length) matchmakingQueues.delete(k);
  }
}
function seatPlayer(socket, room, seatIndex, name, color) {
  const seat = {
    seat: seatIndex,
    playerToken: makeToken(),
    name: cleanName(name),
    color: cleanColor(color, ['#368cfa', '#e34d53', '#e0aa37', '#36a75c'][seatIndex]),
    connected: true,
    socketId: socket.id,
  };
  room.seats[seatIndex] = seat;
  socket.join('room:' + room.code);
  socket.data.roomCode = room.code;
  socket.data.seat = seatIndex;
  socket.data.spectator = false;
  return seat;
}
function leaveCurrent(socket, silent = false) {
  const code = socket.data.roomCode;
  if (!code) return;
  const room = rooms.get(code);
  socket.leave('room:' + code);
  socket.data.roomCode = null;
  if (!room) return;
  if (socket.data.spectator) { socket.data.spectator = false; return; }
  const seatIndex = socket.data.seat;
  socket.data.seat = null;
  if (seatIndex == null) return;
  const seat = room.seats[seatIndex];
  if (!seat || seat.socketId !== socket.id) return;
  seat.connected = false;
  seat.socketId = null;
  if (!room.started) room.seats[seatIndex] = null; // free the seat in the lobby
  if (!room.seats.some(Boolean)) { rooms.delete(code); return; } // empty room dies
  if (!silent) io.to('room:' + code).emit('room:player-left', { name: seat.name });
  broadcastRoom(room);
  io.to('room:' + code).emit('room:host-changed', { hostPlayerId: hostSeat(room) });
}

// ── Socket events ───────────────────────────────────────────────────
io.on('connection', socket => {
  socket.data.roomCode = null;
  socket.data.seat = null;
  socket.data.spectator = false;

  socket.on('room:create', (opts = {}, cb = () => {}) => {
    leaveCurrent(socket);
    const maxPlayers = [2, 3, 4].includes(opts.maxPlayers) ? opts.maxPlayers : 2;
    const code = makeCode();
    const room = { code, maxPlayers, seats: Array(maxPlayers).fill(null), started: false, revision: 0, snapshot: null, currentTurn: 0 };
    rooms.set(code, room);
    const seat = seatPlayer(socket, room, 0, opts.playerName, opts.color);
    const info = roomInfo(room);
    cb({ ok: true, roomCode: code, playerId: 0, playerToken: seat.playerToken, hostPlayerId: 0, maxPlayers, players: info.players });
    broadcastRoom(room);
  });

  socket.on('room:join', (opts = {}, cb = () => {}) => {
    leaveCurrent(socket);
    const room = rooms.get(String(opts.roomCode || '').toUpperCase().trim());
    if (!room) return cb({ ok: false, message: 'Room not found. Double-check the code.' });
    if (room.started && !room.seats.some(s => s === null)) return cb({ ok: false, message: 'That match is full.' });
    const seatIndex = room.seats.findIndex(s => s === null);
    if (seatIndex === -1) return cb({ ok: false, message: 'That room is full.' });
    seatPlayer(socket, room, seatIndex, opts.playerName, opts.color);
    const info = roomInfo(room);
    cb({ ok: true, roomCode: room.code, playerId: seatIndex, playerToken: room.seats[seatIndex].playerToken, hostPlayerId: info.hostPlayerId, maxPlayers: room.maxPlayers, started: room.started, revision: room.revision, snapshot: room.snapshot, players: info.players });
    broadcastRoom(room);
  });

  socket.on('room:resume', (opts = {}, cb = () => {}) => {
    const room = rooms.get(String(opts.roomCode || '').toUpperCase().trim());
    if (!room) return cb({ ok: false, message: 'Room not found.' });
    const seatIndex = room.seats.findIndex(s => s && s.playerToken === opts.playerToken);
    if (seatIndex === -1) return cb({ ok: false, message: 'Session expired.' });
    leaveCurrent(socket);
    const seat = room.seats[seatIndex];
    seat.connected = true;
    seat.socketId = socket.id;
    socket.join('room:' + room.code);
    socket.data.roomCode = room.code;
    socket.data.seat = seatIndex;
    socket.data.spectator = false;
    const info = roomInfo(room);
    cb({ ok: true, roomCode: room.code, playerId: seatIndex, playerToken: seat.playerToken, hostPlayerId: info.hostPlayerId, maxPlayers: room.maxPlayers, started: room.started, revision: room.revision, snapshot: room.snapshot, players: info.players });
    broadcastRoom(room);
  });

  socket.on('room:leave', () => leaveCurrent(socket));

  socket.on('room:start-game', (snapshot, cb = () => {}) => {
    const room = getSocketRoom(socket);
    if (!room) return cb({ ok: false, message: 'You are not in a room.' });
    if (socket.data.seat !== hostSeat(room)) return cb({ ok: false, message: 'Only the host can start the match.' });
    if (room.seats.filter(s => s && s.connected).length < 2) return cb({ ok: false, message: 'Need at least 2 connected players.' });
    room.started = true;
    room.revision = 1;
    room.snapshot = snapshot;
    room.currentTurn = 0;
    io.to('room:' + room.code).emit('room:game-started', { room: roomInfo(room), revision: room.revision, snapshot });
    cb({ ok: true, revision: room.revision });
  });

  // Turn sync — only the player whose turn it is may submit state.
  socket.on('room:state', (snapshot, cb = () => {}) => {
    const room = getSocketRoom(socket);
    if (!room || !room.started || socket.data.spectator || socket.data.seat == null) return cb({ ok: false, message: 'No active match.' });
    if (socket.data.seat !== room.currentTurn) return cb({ ok: false, message: 'Not your turn.' });
    if (!snapshot || !Array.isArray(snapshot.map) || !Array.isArray(snapshot.units)) return cb({ ok: false, message: 'Invalid game state.' });
    room.snapshot = snapshot;
    room.revision++;
    if (Number.isInteger(snapshot.turn)) room.currentTurn = snapshot.turn;
    socket.to('room:' + room.code).emit('room:state', { snapshot, revision: room.revision });
    cb({ ok: true, revision: room.revision });
  });

  socket.on('matchmaking:join', (opts = {}, cb = () => {}) => {
    removeFromQueues(socket);
    const maxPlayers = [2, 3, 4].includes(opts.maxPlayers) ? opts.maxPlayers : 2;
    const q = matchmakingQueues.get(maxPlayers) || [];
    const i = q.findIndex(e => e.socketId !== socket.id);
    if (i >= 0) {
      const other = q.splice(i, 1)[0];
      if (!q.length) matchmakingQueues.delete(maxPlayers);
      const otherSocket = io.sockets.sockets.get(other.socketId);
      if (!otherSocket) { matchmakingQueues.set(maxPlayers, [{ socketId: socket.id, name: opts.playerName, color: opts.color }]); return cb({ ok: true, waiting: true }); }
      leaveCurrent(socket);
      const code = makeCode();
      const room = { code, maxPlayers, seats: Array(maxPlayers).fill(null), started: false, revision: 0, snapshot: null, currentTurn: 0 };
      rooms.set(code, room);
      seatPlayer(otherSocket, room, 0, other.name, other.color);
      seatPlayer(socket, room, 1, opts.playerName, opts.color);
      const info = roomInfo(room);
      otherSocket.emit('matchmaking:found', { roomCode: code, playerId: 0, playerToken: room.seats[0].playerToken, hostPlayerId: 0, maxPlayers, players: info.players });
      socket.emit('matchmaking:found', { roomCode: code, playerId: 1, playerToken: room.seats[1].playerToken, hostPlayerId: 0, maxPlayers, players: info.players });
      broadcastRoom(room);
      cb({ ok: true });
    } else {
      q.push({ socketId: socket.id, name: opts.playerName, color: opts.color });
      matchmakingQueues.set(maxPlayers, q);
      cb({ ok: true, waiting: true });
    }
  });

  socket.on('matchmaking:cancel', () => removeFromQueues(socket));

  socket.on('rooms:public', cb => {
    cb([...rooms.values()].filter(r => r.started).map(r => ({ roomCode: r.code, maxPlayers: r.maxPlayers, players: roomInfo(r).players })));
  });

  socket.on('room:spectate', (opts = {}, cb = () => {}) => {
    leaveCurrent(socket);
    const room = rooms.get(String(opts.roomCode || '').toUpperCase().trim());
    if (!room || !room.started || !room.snapshot) return cb({ ok: false, message: 'No live match with that code.' });
    socket.join('room:' + room.code);
    socket.data.roomCode = room.code;
    socket.data.spectator = true;
    cb({ ok: true, roomCode: room.code, maxPlayers: room.maxPlayers, revision: room.revision, snapshot: room.snapshot, players: roomInfo(room).players });
  });

  socket.on('room:save', (opts = {}, cb = () => {}) => {
    const room = getSocketRoom(socket);
    if (!room || !room.started || socket.data.spectator || !room.snapshot) return cb({ ok: false, message: 'No match to save.' });
    const id = makeToken().slice(0, 10);
    saves.set(id, { id, name: cleanName(opts.name).slice(0, 40), roomCode: room.code, revision: room.revision, updated: Date.now(), snapshot: room.snapshot });
    persistSaves();
    cb({ ok: true });
  });

  socket.on('saves:list', cb => {
    cb([...saves.values()].sort((a, b) => b.updated - a.updated).map(s => ({ id: s.id, name: s.name, updated: s.updated })));
  });

  socket.on('room:load-save', (opts = {}, cb = () => {}) => {
    leaveCurrent(socket);
    const save = saves.get(String(opts.id || ''));
    if (!save) return cb({ ok: false, message: 'Save not found.' });
    const maxPlayers = save.snapshot.gamePlayers || 2;
    const code = makeCode();
    const room = { code, maxPlayers, seats: Array(maxPlayers).fill(null), started: true, revision: (save.revision || 0) + 1, snapshot: save.snapshot, currentTurn: save.snapshot.turn || 0 };
    rooms.set(code, room);
    seatPlayer(socket, room, 0, opts.playerName, opts.color);
    const info = roomInfo(room);
    cb({ ok: true, roomCode: code, playerId: 0, playerToken: room.seats[0].playerToken, hostPlayerId: 0, maxPlayers, revision: room.revision, snapshot: room.snapshot, players: info.players });
    broadcastRoom(room);
  });

  socket.on('disconnect', () => {
    removeFromQueues(socket);
    leaveCurrent(socket, true);
  });
});

server.listen(PORT, () => console.log(`⚔️  Warrior Battle server running on http://localhost:${PORT}`));
