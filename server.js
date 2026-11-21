'use strict';

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '32kb' }));
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  next();
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: process.env.ALLOWED_ORIGIN || '*', methods: ['GET', 'POST'] },
  maxHttpBufferSize: 64 * 1024,
  pingInterval: 25000,
  pingTimeout: 20000
});

const PORT = Number(process.env.PORT) || 10000;
const MAX_ROOMS = 100;
const MAX_PLAYERS_PER_ROOM = 4;
const rooms = new Map();
const socketRoom = new Map();

function cleanText(value, max = 20) {
  return String(value ?? '').replace(/[<>\u0000-\u001f]/g, '').trim().slice(0, max);
}
function publicRoom(room) {
  return {
    id: room.id,
    name: room.name,
    map: room.map,
    status: room.status,
    players: [...room.players.values()].map(p => ({ id: p.id, nick: p.nick, ready: p.ready })),
    playerCount: room.players.size,
    maxPlayers: MAX_PLAYERS_PER_ROOM
  };
}
function leaveRoom(socket) {
  const roomId = socketRoom.get(socket.id);
  if (!roomId) return;
  const room = rooms.get(roomId);
  socketRoom.delete(socket.id);
  if (!room) return;
  room.players.delete(socket.id);
  socket.leave(roomId);
  if (room.players.size === 0) {
    rooms.delete(roomId);
  } else {
    if (room.hostId === socket.id) room.hostId = room.players.keys().next().value;
    io.to(roomId).emit('room:updated', publicRoom(room));
  }
  io.emit('rooms:updated', listRooms());
}
function listRooms() {
  return [...rooms.values()].map(publicRoom).filter(r => r.status !== 'closed');
}
function getRoomFor(socket) {
  const id = socketRoom.get(socket.id);
  return id ? rooms.get(id) : null;
}

app.get('/', (_req, res) => res.json({
  name: 'Наша гра — сервер',
  status: 'online',
  version: '1.0.0',
  endpoints: ['/health', '/api/rooms']
}));
app.get('/health', (_req, res) => res.status(200).json({ ok: true, status: 'online', rooms: rooms.size }));
app.get('/api/rooms', (_req, res) => res.json({ rooms: listRooms() }));

io.on('connection', socket => {
  socket.emit('server:hello', { ok: true, message: 'Підключено до сервера Нашої гри' });
  socket.emit('rooms:list', listRooms());

  socket.on('rooms:list:request', () => socket.emit('rooms:list', listRooms()));

  socket.on('room:create', payload => {
    if (getRoomFor(socket)) return socket.emit('server:error', { message: 'Спочатку вийди з поточної кімнати.' });
    if (rooms.size >= MAX_ROOMS) return socket.emit('server:error', { message: 'Зараз усі кімнати зайняті. Спробуй пізніше.' });
    const nick = cleanText(payload?.nick, 20) || 'Гравець';
    const name = cleanText(payload?.name, 28) || `Кімната ${rooms.size + 1}`;
    const map = ['Мапа 1', 'Мапа 2'].includes(payload?.map) ? payload.map : 'Мапа 1';
    const id = crypto.randomBytes(3).toString('hex').toUpperCase();
    const room = { id, name, map, status: 'waiting', hostId: socket.id, players: new Map(), createdAt: Date.now(), gameState: null };
    room.players.set(socket.id, { id: socket.id, nick, ready: false });
    rooms.set(id, room);
    socket.join(id);
    socketRoom.set(socket.id, id);
    socket.emit('room:joined', { room: publicRoom(room), isHost: true });
    io.emit('rooms:updated', listRooms());
  });

  socket.on('room:join', payload => {
    if (getRoomFor(socket)) return socket.emit('server:error', { message: 'Ти вже перебуваєш у кімнаті.' });
    const room = rooms.get(cleanText(payload?.roomId, 8).toUpperCase());
    if (!room || room.status === 'closed') return socket.emit('server:error', { message: 'Кімнату не знайдено.' });
    if (room.players.size >= MAX_PLAYERS_PER_ROOM) return socket.emit('server:error', { message: 'У цій кімнаті вже 4 гравці.' });
    if (room.status === 'playing') return socket.emit('server:error', { message: 'Гра в цій кімнаті вже почалася.' });
    const nick = cleanText(payload?.nick, 20) || 'Гравець';
    room.players.set(socket.id, { id: socket.id, nick, ready: false });
    socket.join(room.id);
    socketRoom.set(socket.id, room.id);
    io.to(room.id).emit('room:updated', publicRoom(room));
    socket.emit('room:joined', { room: publicRoom(room), isHost: false });
    io.emit('rooms:updated', listRooms());
  });

  socket.on('player:ready', ready => {
    const room = getRoomFor(socket);
    if (!room) return;
    room.players.get(socket.id).ready = Boolean(ready);
    io.to(room.id).emit('room:updated', publicRoom(room));
  });

  socket.on('game:start', () => {
    const room = getRoomFor(socket);
    if (!room) return;
    if (room.hostId !== socket.id) return socket.emit('server:error', { message: 'Почати гру може тільки власник кімнати.' });
    if (room.players.size < 1) return;
    room.status = 'playing';
    room.gameState = { wave: 0, baseHp: 500, startedAt: Date.now(), updatedAt: Date.now() };
    io.to(room.id).emit('game:started', { room: publicRoom(room), state: room.gameState });
    io.emit('rooms:updated', listRooms());
  });

  // Temporary shared-state channel for the first client integration.
  // Only the room host may publish a bounded snapshot; full server-authoritative
  // battle simulation and persistent accounts will be added in a later version.
  socket.on('game:snapshot', snapshot => {
    const room = getRoomFor(socket);
    if (!room || room.hostId !== socket.id || room.status !== 'playing') return;
    if (!snapshot || typeof snapshot !== 'object' || JSON.stringify(snapshot).length > 24000) {
      return socket.emit('server:error', { message: 'Некоректний або завеликий стан гри.' });
    }
    room.gameState = { ...snapshot, updatedAt: Date.now() };
    socket.to(room.id).emit('game:snapshot', room.gameState);
  });

  socket.on('game:action', action => {
    const room = getRoomFor(socket);
    if (!room || room.status !== 'playing') return;
    const safeAction = {
      type: cleanText(action?.type, 24),
      data: action?.data && typeof action.data === 'object' ? action.data : {},
      playerId: socket.id
    };
    if (JSON.stringify(safeAction).length > 4000) return;
    socket.to(room.id).emit('game:action', safeAction);
  });

  socket.on('room:leave', () => leaveRoom(socket));
  socket.on('disconnect', () => leaveRoom(socket));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Nasha Hra server listening on ${PORT}`);
});
