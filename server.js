// SkyRush multiplayer server — zero-dependency WebSocket (Node stdlib only)
// Features: presence by UID, friend requests (relay + persisted), rooms/lobby,
// room invites, match state relay, kill/score events, match end results.
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const DB_FILE = path.join(__dirname, 'skyrush_db.json');
let db = { friends: {} }; // uid -> Set-like object {friendUid: true}
try { db = JSON.parse(fs.readFileSync(DB_FILE, 'utf8')); } catch (e) {}
function saveDb() { try { fs.writeFileSync(DB_FILE, JSON.stringify(db)); } catch (e) {} }
function friendsOf(uid) { return Object.keys(db.friends[uid] || {}); }
function addFriendship(a, b) {
  db.friends[a] = db.friends[a] || {}; db.friends[a][b] = true;
  db.friends[b] = db.friends[b] || {}; db.friends[b][a] = true;
  saveDb();
}

// ---------- WebSocket implementation (RFC6455, server side) ----------
const clients = new Map(); // ws -> {name, uid, room}
const byUid = new Map();   // uid -> ws
const rooms = new Map();   // code -> {code, hostUid, size, map, mode, members:Set<ws>, started}

function wsAccept(key) {
  return crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
}
function encodeFrame(str) {
  const data = Buffer.from(str);
  const len = data.length;
  let head;
  if (len < 126) { head = Buffer.from([0x81, len]); }
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x81; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x81; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, data]);
}
function send(ws, obj) { try { ws.write(encodeFrame(JSON.stringify(obj))); } catch (e) {} }
function broadcast(room, obj, except) {
  for (const m of room.members) if (m !== except) send(m, obj);
}
function roomState(room) {
  return {
    code: room.code, size: room.size, map: room.map, mode: room.mode, started: room.started,
    players: [...room.members].map(m => ({ name: clients.get(m).name, uid: clients.get(m).uid, host: clients.get(m).uid === room.hostUid }))
  };
}

function handleMsg(ws, msg) {
  const me = clients.get(ws);
  if (!me) return;
  switch (msg.type) {
    case 'hello': {
      me.name = String(msg.name || 'Player').slice(0, 20);
      me.uid = String(msg.uid || '');
      if (me.uid) byUid.set(me.uid, ws);
      send(ws, { type: 'welcome', uid: me.uid, friends: friendsOf(me.uid), online: [...byUid.keys()].length });
      break;
    }
    case 'friend_request': {
      const to = String(msg.toUid || '');
      const target = byUid.get(to);
      if (target) send(target, { type: 'friend_request', fromUid: me.uid, fromName: me.name });
      else send(ws, { type: 'friend_request_result', toUid: to, delivered: false });
      break;
    }
    case 'friend_response': {
      const from = String(msg.fromUid || '');
      const target = byUid.get(from);
      if (msg.accept) {
        addFriendship(me.uid, from);
        send(ws, { type: 'friends_update', friends: friendsOf(me.uid) });
        if (target) send(target, { type: 'friend_accepted', uid: me.uid, name: me.name, friends: friendsOf(from) });
      } else if (target) send(target, { type: 'friend_declined', uid: me.uid, name: me.name });
      break;
    }
    case 'create_room': {
      let code; do { code = String(Math.floor(1000 + Math.random() * 9000)); } while (rooms.has(code));
      const room = { code, hostUid: me.uid, size: Math.max(2, Math.min(20, msg.size || 20)), map: msg.map || 'Jungle Outpost', mode: msg.mode || 'DM', members: new Set([ws]), started: false };
      rooms.set(code, room); me.room = code;
      send(ws, { type: 'room_joined', room: roomState(room) });
      break;
    }
    case 'join_room': {
      const room = rooms.get(String(msg.code || ''));
      if (!room) { send(ws, { type: 'error', message: 'Room not found' }); break; }
      if (room.members.size >= room.size) { send(ws, { type: 'error', message: 'Room is full' }); break; }
      room.members.add(ws); me.room = room.code;
      broadcast(room, { type: 'room_joined', room: roomState(room) });
      send(ws, { type: 'room_joined', room: roomState(room) });
      break;
    }
    case 'leave_room': {
      const room = rooms.get(me.room);
      if (room) { room.members.delete(ws); if (room.members.size === 0) rooms.delete(room.code); else broadcast(room, { type: 'room_joined', room: roomState(room) }); }
      me.room = null;
      break;
    }
    case 'invite': {
      const target = byUid.get(String(msg.toUid || ''));
      const room = rooms.get(me.room);
      if (target && room) send(target, { type: 'room_invite', fromUid: me.uid, fromName: me.name, code: room.code, map: room.map });
      break;
    }
    case 'start_game': {
      const room = rooms.get(me.room);
      if (room && me.uid === room.hostUid) { room.started = true; broadcast(room, { type: 'game_start', room: roomState(room) }); send(ws, { type: 'game_start', room: roomState(room) }); }
      break;
    }
    case 'state': {
      const room = rooms.get(me.room);
      if (room) broadcast(room, { type: 'state', uid: me.uid, s: msg.s }, ws);
      break;
    }
    case 'event': {
      const room = rooms.get(me.room);
      if (room) broadcast(room, { type: 'event', uid: me.uid, ev: msg.ev }, ws);
      break;
    }
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ ok: true, online: byUid.size, rooms: rooms.size })); return; }
  res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('SkyRush server OK');
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + wsAccept(key) + '\r\n\r\n');
  clients.set(socket, { name: 'Player', uid: '', room: null });
  let buf = Buffer.alloc(0);
  socket.on('data', chunk => {
    buf = Buffer.concat([buf, chunk]);
    while (true) {
      if (buf.length < 2) return;
      const fin = (buf[0] & 0x80) !== 0, op = buf[0] & 0x0f, masked = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      const maskOff = off; if (masked) off += 4;
      if (buf.length < off + len) return;
      let payload = buf.slice(off, off + len);
      if (masked) { const mask = buf.slice(maskOff, maskOff + 4); payload = Buffer.from(payload.map((b, i) => b ^ mask[i % 4])); }
      buf = buf.slice(off + len);
      if (op === 8) { socket.end(); return; }
      if (op === 9) { const pong = Buffer.from([0x8a, 0]); socket.write(pong); continue; }
      if (op === 1 && fin) { try { handleMsg(socket, JSON.parse(payload.toString())); } catch (e) {} }
    }
  });
  const cleanup = () => {
    const me = clients.get(socket);
    if (me) {
      if (me.uid && byUid.get(me.uid) === socket) byUid.delete(me.uid);
      const room = rooms.get(me.room);
      if (room) { room.members.delete(socket); if (room.members.size === 0) rooms.delete(room.code); else broadcast(room, { type: 'room_joined', room: roomState(room) }); }
    }
    clients.delete(socket);
  };
  socket.on('close', cleanup);
  socket.on('error', cleanup);
});

server.listen(PORT, () => console.log('SkyRush server listening on port ' + PORT));
