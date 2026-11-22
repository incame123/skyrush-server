// SkyRush multiplayer server — Deno Deploy version
// Same protocol as the Node server: presence by UID, friend requests
// (relay + persisted), rooms/lobby, room invites, match state relay,
// kill/score events, match end results.

type Client = { name: string; uid: string; room: string | null; ws: WebSocket };
type Room = {
  code: string; hostUid: string; size: number; map: string; mode: string;
  members: Set<Client>; started: boolean;
};

const clients = new Set<Client>();
const byUid = new Map<string, Client>();
const rooms = new Map<string, Room>();

// ---------- friends persistence (Deno KV when available, else memory) ----------
let kv: Deno.Kv | null = null;
try { kv = await Deno.openKv(); } catch (_e) { kv = null; }
const memFriends = new Map<string, string[]>();

async function friendsOf(uid: string): Promise<string[]> {
  if (kv) {
    try { const r = await kv.get<string[]>(["friends", uid]); return r.value ?? []; } catch (_e) { /* fall through */ }
  }
  return memFriends.get(uid) ?? [];
}
async function addFriendship(a: string, b: string): Promise<void> {
  const fa = await friendsOf(a); if (!fa.includes(b)) fa.push(b);
  const fb = await friendsOf(b); if (!fb.includes(a)) fb.push(a);
  memFriends.set(a, fa); memFriends.set(b, fb);
  if (kv) {
    try { await kv.set(["friends", a], fa); await kv.set(["friends", b], fb); } catch (_e) { /* memory copy kept */ }
  }
}

// ---------- helpers ----------
function send(c: Client, obj: unknown) {
  try { c.ws.send(JSON.stringify(obj)); } catch (_e) { /* closed */ }
}
function broadcast(room: Room, obj: unknown, except?: Client) {
  for (const m of room.members) if (m !== except) send(m, obj);
}
function roomState(room: Room) {
  return {
    code: room.code, size: room.size, map: room.map, mode: room.mode, started: room.started,
    players: [...room.members].map((m) => ({ name: m.name, uid: m.uid, host: m.uid === room.hostUid })),
  };
}

async function handleMsg(me: Client, msg: Record<string, unknown>) {
  switch (msg.type) {
    case "hello": {
      me.name = String(msg.name || "Player").slice(0, 20);
      me.uid = String(msg.uid || "");
      if (me.uid) byUid.set(me.uid, me);
      send(me, { type: "welcome", uid: me.uid, friends: await friendsOf(me.uid), online: byUid.size });
      break;
    }
    case "friend_request": {
      const to = String(msg.toUid || "");
      const target = byUid.get(to);
      if (target) send(target, { type: "friend_request", fromUid: me.uid, fromName: me.name });
      else send(me, { type: "friend_request_result", toUid: to, delivered: false });
      break;
    }
    case "friend_response": {
      const from = String(msg.fromUid || "");
      const target = byUid.get(from);
      if (msg.accept) {
        await addFriendship(me.uid, from);
        send(me, { type: "friends_update", friends: await friendsOf(me.uid) });
        if (target) send(target, { type: "friend_accepted", uid: me.uid, name: me.name, friends: await friendsOf(from) });
      } else if (target) send(target, { type: "friend_declined", uid: me.uid, name: me.name });
      break;
    }
    case "create_room": {
      let code: string;
      do { code = String(Math.floor(1000 + Math.random() * 9000)); } while (rooms.has(code));
      const room: Room = {
        code, hostUid: me.uid,
        size: Math.max(2, Math.min(20, Number(msg.size) || 20)),
        map: String(msg.map || "Jungle Outpost"), mode: String(msg.mode || "DM"),
        members: new Set([me]), started: false,
      };
      rooms.set(code, room); me.room = code;
      send(me, { type: "room_joined", room: roomState(room) });
      break;
    }
    case "join_room": {
      const room = rooms.get(String(msg.code || ""));
      if (!room) { send(me, { type: "error", message: "Room not found" }); break; }
      if (room.members.size >= room.size) { send(me, { type: "error", message: "Room is full" }); break; }
      room.members.add(me); me.room = room.code;
      broadcast(room, { type: "room_joined", room: roomState(room) });
      send(me, { type: "room_joined", room: roomState(room) });
      break;
    }
    case "leave_room": {
      const room = me.room ? rooms.get(me.room) : undefined;
      if (room) {
        room.members.delete(me);
        if (room.members.size === 0) rooms.delete(room.code);
        else broadcast(room, { type: "room_joined", room: roomState(room) });
      }
      me.room = null;
      break;
    }
    case "invite": {
      const target = byUid.get(String(msg.toUid || ""));
      const room = me.room ? rooms.get(me.room) : undefined;
      if (target && room) send(target, { type: "room_invite", fromUid: me.uid, fromName: me.name, code: room.code, map: room.map });
      break;
    }
    case "start_game": {
      const room = me.room ? rooms.get(me.room) : undefined;
      if (room && me.uid === room.hostUid) {
        room.started = true;
        broadcast(room, { type: "game_start", room: roomState(room) });
        send(me, { type: "game_start", room: roomState(room) });
      }
      break;
    }
    case "state": {
      const room = me.room ? rooms.get(me.room) : undefined;
      if (room) broadcast(room, { type: "state", uid: me.uid, s: msg.s }, me);
      break;
    }
    case "event": {
      const room = me.room ? rooms.get(me.room) : undefined;
      if (room) broadcast(room, { type: "event", uid: me.uid, ev: msg.ev }, me);
      break;
    }
  }
}

function cleanup(me: Client) {
  if (me.uid && byUid.get(me.uid) === me) byUid.delete(me.uid);
  const room = me.room ? rooms.get(me.room) : undefined;
  if (room) {
    room.members.delete(me);
    if (room.members.size === 0) rooms.delete(room.code);
    else broadcast(room, { type: "room_joined", room: roomState(room) });
  }
  clients.delete(me);
}

Deno.serve((req) => {
  const url = new URL(req.url);
  if (url.pathname === "/health") {
    return new Response(JSON.stringify({ ok: true, online: byUid.size, rooms: rooms.size }), {
      headers: { "Content-Type": "application/json" },
    });
  }
  if (req.headers.get("upgrade") === "websocket") {
    const { socket, response } = Deno.upgradeWebSocket(req);
    const me: Client = { name: "Player", uid: "", room: null, ws: socket };
    clients.add(me);
    socket.onmessage = (ev) => {
      try {
        const msg = JSON.parse(String(ev.data));
        handleMsg(me, msg);
      } catch (_e) { /* ignore bad frames */ }
    };
    socket.onclose = () => cleanup(me);
    socket.onerror = () => cleanup(me);
    return response;
  }
  return new Response("SkyRush server OK", { headers: { "Content-Type": "text/plain" } });
});
