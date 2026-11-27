# skyrush-server

Zero-dependency WebSocket server (Node.js standard library only) for **SkyRush**,
a 2D jetpack shooter for CrazyGames.

What it does:

- Player presence by UID
- Friend requests (relay + persisted)
- Rooms / lobby, room invites, match state relay
- Kill / score events and match-end results

## Run

```bash
node server.js
```

Listens on port `3000` by default (`PORT` env var overrides it).
Requires Node.js 18 or newer. No dependencies to install.

## License

MIT — see [LICENSE](LICENSE).
