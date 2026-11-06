## neshub

One connection out of the box.

Everything inside the guest that produces or consumes a stream talks to neshub
over a Unix socket. neshub muxes it all into a single [iroh](https://www.iroh.computer)
QUIC endpoint and fans client input back the other way. A client dials that
endpoint with a *ticket* and gets video, audio, cursor and stats on it.

### The sockets

| socket | default | direction | carries |
| --- | --- | --- | --- |
| `--video-ipc` | `/tmp/nestri-video.sock` | nescapture → neshub | encoded video frames |
| `--audio-ipc` | `/tmp/nestri-audio.sock` | neswire → neshub | Opus packets |
| `--input-ipc` | `/tmp/nestri-input.sock` | neshub ↔ nescope | input out, cursor and stats back |
| `--stats-ipc` | `/tmp/nestri-stats.sock` | nescapture → neshub | encoder stats |
| `--screenshot-ipc` | `/tmp/nestri-screenshot.sock` | neshub → nescope | a picture of the screen, on request |
| `--ticket-ipc` | `/tmp/nestri-ticket.sock` | neshub → nesinit | the ticket, once |
| — | `/tmp/nescapture-cmd.sock` | neshub → nescapture | IDR requests, encode settings |

neshub is the listener on every one of them and the other side dials in. That
is deliberate: it removes the startup ordering problem entirely, since a
producer that is not running yet simply has not connected yet.

### The ticket

```
nestri:<base64 of {endpoint_addr, stream_name}>
```

Generated once per boot, when the endpoint binds. It is served on a socket
rather than printed because stdout here is a log file inside a virtual machine
and the person who needs it is outside one — `nesinit` reads it and carries it
to the host.

### Browsers

A browser cannot dial an iroh endpoint directly — from inside one, iroh only
reaches a peer through a relay — so neshub can also serve browsers over
WebRTC. It is off unless `--webrtc-http` is set, and then adds:

| route | |
| --- | --- |
| `GET /` | a bare player page: picture, sound, cursor, keyboard and mouse |
| `POST /offer` | an SDP offer in, the answer out, the way WHEP does it; needs `Authorization: Bearer <token>` |

Media for every browser shares one UDP port. A browser is shown what a native
client that never subscribed is shown: the primary picture, its Opus audio,
and the cursor, which comes over the data channel and is drawn by the page.
Keyboard and mouse go back on the same channel, in the input stream's own
event format. Browsers count in codec negotiation like any client, so a
browser that only decodes H.264 holds the room to H.264. The encoder's bitrate
follows the browsers' congestion feedback unless a native client is streaming,
whose controller decides instead.

| flag | env | default | |
| --- | --- | --- | --- |
| `--webrtc-http` | `NESTRI_WEBRTC_HTTP` | off | TCP address for the page and the offer route |
| `--webrtc-udp-port` | `NESTRI_WEBRTC_UDP_PORT` | `8443` | the media port |
| `--webrtc-public-ip` | `NESTRI_WEBRTC_PUBLIC_IP` | this machine's address on its default route | where browsers send media |
| `--webrtc-public-port` | `NESTRI_WEBRTC_PUBLIC_PORT` | the media port | the outside port, when a forward changes it |
| `--webrtc-token` | `NESTRI_WEBRTC_TOKEN` | random, logged at start | what a session request must carry |
| `--webrtc-codecs` | `NESTRI_WEBRTC_CODECS` | `h264` | video codecs offered to browsers, preferred first: `h264`, `h265`, `av1` |

Behind a port forward — a container on a rented machine, say — publish the UDP
port and the TCP one, then set `--webrtc-public-ip` to the public address and
`--webrtc-public-port` to the outside port the forward chose. Nothing inside
the container can see either.

Not there yet: gamepads, window subscriptions, more than stereo audio, and a
TCP fallback for networks that block UDP.

To try it with no game and no GPU, the `webrtc_feed` example plays an encoded
clip into the hub's sockets the way the encoder would, prints the input that
reaches the compositor's socket, and moves a cursor. Its header has the
commands that make the clip.

### What it does not do

neshub does not start the payload, know its name, or decide when the box is
finished — `nesinit` owns all three, and shuts the VM down around this process.
The same neshub binary serves a game, a desktop, or anything else that draws to
`nescope`, because it never learns which one it is looking at.

### Running it

```bash
cargo run --bin neshub                    # every socket at its default
cargo run --bin neshub -- --relay none    # direct connections only
RUST_LOG=neshub=debug cargo run --bin neshub
cargo run --bin neshub -- --webrtc-http 0.0.0.0:8080   # browsers too
```
