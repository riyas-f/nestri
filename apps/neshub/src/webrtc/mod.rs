//! Browsers, over WebRTC.
//!
//! A browser cannot dial an iroh endpoint directly: from inside one, iroh only
//! reaches a peer through a relay, every packet, for the whole session. WebRTC
//! is what browsers carry media over natively, so this serves the same picture,
//! audio and cursor that a native client which never subscribed is shown, and
//! takes input back on a data channel in the event format the native input
//! stream already uses.
//!
//! Signalling is one HTTP POST, the shape WHEP uses: the browser sends an SDP
//! offer and gets the answer back. Media then flows over one UDP port that
//! every browser shares, with this end as an ICE-lite agent -- it answers the
//! browser's connectivity checks and makes none of its own, which is all a
//! server whose address is known needs.

mod http;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use str0m::bwe::{Bitrate, BweKind};
use str0m::change::SdpOffer;
use str0m::channel::ChannelId;
use str0m::format::{Codec, PayloadParams};
use str0m::media::{Frequency, MediaKind, MediaTime, Mid, Pt};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, IceConnectionState, Input, Output, Rtc, RtcConfig};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tracing::{debug, info, warn};

use nesprotocol::{CODEC_AV1, CODEC_H264, CODEC_H265, ClientCaps, DEPTH_8, FLAG_KEYFRAME};

use crate::capture_cmd::keyframe_request;
use crate::control::Controller;
use crate::negotiation::Viewer;
use crate::session::{SessionManager, TaggedInput, split_input_events};

/// What the hub broadcasts that a browser is shown, as the hub broadcasts it.
#[derive(Clone, Debug)]
pub enum Tap {
    /// A video payload, in `nesprotocol::encode_video_payload`'s layout.
    Video(Bytes),
    /// An audio payload, in `nesprotocol::encode_audio_payload`'s layout.
    Audio(Bytes),
    /// A cursor message from nescope, passed to the page unread.
    Cursor(Bytes),
}

pub struct Config {
    /// Where the player page and the offer route are served.
    pub http: SocketAddr,
    /// The UDP socket every browser's media shares.
    pub udp: SocketAddr,
    /// The address browsers are told to send media to: the public address and
    /// port that reach `udp`, which behind a port forward is not `udp` itself.
    pub advertise: SocketAddr,
    /// What a request for a session has to carry.
    pub token: String,
    /// Video codecs offered to browsers, as nesprotocol codec ids.
    pub codecs: Vec<u8>,
    /// The box's video ceiling: as much as the bandwidth estimate may ask for.
    pub ceiling_kbps: u32,
}

/// More would be one encoder serving a crowd at the pace of its slowest
/// member; a session is one player, and this leaves room for a spectator or a
/// reconnect that overlaps the old connection.
const MAX_BROWSERS: usize = 4;

/// An offer accepted but never followed by a connection is let go after this.
const CONNECT_DEADLINE: Duration = Duration::from_secs(20);

/// A browser asks for a keyframe on every loss it cannot repair, and a burst
/// of loss is many asks for what one keyframe answers.
const KEYFRAME_ASK_EVERY: Duration = Duration::from_millis(300);

/// Where the bandwidth estimate starts before it has measured anything.
const START_KBPS: u32 = 6_000;

/// Below this a game is not worth looking at, whatever the estimate says.
const FLOOR_KBPS: u32 = 1_000;

/// The player page, served at `/`.
const PLAYER: &str = include_str!("../../web/player.html");

struct Offer {
    sdp: String,
    reply: oneshot::Sender<Result<String, (u16, String)>>,
}

/// Serve browsers until the hub's broadcast ends.
pub async fn run(
    cfg: Config,
    mgr: Arc<SessionManager>,
    input_tx: broadcast::Sender<TaggedInput>,
    cmd_tx: mpsc::UnboundedSender<Vec<u8>>,
    controller: Arc<Mutex<Controller>>,
) -> Result<()> {
    str0m::crypto::from_feature_flags().install_process_default();

    let socket = UdpSocket::bind(cfg.udp)
        .await
        .with_context(|| format!("binding WebRTC media to udp {}", cfg.udp))?;
    let listener = TcpListener::bind(cfg.http)
        .await
        .with_context(|| format!("binding the WebRTC page to tcp {}", cfg.http))?;
    info!(
        "WebRTC: player page on tcp {}, media on udp {} (browsers are told {})",
        cfg.http, cfg.udp, cfg.advertise
    );

    let (offer_tx, offer_rx) = mpsc::channel(8);
    tokio::spawn(serve_http(
        listener,
        Arc::from(cfg.token.as_str()),
        offer_tx,
    ));

    Driver {
        cfg,
        mgr,
        input_tx,
        cmd_tx,
        controller,
        browsers: Vec::new(),
        next_id: 1,
        rate: RateKeeper::default(),
    }
    .run(socket, offer_rx)
    .await
}

async fn serve_http(listener: TcpListener, token: Arc<str>, offers: mpsc::Sender<Offer>) {
    loop {
        let (mut stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                warn!("WebRTC page: accept failed: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let token = token.clone();
        let offers = offers.clone();
        tokio::spawn(async move {
            let request =
                tokio::time::timeout(Duration::from_secs(10), http::read_request(&mut stream))
                    .await;
            let reply = match request {
                Ok(Ok(request)) => route(request, &token, &offers).await,
                Ok(Err(http::HttpError::TooLarge)) => {
                    http::response(413, "text/plain", b"request too large")
                }
                Ok(Err(e @ http::HttpError::Malformed(_))) => {
                    debug!(%peer, "WebRTC page: {e}");
                    http::response(400, "text/plain", b"bad request")
                }
                // Closed early, an I/O error, or too slow: nobody to answer.
                Ok(Err(_)) | Err(_) => return,
            };
            let _ = stream.write_all(&reply).await;
            let _ = stream.shutdown().await;
        });
    }
}

async fn route(request: http::Request, token: &str, offers: &mpsc::Sender<Offer>) -> Vec<u8> {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => http::response(200, "text/html; charset=utf-8", PLAYER.as_bytes()),
        ("OPTIONS", "/offer") => http::response(204, "text/plain", b""),
        ("POST", "/offer") => {
            if !authorized(&request, token) {
                return http::response(401, "text/plain", b"missing or wrong token");
            }
            let Ok(sdp) = String::from_utf8(request.body) else {
                return http::response(400, "text/plain", b"the offer is not text");
            };
            let (reply, answer) = oneshot::channel();
            if offers.send(Offer { sdp, reply }).await.is_err() {
                return http::response(503, "text/plain", b"not serving browsers");
            }
            match tokio::time::timeout(Duration::from_secs(5), answer).await {
                Ok(Ok(Ok(sdp))) => http::response(201, "application/sdp", sdp.as_bytes()),
                Ok(Ok(Err((status, why)))) => http::response(status, "text/plain", why.as_bytes()),
                _ => http::response(503, "text/plain", b"no answer"),
            }
        }
        _ => http::response(404, "text/plain", b"not found"),
    }
}

/// Whether a request carries the token, as a bearer header or a `token`
/// query parameter. The page uses the header; the parameter is for a quick
/// test with `curl`.
fn authorized(request: &http::Request, token: &str) -> bool {
    let given = request
        .header("authorization")
        .and_then(|v| v.strip_prefix("Bearer "))
        .or_else(|| request.query_param("token"));
    given.is_some_and(|given| same_bytes(given.as_bytes(), token.as_bytes()))
}

/// Equality that takes as long however early the inputs differ, so the
/// token cannot be found a byte at a time by timing refusals.
fn same_bytes(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

struct Browser {
    id: u64,
    rtc: Rtc,
    created: Instant,
    connected: bool,
    dead: bool,
    video: Option<Mid>,
    /// The payload type to send each codec with, from what the browser offered.
    video_pts: Vec<(u8, Pt)>,
    audio: Option<Mid>,
    audio_pt: Option<Pt>,
    /// The data channel input arrives on and the cursor goes out on.
    channel: Option<ChannelId>,
    /// A delta frame means nothing to a decoder that has not had a keyframe,
    /// so none is sent until one has been.
    awaiting_keyframe: bool,
    last_codec: Option<u8>,
    keyframe_asked: Option<Instant>,
    /// This browser's tag on its input, as a native client's gamepad session
    /// tags its own.
    input_session: u32,
    estimate_kbps: Option<u32>,
}

impl Browser {
    fn pt_for(&self, codec: u8) -> Option<Pt> {
        self.video_pts
            .iter()
            .find(|(c, _)| *c == codec)
            .map(|(_, pt)| *pt)
    }

    fn ask_keyframe(&mut self, cmd_tx: &mpsc::UnboundedSender<Vec<u8>>, encoder: Option<u32>) {
        let now = Instant::now();
        if self
            .keyframe_asked
            .is_some_and(|at| now.duration_since(at) < KEYFRAME_ASK_EVERY)
        {
            return;
        }
        self.keyframe_asked = Some(now);
        let _ = cmd_tx.send(keyframe_request(encoder));
    }
}

struct Driver {
    cfg: Config,
    mgr: Arc<SessionManager>,
    input_tx: broadcast::Sender<TaggedInput>,
    cmd_tx: mpsc::UnboundedSender<Vec<u8>>,
    controller: Arc<Mutex<Controller>>,
    browsers: Vec<Browser>,
    next_id: u64,
    rate: RateKeeper,
}

impl Driver {
    async fn run(mut self, socket: UdpSocket, mut offers: mpsc::Receiver<Offer>) -> Result<()> {
        let mut taps = self.mgr.web_tap();
        let mut buf = vec![0u8; 2048];
        loop {
            let wake = self.drive(&socket).await;
            self.reap().await;
            tokio::select! {
                received = socket.recv_from(&mut buf) => match received {
                    Ok((n, source)) => self.receive(&buf[..n], source),
                    // On Linux a UDP socket reports an ICMP refusal of an
                    // earlier send here; that browser's own timeout ends it.
                    Err(e) => debug!("WebRTC socket: {e}"),
                },
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(wake)) => {}
                Some(offer) = offers.recv() => self.offer(offer),
                tap = taps.recv() => match tap {
                    Ok(tap) => self.write(tap),
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("WebRTC fell {n} media items behind; restarting the picture");
                        for b in &mut self.browsers {
                            b.awaiting_keyframe = true;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                },
            }
        }
    }

    /// Let every browser do what it has to now, and say when one next needs
    /// to.
    async fn drive(&mut self, socket: &UdpSocket) -> Instant {
        let now = Instant::now();
        let mut wake = now + Duration::from_millis(100);
        let mut events = Vec::new();
        for b in &mut self.browsers {
            if let Err(e) = b.rtc.handle_input(Input::Timeout(now)) {
                debug!(browser = b.id, "WebRTC timeout handling failed: {e}");
                b.dead = true;
                continue;
            }
            loop {
                match b.rtc.poll_output() {
                    Ok(Output::Transmit(t)) => {
                        if let Err(e) = socket.send_to(&t.contents, t.destination).await {
                            debug!(browser = b.id, "WebRTC send to {}: {e}", t.destination);
                        }
                    }
                    Ok(Output::Timeout(at)) => {
                        wake = wake.min(at);
                        break;
                    }
                    Ok(Output::Event(event)) => events.push((b.id, event)),
                    Err(e) => {
                        debug!(browser = b.id, "WebRTC failed: {e}");
                        b.dead = true;
                        break;
                    }
                }
            }
        }
        if !events.is_empty() {
            // Acting on an event can leave a browser with more to send.
            wake = now;
        }
        for (id, event) in events {
            self.event(id, event).await;
        }
        wake
    }

    async fn event(&mut self, id: u64, event: Event) {
        let encoder = self.mgr.primary_encoder();
        let Some(b) = self.browsers.iter_mut().find(|b| b.id == id) else {
            return;
        };
        match event {
            Event::IceConnectionStateChange(IceConnectionState::Disconnected) => {
                b.dead = true;
            }
            Event::Connected => {
                info!(browser = id, "browser connected");
                b.connected = true;
                b.awaiting_keyframe = true;
                b.ask_keyframe(&self.cmd_tx, encoder);
                b.rtc
                    .bwe()
                    .set_desired_bitrate(Bitrate::kbps(u64::from(self.cfg.ceiling_kbps)));
            }
            Event::MediaAdded(added) => match added.kind {
                MediaKind::Video => {
                    b.video = Some(added.mid);
                    b.video_pts = match b.rtc.writer(added.mid) {
                        Some(writer) => {
                            let offered: Vec<PayloadParams> =
                                writer.payload_params().cloned().collect();
                            pick_video_pts(&offered, &self.cfg.codecs)
                        }
                        None => Vec::new(),
                    };
                    let caps = caps_of(&b.video_pts);
                    if caps.is_empty() {
                        warn!(
                            browser = id,
                            "the browser decodes none of the codecs offered to it; it will see nothing"
                        );
                    } else {
                        info!(browser = id, "browser decodes {:#010b}", caps.bits());
                    }
                    let negotiator = self.mgr.negotiator();
                    negotiator.state(Viewer::Browser(id), caps);
                    negotiator.settle(&self.controller, &self.cmd_tx).await;
                }
                MediaKind::Audio => {
                    b.audio = Some(added.mid);
                    b.audio_pt = b.rtc.writer(added.mid).and_then(|writer| {
                        writer
                            .payload_params()
                            .find(|p| p.spec().codec == Codec::Opus)
                            .map(|p| p.pt())
                    });
                }
            },
            Event::ChannelOpen(channel, label) => {
                if label == "input" {
                    b.channel = Some(channel);
                }
            }
            // A page closes its channel when it closes the connection. Without
            // this a browser that left is noticed only when its connectivity
            // checks time out, several seconds later, and the encoder keeps
            // serving it until then.
            Event::ChannelClose(channel) if Some(channel) == b.channel => {
                b.dead = true;
            }
            Event::ChannelData(data) => {
                if Some(data.id) == b.channel && data.binary {
                    let window = self.mgr.primary_window();
                    for event in split_input_events(&data.data) {
                        let _ = self.input_tx.send((b.input_session, window, event));
                    }
                }
            }
            Event::KeyframeRequest(_) => b.ask_keyframe(&self.cmd_tx, encoder),
            Event::EgressBitrateEstimate(BweKind::Twcc { estimate, .. }) => {
                b.estimate_kbps = Some((estimate.as_u64() / 1000).min(u64::from(u32::MAX)) as u32);
                self.retarget().await;
            }
            _ => {}
        }
    }

    /// Point the encoder at what the slowest browser's path carries, unless a
    /// native client is streaming: its controller has the better view, from
    /// the client's own receiver reports, and two controllers taking turns
    /// would serve neither.
    async fn retarget(&mut self) {
        let Some(lowest) = self
            .browsers
            .iter()
            .filter(|b| b.connected)
            .filter_map(|b| b.estimate_kbps)
            .min()
        else {
            return;
        };
        if !self.mgr.streaming_clients().await.is_empty() {
            return;
        }
        let target = target_kbps(lowest, self.cfg.ceiling_kbps);
        if let Some(kbps) = self.rate.next(target, Instant::now()) {
            let mut cmd = vec![nesprotocol::MSG_ENCODE_SETTINGS];
            nesprotocol::encode_bitrate_only(&mut cmd, kbps);
            let _ = self.cmd_tx.send(cmd);
            debug!("browser video target {kbps} kbps, from an estimate of {lowest} kbps");
        }
    }

    fn receive(&mut self, contents: &[u8], source: SocketAddr) {
        let Ok(contents) = contents.try_into() else {
            return;
        };
        let input = Input::Receive(
            Instant::now(),
            Receive {
                proto: Protocol::Udp,
                source,
                // What the browser sent to, which is what it was told.
                destination: self.cfg.advertise,
                contents,
            },
        );
        if let Some(b) = self.browsers.iter_mut().find(|b| b.rtc.accepts(&input))
            && let Err(e) = b.rtc.handle_input(input)
        {
            debug!(browser = b.id, "WebRTC input failed: {e}");
            b.dead = true;
        }
    }

    fn offer(&mut self, offer: Offer) {
        let result = if self.browsers.len() >= MAX_BROWSERS {
            Err((
                503,
                format!("{MAX_BROWSERS} browsers are already connected"),
            ))
        } else {
            self.accept(&offer.sdp)
        };
        let _ = offer.reply.send(result);
    }

    fn accept(&mut self, sdp: &str) -> Result<String, (u16, String)> {
        let offer =
            SdpOffer::from_sdp_string(sdp).map_err(|e| (400, format!("unreadable offer: {e}")))?;
        let mut config = RtcConfig::new()
            .set_ice_lite(true)
            .clear_codecs()
            .enable_opus(true, false);
        for &codec in &self.cfg.codecs {
            config = match codec {
                CODEC_H264 => config.enable_h264(true),
                CODEC_H265 => config.enable_h265(true),
                CODEC_AV1 => config.enable_av1(true),
                _ => config,
            };
        }
        let start = START_KBPS.min(self.cfg.ceiling_kbps);
        let mut rtc = config
            .enable_bwe(Some(Bitrate::kbps(u64::from(start))))
            .build(Instant::now());
        let candidate = Candidate::host(self.cfg.advertise, "udp")
            .map_err(|e| (503, format!("cannot advertise {}: {e}", self.cfg.advertise)))?;
        rtc.add_local_candidate(candidate);
        let answer = rtc
            .sdp_api()
            .accept_offer(offer)
            .map_err(|e| (400, format!("offer not accepted: {e}")))?;

        let id = self.next_id;
        self.next_id += 1;
        info!(browser = id, "browser offered a session");
        self.browsers.push(Browser {
            id,
            rtc,
            created: Instant::now(),
            connected: false,
            dead: false,
            video: None,
            video_pts: Vec::new(),
            audio: None,
            audio_pt: None,
            channel: None,
            awaiting_keyframe: true,
            last_codec: None,
            keyframe_asked: None,
            input_session: self.mgr.input_session(),
            estimate_kbps: None,
        });
        Ok(answer.to_sdp_string())
    }

    fn write(&mut self, tap: Tap) {
        let now = Instant::now();
        match tap {
            Tap::Video(payload) => {
                let Some((head, data)) = nesprotocol::decode_video_payload(&payload) else {
                    return;
                };
                let keyframe = head.flags & FLAG_KEYFRAME != 0;
                let encoder = self.mgr.primary_encoder();
                // The capture time, on the encoder's microsecond clock, as the
                // 90 kHz RTP clock video uses: frames are spaced as they were
                // drawn, not as they happened to arrive.
                let time = MediaTime::from_90khz(head.captured_us.wrapping_mul(9) / 100);
                for b in &mut self.browsers {
                    if !b.connected {
                        continue;
                    }
                    let (Some(mid), Some(pt)) = (b.video, b.pt_for(head.codec)) else {
                        // A codec this browser does not take: the encoder is
                        // being told what it does, and will change.
                        continue;
                    };
                    if b.last_codec != Some(head.codec) {
                        b.last_codec = Some(head.codec);
                        b.awaiting_keyframe = true;
                    }
                    if b.awaiting_keyframe {
                        if !keyframe {
                            b.ask_keyframe(&self.cmd_tx, encoder);
                            continue;
                        }
                        b.awaiting_keyframe = false;
                    }
                    if let Some(writer) = b.rtc.writer(mid)
                        && let Err(e) = writer.write(pt, now, time, data)
                    {
                        debug!(browser = b.id, "video write: {e}");
                    }
                }
            }
            Tap::Audio(payload) => {
                let Some((position, opus)) = nesprotocol::decode_audio_payload(&payload) else {
                    return;
                };
                // The sample position on the 48 kHz clock Opus is carried on.
                let time = MediaTime::new(position, Frequency::FORTY_EIGHT_KHZ);
                for b in &mut self.browsers {
                    if !b.connected {
                        continue;
                    }
                    let (Some(mid), Some(pt)) = (b.audio, b.audio_pt) else {
                        continue;
                    };
                    if let Some(writer) = b.rtc.writer(mid)
                        && let Err(e) = writer.write(pt, now, time, opus)
                    {
                        debug!(browser = b.id, "audio write: {e}");
                    }
                }
            }
            Tap::Cursor(message) => {
                for b in &mut self.browsers {
                    if let Some(id) = b.channel
                        && let Some(mut channel) = b.rtc.channel(id)
                    {
                        let _ = channel.write(true, &message);
                    }
                }
            }
        }
    }

    /// Let go of browsers that left, never connected, or failed, and let the
    /// encoder move to what the ones still here decode.
    async fn reap(&mut self) {
        let now = Instant::now();
        let mut gone = Vec::new();
        self.browsers.retain(|b| {
            let never_came = !b.connected && now.duration_since(b.created) > CONNECT_DEADLINE;
            let keep = !b.dead && !never_came && b.rtc.is_alive();
            if !keep {
                gone.push(b.id);
            }
            keep
        });
        if gone.is_empty() {
            return;
        }
        let negotiator = self.mgr.negotiator();
        for id in gone {
            info!(
                browser = id,
                "browser gone ({} still connected)",
                self.browsers.len()
            );
            negotiator.forget(Viewer::Browser(id));
        }
        negotiator.settle(&self.controller, &self.cmd_tx).await;
        if self.browsers.is_empty() {
            self.rate = RateKeeper::default();
        }
    }
}

/// The video codecs a browser takes, each with the payload type to send it
/// with, in the order `wanted` prefers them.
fn pick_video_pts(offered: &[PayloadParams], wanted: &[u8]) -> Vec<(u8, Pt)> {
    let offered: Vec<Offered> = offered
        .iter()
        .filter_map(|p| {
            let spec = p.spec();
            Some(Offered {
                codec: nes_codec(spec.codec)?,
                packetization_mode: spec.format.packetization_mode,
                profile_level_id: spec.format.profile_level_id,
                pt: p.pt(),
            })
        })
        .collect();
    pick(&offered, wanted)
}

/// One payload type a browser offered, as much of it as the choice needs.
struct Offered {
    codec: u8,
    packetization_mode: Option<u8>,
    profile_level_id: Option<u32>,
    pt: Pt,
}

fn pick(offered: &[Offered], wanted: &[u8]) -> Vec<(u8, Pt)> {
    wanted
        .iter()
        .filter_map(|&codec| {
            offered
                .iter()
                .filter(|o| o.codec == codec)
                .filter_map(|o| rank(o).map(|r| (r, o.pt)))
                .min_by_key(|(r, _)| *r)
                .map(|(_, pt)| (codec, pt))
        })
        .collect()
}

/// How good a payload type is for this stream, lower being better; `None`
/// for one that cannot carry it.
///
/// An H.264 frame from a game is far larger than one packet, which only
/// packetization mode 1 can split. The profile a browser names says what it
/// promises to decode, not what it can: every browser decodes the High
/// profile an encoder produces under any of these, so the order only prefers
/// the one most widely offered.
fn rank(o: &Offered) -> Option<u8> {
    if o.codec != CODEC_H264 {
        return Some(0);
    }
    if o.packetization_mode != Some(1) {
        return None;
    }
    Some(match o.profile_level_id {
        Some(0x42e01f) => 0,
        Some(0x4d001f) => 1,
        Some(0x42001f) => 2,
        _ => 3,
    })
}

fn nes_codec(codec: Codec) -> Option<u8> {
    match codec {
        Codec::H264 => Some(CODEC_H264),
        Codec::H265 => Some(CODEC_H265),
        Codec::Av1 => Some(CODEC_AV1),
        _ => None,
    }
}

fn caps_of(pts: &[(u8, Pt)]) -> ClientCaps {
    pts.iter().fold(ClientCaps::empty(), |caps, (codec, _)| {
        caps.with(*codec, DEPTH_8)
    })
}

/// What to ask the encoder for, given the lowest estimate among browsers.
///
/// Below the estimate, because an estimate is what the path carried with
/// nothing to spare, and a game's keyframes and scene changes need some.
fn target_kbps(estimate_kbps: u32, ceiling_kbps: u32) -> u32 {
    (estimate_kbps.saturating_mul(85) / 100).clamp(FLOOR_KBPS.min(ceiling_kbps), ceiling_kbps)
}

/// Which targets are worth telling the encoder.
///
/// An estimate moves with every feedback report, and each new target can make
/// the encoder rebuild its rate control; most of those moves are noise.
#[derive(Default)]
struct RateKeeper {
    sent: Option<(u32, Instant)>,
}

impl RateKeeper {
    /// `target`, if it is worth sending now: the first one, or one at least a
    /// tenth away from the last, no sooner than half a second after it.
    fn next(&mut self, target: u32, now: Instant) -> Option<u32> {
        if let Some((last, at)) = self.sent {
            if now.duration_since(at) < Duration::from_millis(500) {
                return None;
            }
            if target.abs_diff(last).saturating_mul(10) < last {
                return None;
            }
        }
        self.sent = Some((target, now));
        Some(target)
    }
}

/// The codec id for a name given on the command line.
pub fn codec_named(name: &str) -> Option<u8> {
    match name.trim().to_ascii_lowercase().as_str() {
        "h264" | "avc" => Some(CODEC_H264),
        "h265" | "hevc" => Some(CODEC_H265),
        "av1" => Some(CODEC_AV1),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h264(pmode: Option<u8>, profile: u32, pt: u8) -> Offered {
        Offered {
            codec: CODEC_H264,
            packetization_mode: pmode,
            profile_level_id: Some(profile),
            pt: Pt::new_with_value(pt),
        }
    }

    #[test]
    fn h264_goes_out_on_a_payload_type_that_can_split_a_frame() {
        let offered = [
            h264(Some(0), 0x42e01f, 96),
            h264(Some(1), 0x42001f, 97),
            h264(Some(1), 0x42e01f, 98),
            h264(Some(1), 0x4d001f, 99),
        ];
        assert_eq!(
            pick(&offered, &[CODEC_H264]),
            vec![(CODEC_H264, Pt::new_with_value(98))]
        );

        let only_single_nal = [h264(Some(0), 0x42e01f, 96), h264(None, 0x42e01f, 97)];
        assert!(pick(&only_single_nal, &[CODEC_H264]).is_empty());
    }

    #[test]
    fn a_codec_is_only_picked_when_it_was_both_wanted_and_offered() {
        let offered = [
            h264(Some(1), 0x42e01f, 98),
            Offered {
                codec: CODEC_AV1,
                packetization_mode: None,
                profile_level_id: None,
                pt: Pt::new_with_value(45),
            },
        ];
        assert_eq!(
            pick(&offered, &[CODEC_AV1, CODEC_H264]),
            vec![
                (CODEC_AV1, Pt::new_with_value(45)),
                (CODEC_H264, Pt::new_with_value(98))
            ]
        );
        assert_eq!(pick(&offered, &[CODEC_H265]), vec![]);
        let caps = caps_of(&pick(&offered, &[CODEC_H264]));
        assert!(caps.supports_codec(CODEC_H264) && !caps.supports_codec(CODEC_AV1));
    }

    #[test]
    fn the_target_leaves_headroom_and_stays_between_floor_and_ceiling() {
        assert_eq!(target_kbps(10_000, 20_000), 8_500);
        assert_eq!(target_kbps(50_000, 20_000), 20_000);
        assert_eq!(target_kbps(300, 20_000), FLOOR_KBPS);
        // A ceiling below the floor wins: a ceiling is never exceeded.
        assert_eq!(target_kbps(300, 800), 800);
    }

    #[test]
    fn small_or_hurried_changes_are_not_sent() {
        let mut rate = RateKeeper::default();
        let t0 = Instant::now();
        assert_eq!(
            rate.next(8_000, t0),
            Some(8_000),
            "the first is always sent"
        );
        assert_eq!(
            rate.next(4_000, t0 + Duration::from_millis(100)),
            None,
            "too soon"
        );
        assert_eq!(
            rate.next(8_500, t0 + Duration::from_secs(1)),
            None,
            "too small"
        );
        assert_eq!(rate.next(6_000, t0 + Duration::from_secs(1)), Some(6_000));
    }

    #[test]
    fn the_token_must_match_exactly() {
        assert!(same_bytes(b"abc", b"abc"));
        assert!(!same_bytes(b"abc", b"abd"));
        assert!(!same_bytes(b"abc", b"abcd"));
        assert!(!same_bytes(b"", b"a"));
    }

    #[test]
    fn codecs_are_named_as_people_write_them() {
        assert_eq!(codec_named("H264"), Some(CODEC_H264));
        assert_eq!(codec_named(" hevc "), Some(CODEC_H265));
        assert_eq!(codec_named("av1"), Some(CODEC_AV1));
        assert_eq!(codec_named("vp8"), None);
    }
}
