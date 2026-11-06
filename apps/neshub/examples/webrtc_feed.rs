//! Stand in for a box's capture side, to try neshub's browser output on a
//! machine with no game and no GPU.
//!
//! Plays an H.264 stream, and optionally an Ogg Opus file, into the hub's
//! video and audio sockets the way the encoder and the audio service would, at
//! a steady rate, looping. Answers keyframe requests by skipping ahead to the
//! next keyframe. Connects to the input socket the way the compositor would,
//! prints what browsers send, and moves a cursor in a circle so the page has
//! one to draw.
//!
//! The H.264 has to carry access unit delimiters, which is how frames are told
//! apart, and a short keyframe interval, which is how fast a new viewer starts:
//!
//! ```text
//! ffmpeg -f lavfi -i testsrc2=size=1280x720:rate=60 -t 20 -c:v libx264 \
//!   -profile:v high -g 30 -bf 0 -tune zerolatency \
//!   -bsf:v h264_metadata=aud=insert -f h264 /tmp/feed.h264
//! ffmpeg -f lavfi -i sine=frequency=440 -t 20 -ac 2 -c:a libopus -b:a 96k /tmp/feed.ogg
//!
//! cargo run -p neshub -- --webrtc-http 0.0.0.0:8080 &
//! cargo run -p neshub --example webrtc_feed -- /tmp/feed.h264 /tmp/feed.ogg
//! ```
//!
//! Options after the files: `--fps` (60), `--size` (1280x720).

use std::os::unix::io::AsRawFd;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixDatagram, UnixStream};

use nesprotocol::input::{CURSOR_NAMED, decode_input_event, encode_cursor_update};
use nesprotocol::{
    CAPTURE_CMD_SOCKET, CODEC_H264, CODEC_OPUS, FLAG_KEYFRAME, MSG_CLIENT_CAPS,
    MSG_ENCODE_SETTINGS, MSG_IDR_REQUEST, STREAM_AUDIO, STREAM_VIDEO, encode_ipc_frame,
};

const VIDEO_SOCKET: &str = "/tmp/nestri-video.sock";
const AUDIO_SOCKET: &str = "/tmp/nestri-audio.sock";
const INPUT_SOCKET: &str = "/tmp/nestri-input.sock";

/// What libopus produces unless told otherwise: 20 ms at 48 kHz.
const OPUS_FRAME_SAMPLES: u64 = 960;

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let video_path = args
        .next()
        .context("usage: webrtc_feed <video.h264> [audio.ogg]")?;
    let mut audio_path = None;
    let (mut fps, mut width, mut height) = (60u32, 1280u16, 720u16);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--fps" => fps = args.next().context("--fps needs a value")?.parse()?,
            "--size" => {
                let size = args.next().context("--size needs a value")?;
                let (w, h) = size.split_once('x').context("--size is WxH")?;
                (width, height) = (w.parse()?, h.parse()?);
            }
            path if audio_path.is_none() => audio_path = Some(path.to_owned()),
            other => bail!("unexpected argument {other:?}"),
        }
    }

    let units = access_units(&std::fs::read(&video_path)?);
    if units.is_empty() {
        bail!("no access units in {video_path}: was it encoded with aud=insert?");
    }
    let keyframes = units.iter().filter(|u| u.keyframe).count();
    println!(
        "{} frames, {keyframes} keyframes, at {fps} fps, {width}x{height}",
        units.len()
    );

    // Keyframe requests and settings from the hub.
    let _ = std::fs::remove_file(CAPTURE_CMD_SOCKET);
    let commands = UnixDatagram::bind(CAPTURE_CMD_SOCKET)?;
    let (skip_tx, mut skip_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    tokio::spawn(async move {
        let mut buf = [0u8; 256];
        while let Ok(n) = commands.recv(&mut buf).await {
            match buf.first() {
                Some(&MSG_IDR_REQUEST) => {
                    println!("hub: keyframe requested");
                    let _ = skip_tx.send(());
                }
                Some(&MSG_ENCODE_SETTINGS) => {
                    match nesprotocol::decode_encode_settings_ext(&buf[1..n]) {
                        Some(s) => {
                            println!("hub: encode settings, codec {} value {}", s.codec, s.value)
                        }
                        None => println!("hub: encode settings ({n} bytes)"),
                    }
                }
                Some(&MSG_CLIENT_CAPS) => println!("hub: viewers decode {:02x?}", &buf[1..n]),
                Some(other) => println!("hub: command {other:#04x}"),
                None => {}
            }
        }
    });

    if let Some(path) = audio_path {
        let packets = ogg_packets(&std::fs::read(&path)?)?;
        println!("{} Opus packets", packets.len());
        tokio::spawn(play_audio(packets));
    }
    tokio::spawn(stand_in_for_compositor(width, height));

    let video = UnixDatagram::unbound()?;
    grow_send_buffer(&video);
    let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / f64::from(fps)));
    let mut i = 0usize;
    loop {
        tick.tick().await;
        if skip_rx.try_recv().is_ok() {
            while skip_rx.try_recv().is_ok() {}
            if let Some(next) = (i..units.len()).chain(0..i).find(|&j| units[j].keyframe) {
                i = next;
            }
        }
        let unit = &units[i];
        let flags = if unit.keyframe { FLAG_KEYFRAME } else { 0 };
        let frame = encode_ipc_frame(
            STREAM_VIDEO,
            CODEC_H264,
            flags,
            nesprotocol::clock::monotonic_us(),
            width,
            height,
            &unit.data,
        );
        if let Err(e) = video.send_to(&frame, VIDEO_SOCKET).await {
            println!("video socket: {e} (is neshub running?)");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        i = (i + 1) % units.len();
    }
}

struct AccessUnit {
    data: Vec<u8>,
    keyframe: bool,
}

/// Split an Annex B stream into access units at each delimiter.
fn access_units(stream: &[u8]) -> Vec<AccessUnit> {
    let mut starts = Vec::new();
    let mut i = 0;
    while i + 3 <= stream.len() {
        if stream[i] == 0 && stream[i + 1] == 0 && stream[i + 2] == 1 {
            // Include a fourth leading zero in the start code when present.
            let at = if i > 0 && stream[i - 1] == 0 {
                i - 1
            } else {
                i
            };
            starts.push((at, i + 3));
            i += 3;
        } else {
            i += 1;
        }
    }
    let mut units: Vec<AccessUnit> = Vec::new();
    for (n, &(start, payload)) in starts.iter().enumerate() {
        let end = starts.get(n + 1).map_or(stream.len(), |&(s, _)| s);
        let Some(&header) = stream.get(payload) else {
            continue;
        };
        let kind = header & 0x1f;
        if kind == 9 || units.is_empty() {
            units.push(AccessUnit {
                data: Vec::new(),
                keyframe: false,
            });
        }
        let unit = units.last_mut().expect("pushed above");
        unit.keyframe |= kind == 5;
        unit.data.extend_from_slice(&stream[start..end]);
    }
    units
}

/// The packets of an Ogg stream, without its two Opus header packets.
fn ogg_packets(file: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut packets = Vec::new();
    let mut current = Vec::new();
    let mut at = 0;
    while at + 27 <= file.len() {
        if &file[at..at + 4] != b"OggS" {
            bail!("not an Ogg page at byte {at}");
        }
        let segments = usize::from(file[at + 26]);
        let table = &file[at + 27..at + 27 + segments];
        let mut body = at + 27 + segments;
        for &lace in table {
            let lace = usize::from(lace);
            current.extend_from_slice(&file[body..body + lace]);
            body += lace;
            if lace < 255 {
                packets.push(std::mem::take(&mut current));
            }
        }
        at = body;
    }
    Ok(packets.into_iter().skip(2).collect())
}

async fn play_audio(packets: Vec<Vec<u8>>) {
    let Ok(socket) = UnixDatagram::unbound() else {
        return;
    };
    let mut tick = tokio::time::interval(Duration::from_millis(20));
    let mut position = 0u64;
    for packet in packets.iter().cycle() {
        tick.tick().await;
        let frame = encode_ipc_frame(STREAM_AUDIO, CODEC_OPUS, 0, position, 0, 0, packet);
        let _ = socket.send_to(&frame, AUDIO_SOCKET).await;
        position += OPUS_FRAME_SAMPLES;
    }
}

/// Take the compositor's place on the input socket: print the input that
/// reaches it, and send cursor positions back.
async fn stand_in_for_compositor(width: u16, height: u16) {
    loop {
        let Ok(stream) = UnixStream::connect(INPUT_SOCKET).await else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        println!("connected to the input socket");
        let (mut read, mut write) = stream.into_split();
        let cursor = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(33));
            let mut t = 0f32;
            loop {
                tick.tick().await;
                t += 0.05;
                let x = f32::from(width) * (0.5 + 0.3 * t.cos());
                let y = f32::from(height) * (0.5 + 0.3 * t.sin());
                let mut message = Vec::new();
                encode_cursor_update(&mut message, x, y, CURSOR_NAMED);
                let mut frame = (message.len() as u16).to_le_bytes().to_vec();
                frame.extend_from_slice(&message);
                if write.write_all(&frame).await.is_err() {
                    break;
                }
            }
        });
        let mut len = [0u8; 2];
        while read.read_exact(&mut len).await.is_ok() {
            let mut message = vec![0u8; usize::from(u16::from_le_bytes(len))];
            if read.read_exact(&mut message).await.is_err() {
                break;
            }
            match decode_input_event(&message) {
                Some(event) => println!("input: {event:?}"),
                None => println!("input: {} unreadable bytes", message.len()),
            }
        }
        cursor.abort();
        println!("input socket closed");
    }
}

/// A keyframe can be bigger than a datagram socket's default send buffer.
fn grow_send_buffer(socket: &UnixDatagram) {
    let size: libc::c_int = 4 * 1024 * 1024;
    // SAFETY: a valid socket, and a pointer to an int that outlives the call.
    unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &size as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
