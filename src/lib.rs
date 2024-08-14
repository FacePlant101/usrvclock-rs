//! Implementation of [usrvclock](https://gitlab.com/lumifaza/usrvclock) protocol

// Copyright 2024 Teodor Wozniak
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use std::{os::unix::net::{SocketAddr, UnixDatagram}, path::PathBuf, sync::Mutex, time::Duration};
use custom_error::custom_error;


const OVERLAY_SIZE_BYTES: usize = 40;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClockOverlay {
    pub clock_id: i64,
    pub last_sync: i64,
    pub shift: i64,
    pub freq_scale: f64,
}

custom_error! { pub OverlayReceiveError
    PacketTooShort = "packet too short",
    UnexpectedData = "unexpected data in packet",
    UnsupportedMajorVersion = "unsupported major version",
    InvalidFlags = "invalid flags (client too old?)",
}

const PROTOCOL_MAJOR_VERSION: u16 = 1;
const PROTOCOL_MINOR_VERSION: u16 = 0;

impl ClockOverlay {
    fn decode(buff: &[u8]) -> Result<Self, OverlayReceiveError> {
        if buff.len() < OVERLAY_SIZE_BYTES {
            return Err(OverlayReceiveError::PacketTooShort);
        }
        if buff[0]!=b'V' || buff[1]!=b'C' {
            return Err(OverlayReceiveError::UnexpectedData);
        }
        let major_version = u16::from_ne_bytes(buff[2..4].try_into().unwrap());
        if major_version != PROTOCOL_MAJOR_VERSION {
            return Err(OverlayReceiveError::UnsupportedMajorVersion);
        }
        let flags = u16::from_ne_bytes(buff[6..8].try_into().unwrap());
        if (flags & 1) == 0 {
            return Err(OverlayReceiveError::InvalidFlags);
        }
        Ok(Self {
            clock_id: i64::from_ne_bytes(buff[8..16].try_into().unwrap()),
            last_sync: i64::from_ne_bytes(buff[16..24].try_into().unwrap()),
            shift: i64::from_ne_bytes(buff[24..32].try_into().unwrap()),
            freq_scale: f64::from_ne_bytes(buff[32..40].try_into().unwrap()),
        })
    }

    pub fn underlying_to_overlay_ns(&self, timestamp: i64) -> i64 {
        let elapsed = timestamp.wrapping_sub(self.last_sync);
        let correction = ((elapsed as f64) * self.freq_scale).round() as i64;
        timestamp.wrapping_add(self.shift).wrapping_add(correction)
    }

    #[cfg(feature="unix")]
    pub fn now_underlying_ns(&self) -> i64 {
        let timespec = nix::time::ClockId::from_raw(self.clock_id as nix::libc::clockid_t).now().unwrap();
        timespec.tv_sec().wrapping_mul(1_000_000_000).wrapping_add(timespec.tv_nsec())
    }

    #[cfg(feature="unix")]
    pub fn now_ns(&self) -> i64 {
        self.underlying_to_overlay_ns(self.now_underlying_ns())
    }
}

pub const DEFAULT_SERVER_SOCKET_PATH: &str = "/tmp/ptp-usrvclock";

pub struct Server {
    socket_path: PathBuf,
    socket: UnixDatagram,
    clients: Vec<SocketAddr>,
}

impl Server {
    pub fn new(path: PathBuf) -> std::io::Result<Self> {
        let _ = std::fs::remove_file(&path);
        let socket = UnixDatagram::bind(&path)?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket_path: path,
            socket,
            clients: vec![],
        })
    }
    pub fn send(&mut self, overlay: ClockOverlay) {
        let flags: u16 = 1;
        let mut buff = [0u8; OVERLAY_SIZE_BYTES];
        buff[0] = b'V';
        buff[1] = b'C';
        buff[2..4].copy_from_slice(&PROTOCOL_MAJOR_VERSION.to_ne_bytes());
        buff[4..6].copy_from_slice(&PROTOCOL_MINOR_VERSION.to_ne_bytes());
        buff[6..8].copy_from_slice(&flags.to_ne_bytes());
        buff[8..16].copy_from_slice(&overlay.clock_id.to_ne_bytes());
        buff[16..24].copy_from_slice(&overlay.last_sync.to_ne_bytes());
        buff[24..32].copy_from_slice(&overlay.shift.to_ne_bytes());
        buff[32..40].copy_from_slice(&overlay.freq_scale.to_ne_bytes());

        // handle new client(s)
        while let Ok((_size, client)) = self.socket.recv_from(&mut buff) {
            self.clients.push(client);
        }
        
        // send update to all clients while removing disconnected ones
        self.clients.retain(|client| {
            match self.socket.send_to_addr(&buff, &client) {
                Ok(_) => true,
                Err(e) => {
                    match e.kind() {
                        std::io::ErrorKind::WouldBlock => true,
                        _ => false
                    }
                }
            }
        });
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

fn client_socket_path(counter: i32) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("usrvclock-client.{}.{}", std::process::id(), counter));
    path
}

pub struct BlockingClient {
    socket_path: PathBuf,
    local_path: Option<PathBuf>,
    socket: Option<UnixDatagram>,
}

impl BlockingClient {
    pub fn new(path: PathBuf) -> Self {
        Self {
            socket_path: path,
            local_path: None,
            socket: None,
        }
    }
    pub fn recv(&mut self) -> Result<ClockOverlay, OverlayReceiveError> {
        let mut counter = 0;
        loop {
            if self.socket.is_none() {
                let local_path = client_socket_path(counter);
                counter += 1;
                let socket = if let Ok(socket) = UnixDatagram::bind(&local_path) {
                    socket
                } else {
                    std::thread::sleep(Duration::from_millis(250));
                    continue;
                };
                if socket.connect(&self.socket_path).is_err() || socket.send(&[]).is_err() {
                    drop(socket);
                    let _ = std::fs::remove_file(&local_path);
                    std::thread::sleep(Duration::from_millis(250));
                    continue;
                }
                self.socket = Some(socket);
                self.local_path = Some(local_path);
            }
            if let Some(socket) = &self.socket {
                let mut buff = [0u8; 40];
                match socket.recv(&mut buff) {
                    Ok(size) => {
                        return ClockOverlay::decode(&buff[0..size]);
                    },
                    _ => {
                        let _ = std::fs::remove_file(self.local_path.as_ref().unwrap());
                        self.socket = None;
                    }
                };
            }
        }
    }
}

#[cfg(feature="tokio")]
pub struct AsyncClient {
    shutdown: tokio::sync::oneshot::Sender<()>,
    sender: tokio::sync::broadcast::Sender<ClockOverlay>,
    join_handle: tokio::task::JoinHandle<()>,
}

#[cfg(feature="tokio")]
impl AsyncClient {
    pub fn start(path: PathBuf, error_handler: Box<dyn FnMut(OverlayReceiveError) + Send>) -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let (tx, _rx) = tokio::sync::broadcast::channel(16);
        let sender = tx.clone();

        let path = path.to_owned();
        let join_handle = tokio::spawn(async move {
            async fn recv_loop(
                path: PathBuf,
                tx: tokio::sync::broadcast::Sender<ClockOverlay>,
                mut error_handler: Box<dyn FnMut(OverlayReceiveError) + Send>,
                file_to_delete: &Mutex<Option<PathBuf>>
            ) {
                let mut socket_opt: Option<(tokio::net::UnixDatagram, PathBuf)> = None;
                let mut counter: i32 = 0;
                loop {
                    if socket_opt.is_none() {
                        let local_path = client_socket_path(counter);
                        counter += 1;
                        let result = tokio::net::UnixDatagram::bind(&local_path);
                        match result {
                            Ok(socket) => {
                                if socket.connect(&path).is_err() || socket.send(&[]).await.is_err() {
                                    drop(socket);
                                    let _ = std::fs::remove_file(&local_path);
                                    tokio::time::sleep(Duration::from_millis(250)).await;
                                } else {
                                    *file_to_delete.lock().unwrap() = Some(local_path.clone());
                                    socket_opt = Some((socket, local_path));
                                }
                            },
                            Err(_e) => {
                                tokio::time::sleep(Duration::from_millis(250)).await;
                            }
                        }
                    }
                    if let Some((socket, local_path)) = &socket_opt {
                        let mut buff = [0u8; OVERLAY_SIZE_BYTES];
                        match socket.recv(&mut buff).await {
                            Ok(size) => {
                                match ClockOverlay::decode(&buff[0..size]) {
                                    Ok(v) => {
                                        let _ = tx.send(v);
                                    },
                                    Err(e) => {
                                        (error_handler)(e);
                                    }
                                }
                            },
                            _ => {
                                let _ = std::fs::remove_file(&local_path);
                                *file_to_delete.lock().unwrap() = None;
                                socket_opt = None;
                            }
                        }
                    }
                }
            }

            let file_to_delete = Mutex::new(None);

            tokio::select! {
                _ = recv_loop(path, tx, error_handler, &file_to_delete) => {},
                _ = shutdown_rx => {}
            };

            let file_to_delete = file_to_delete.into_inner().unwrap();
            if let Some(todelete) = file_to_delete {
                let _ = std::fs::remove_file(&todelete);
            }
        });
        Self {
            sender,
            shutdown: shutdown_tx,
            join_handle
        }
    }
    pub fn stop(self) -> tokio::task::JoinHandle<()> {
        let _ = self.shutdown.send(());
        self.join_handle
    }
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<ClockOverlay> {
        self.sender.subscribe()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocking_send_recv() {
        let test_overlay = ClockOverlay {
            clock_id: 33,
            last_sync: 13000000000,
            shift: -14000000000,
            freq_scale: -0.0002,
        };
        let test_overlay2 = test_overlay;
        let client_thr = std::thread::spawn(move || {
            let mut client = BlockingClient::new("/tmp/usrvclock-test-blocking".into());
            let overlay = client.recv().unwrap();
            assert_eq!(overlay, test_overlay2);
        });
        let server_thr = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(600));
            let mut server = Server::new("/tmp/usrvclock-test-blocking".into()).unwrap();
            for _ in 0..4 {
                std::thread::sleep(Duration::from_millis(500));
                server.send(test_overlay);
            }
        });
        client_thr.join().unwrap();
        server_thr.join().unwrap();
    }

    #[cfg(feature="tokio")]
    #[tokio::test]
    async fn tokio_send_recv() {
        let test_overlay = ClockOverlay {
            clock_id: 33,
            last_sync: 13000000000,
            shift: -14000000000,
            freq_scale: -0.0002,
        };
        let server_thr = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(600));
            let mut server = Server::new("/tmp/usrvclock-test-tokio".into()).unwrap();
            for _ in 0..2 {
                std::thread::sleep(Duration::from_millis(500));
                server.send(test_overlay);
            }
        });
        let client = AsyncClient::start("/tmp/usrvclock-test-tokio".into(), Box::new(|e| panic!("{e:?}")));
        let mut receiver1 = client.subscribe();
        let mut receiver2 = client.subscribe();
        for _ in 0..2 {
            let overlay = receiver1.recv().await.unwrap();
            assert_eq!(overlay, test_overlay);
            let overlay = receiver2.recv().await.unwrap();
            assert_eq!(overlay, test_overlay);
        }
        client.stop().await.unwrap();
        server_thr.join().unwrap();
    }

    #[test]
    fn timestamp_computations() {
        let clock = nix::time::ClockId::CLOCK_MONOTONIC;
        let shift = -2_500_000_000;
        let shifting_overlay = ClockOverlay {
            clock_id: clock.as_raw() as i64,
            last_sync: 0,
            shift: shift,
            freq_scale: 0.0
        };
        let underlying_ts = 12_312_312_333_333_343i64;
        assert_eq!(shifting_overlay.underlying_to_overlay_ns(underlying_ts), underlying_ts + shift);

        let sync_age = 2_400_000_000;
        let shifting_and_slow_overlay = ClockOverlay {
            clock_id: clock.as_raw() as i64,
            last_sync: underlying_ts - sync_age,
            shift: shift,
            freq_scale: -0.25
        };
        assert_eq!(shifting_and_slow_overlay.underlying_to_overlay_ns(underlying_ts), underlying_ts + shift - sync_age/4);

        let shifting_and_fast_overlay = ClockOverlay {
            clock_id: clock.as_raw() as i64,
            last_sync: underlying_ts - sync_age,
            shift: shift,
            freq_scale: 0.5
        };
        assert_eq!(shifting_and_fast_overlay.underlying_to_overlay_ns(underlying_ts), underlying_ts + shift + sync_age/2);
    }
    
    #[cfg(feature="unix")]
    #[test]
    fn now_computations() {
        let epsilon_ns = 5_000_000; // 5 ms
        let identity_overlay = ClockOverlay {
            clock_id: nix::time::ClockId::CLOCK_MONOTONIC.as_raw() as i64,
            last_sync: 0,
            shift: 0,
            freq_scale: 0.0
        };
        let start = identity_overlay.now_underlying_ns();
        let slow_overlay = ClockOverlay {
            last_sync: start,
            freq_scale: -0.5,
            ..identity_overlay
        };
        let shifting_overlay = ClockOverlay {
            shift: -start,
            ..identity_overlay
        };
        let shifting_fast_overlay = ClockOverlay {
            last_sync: start + 100_000_000,
            shift: -start,
            freq_scale: 1.0,
            ..identity_overlay
        };
        
        let diff = slow_overlay.now_ns().wrapping_sub(start);
        assert!(diff >= 0);
        assert!(diff < epsilon_ns);

        let overlay_ts = shifting_overlay.now_ns();
        assert!(overlay_ts >= 0);
        assert!(overlay_ts < epsilon_ns);

        std::thread::sleep(Duration::from_millis(200));
        
        let diff = slow_overlay.now_ns().wrapping_sub(start);
        assert!((diff - 100_000_000).abs() < epsilon_ns);

        let overlay_ts = shifting_overlay.now_ns();
        assert!((overlay_ts - 200_000_000).abs() < epsilon_ns);

        let overlay_ts = shifting_fast_overlay.now_ns();
        assert!((overlay_ts - 300_000_000).abs() < epsilon_ns);

    }
}
