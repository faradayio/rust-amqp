use amq_proto::{Frame, FrameType, FramePayload};
use futures::{Future, Sink, Stream, sync::mpsc};
use openssl::ssl::{SslConnector, SslMethod};
use std::cmp;
use std::thread;
use std::net::{SocketAddr, ToSocketAddrs};
use tokio;
use tokio::io;
use tokio::net::TcpStream;
use tokio_io::AsyncRead;
use tokio_openssl::SslConnectorExt;

use amqp_error::{AMQPError, AMQPResult};
use codec::FramesCodec;

/// Bytes to send after creating a connection to an AMQP server.
const AMQP_INIT: &'static [u8] = &[b'A', b'M', b'Q', b'P', 0, 0, 9, 1];

/// A connection to an AMQP server.
pub struct Connection {
    /// A readable stream of `Frame`s, boxed so that we don't have know exactly
    /// how it's implemented and we can treat it as an abstract interface.
    /// We also need to implement `Send` and `Sync` so we can pass this stream
    /// off to a background thread.
    ///
    /// We store our `stream` and `sink` pre-`framed` and pre-`split` because
    /// it's very hard to call `frame` and `split` when we don't know the
    /// concrete type.
    stream: Box<Stream<Item = Frame, Error = AMQPError> + Send + Sync>,

    /// A writable sink for `Frame`s.
    sink: Box<Sink<SinkItem = Frame, SinkError = AMQPError> + Send + Sync>,
}

impl Connection {
    /// Open a TLS connection to the specified address.
    #[cfg(feature = "tls")]
    pub fn open_tls(host: &str, port: u16) -> AMQPResult<Connection> {
        let addr = socket_addr(host, port)?;
        TcpStream::connect(&addr)
            .from_err::<AMQPError>()
            .and_then(|tcp| {
                SslConnector::builder(SslMethod::tls())
                    .expect("could not create builder")
                    .build()
                    .connect_async(host, tcp)
                    .from_err::<AMQPError>()
            })
            .and_then(|tls| {
                io::write_all(tls, AMQP_INIT).from_err::<AMQPError>()
            })
            .wait()
            .map(|(tls, _written_data)| {
                // Break into frames and split now, because this is much easier
                // before we stick this in a `Box` and lose type information.
                let (sink, stream) = tls.framed(FramesCodec::new()).split();
                Connection {
                    sink: Box::new(sink),
                    stream: Box::new(stream),
                }
            })
    }

    /// Open a regular TCP connection to the specified address.
    pub fn open(host: &str, port: u16) -> AMQPResult<Connection> {
        let addr = socket_addr(host, port)?;
        TcpStream::connect(&addr)
            .from_err::<AMQPError>()
            .and_then(|tcp| {
                io::write_all(tcp, AMQP_INIT).from_err::<AMQPError>()
            })
            .wait()
            .map(|(tcp, _written_data)| {
                // Break into frames and split now, because this is much easier
                // before we stick this in a `Box` and lose type information.
                let (sink, stream) = tcp.framed(FramesCodec::new()).split();
                Connection {
                    sink: Box::new(sink),
                    stream: Box::new(stream),
                }
            })
    }

    /// Split this connection into an independent `(ReadConnection,
    /// WriteConnection)` pair.
    pub fn split(self) -> (ReadConnection, WriteConnection) {
        // Consume our `self`, and extract our `sink` and `stream`.
        let (sink, stream) = (self.sink, self.stream);

        // Set up our `ReadConnection`.
        let (read_sender, read_receiver) = mpsc::channel(0);
        let read_conn = ReadConnection {
            receiver: Some(read_receiver),
        };

        // Copy inbound frames from `stream` to `read_sender`.
        let reader = stream
            .from_err::<AMQPError>()
            .forward(read_sender)
            .map(|_| { debug!("reader done") })
            .map_err(|e| { error!("reader failed: {}", e) });

        // Set up our `WriteConnection`.
        let (write_sender, write_receiver) = mpsc::channel(0);
        let write_conn = WriteConnection {
            frame_max_limit: 131072,
            sender: Some(write_sender),
        };

        // Copy outbound frames from `write_receiver` to `sink`.
        let writer = write_receiver
            .map_err(|_| AMQPError::MpscReceiveError)
            .forward(sink)
            .map(|_| { debug!("writer done") })
            .map_err(|e| { error!("writer failed: {}", e) });

        // TODO: Figure out background error handling.
        // TODO: Figure out shutdown.

        // Fire up a background thread to handle our futures.
        thread::spawn(move || {
            tokio::run(reader.join(writer).map(|_| ()));
        });

        (read_conn, write_conn)
    }
}

/// Convert a hostname and port into an IP address
fn socket_addr(host: &str, port: u16) -> AMQPResult<SocketAddr> {
    (host, port).to_socket_addrs()?
        .next()
        .ok_or_else(|| {
            AMQPError::Protocol(format!("unable to look up {:?}", host))
        })
}

/// A connection which can read frames from an AMQP server.
pub struct ReadConnection {
    receiver: Option<mpsc::Receiver<Frame>>,
}

impl ReadConnection {
    /// Read the next frame. Blocking.
    pub fn read(&mut self) -> AMQPResult<Frame> {
        // Take ownership of the `receiver` so we can pass it to `into_future`.
        // This will fail if a previous `read` failed.
        let receiver = self.receiver.take().ok_or(AMQPError::MpscReceiveError)?;

        // Use `into_future` to wait for the next item received on our stream.
        // This returns the next value in stream, as well as `rest`, which
        // is a stream that will return any following values.
        match receiver.into_future().wait() {
            // We received a value normally, so replace `self.receiver` with
            // our new `receiver`
            Ok((Some(frame), rest)) => {
                self.receiver = Some(rest);
                Ok(frame)
            }
            Ok((None, _rest)) => Err(AMQPError::MpscReceiveError),
            Err(((), _rest)) => Err(AMQPError::MpscReceiveError),
        }
    }
}

/// A connection which can write frames to an AMQP server.
pub struct WriteConnection {
    frame_max_limit: u32,
    sender: Option<mpsc::Sender<Frame>>,
}

impl WriteConnection {
    /// The maximum size of `BODY` frame to send as a single chunk. Larger
    /// frames will be broken into pieces.
    pub fn frame_max_limit(&self) -> u32 {
        self.frame_max_limit
    }

    /// Set the maximum size of `BODY` frame to send as a single chunk.
    pub fn set_frame_max_limit(&mut self, frame_max_limit: u32) {
        self.frame_max_limit = frame_max_limit;
    }

    /// Write a `Frame` to the server, breaking it into multiple frames if
    /// necessary.
    pub fn write(&mut self, frame: Frame) -> AMQPResult<()> {
        match frame.frame_type {
            FrameType::BODY => {
                // TODO: Check if need to include frame header + end octet into calculation. (9
                // bytes extra)
                let frame_type = frame.frame_type;
                let channel = frame.channel;
                for content_frame in split_content_into_frames(frame.payload.into_inner(),
                                                               self.frame_max_limit)
                    .into_iter() {
                    try!(self.write_frame(Frame {
                        frame_type: frame_type,
                        channel: channel,
                        payload: FramePayload::new(content_frame),
                    }))
                }
                Ok(())
            }
            _ => self.write_frame(frame),
        }
    }

    /// Write a single frame to the server, with no further splitting.
    fn write_frame(&mut self, frame: Frame) -> AMQPResult<()> {
        // Take ownership of the `sender` so we can pass it to `send`. This
        // will fail if a previous `send` failed.
        let sender = self.sender.take().ok_or(AMQPError::MpscReceiveError)?;

        // Send our message, and wait for the result.
        match sender.send(frame).wait() {
            // Our message was sent, and we have a new `sender`, so store it.
            Ok(sender) => {
                self.sender = Some(sender);
                Ok(())
            }
            // Our message failed to send, which means the other end of the
            // channel was dropped.
            Err(_err) => Err(AMQPError::MpscReceiveError),
        }
    }
}

impl Clone for WriteConnection {
    fn clone(&self) -> Self {
        Self {
            frame_max_limit: self.frame_max_limit,
            sender: self.sender.clone(),
        }
    }
}

fn split_content_into_frames(content: Vec<u8>, frame_limit: u32) -> Vec<Vec<u8>> {
    assert!(frame_limit > 0, "Can't have frame_max_limit == 0");
    let mut content_frames = vec![];
    let mut current_pos = 0;
    while current_pos < content.len() {
        let new_pos = current_pos + cmp::min(content.len() - current_pos, frame_limit as usize);
        content_frames.push(content[current_pos..new_pos].to_vec());
        current_pos = new_pos;
    }
    content_frames
}

#[test]
fn test_split_content_into_frames() {
    let content = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let frames = split_content_into_frames(content, 3);
    assert_eq!(frames,
               vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9], vec![10]]);
}
