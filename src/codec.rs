//! Tokio codec for reading and writing AMQP frames.

use amq_proto::{Frame, FrameHeader};
use bytes::{BufMut, BytesMut};
use tokio_io::codec::{Decoder, Encoder};

use amqp_error::AMQPError;

/// The number of bytes in an AMQP frame header.
const HEADER_LEN: usize = 7;

/// The number of trailing bytes after an AMQP payload.
const END_LEN: usize = 1;

/// Encode or decode an AMQP `Frame` object. This is used with Tokio's `framed`
/// method to turn an `AsyncRead + AsyncWrite` into a `Stream + Sync` operating
/// on `Frame`s.
pub(crate) struct FramesCodec {
}

impl FramesCodec {
    /// Create a new `FramesCodec`.
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl Decoder for FramesCodec {
    type Item = Frame;
    type Error = AMQPError;

    fn decode(
        &mut self,
        buf: &mut BytesMut,
    ) -> Result<Option<Frame>, AMQPError> {
        // If we don't have a complete header, give up for now.
        if buf.len() < HEADER_LEN {
            trace!(
                "Need {} bytes for frame header, have {} so far",
                HEADER_LEN,
                buf.len(),
            );
            return Ok(None);
        }

        // Figure out how many bytes we need to read.
        let mut header_bytes = [0; HEADER_LEN];
        header_bytes.clone_from_slice(&buf[..HEADER_LEN]);
        let FrameHeader { payload_size, .. } = FrameHeader::new(header_bytes);
        let frame_len = HEADER_LEN + payload_size as usize + END_LEN;
        if buf.len() < frame_len {
            trace!(
                "Need {} bytes for frame, have {} so far",
                frame_len,
                buf.len(),
            );
            return Ok(None);
        }

        // Parse our frame.
        let to_decode = buf.split_to(frame_len);
        let mut to_decode = &to_decode[..];
        trace!("Attempting to decode {} byte frame", to_decode.len());
        let frame = Frame::decode(&mut to_decode)?;
        assert!(to_decode.is_empty()); // Make sure no leftover data.

        trace!("Decoded frame: {:?}", frame);
        Ok(Some(frame))
    }

    fn decode_eof(
        &mut self,
        buf: &mut BytesMut,
    ) -> Result<Option<Frame>, AMQPError> {
        // This is the last data we're getting. This is interesting enough to
        // log.
        trace!("Frame decoder sees end of file");
        self.decode(buf)
    }
}

impl Encoder for FramesCodec {
    type Item = Frame;
    type Error = AMQPError;

    fn encode(
        &mut self,
        frame: Frame,
        buf: &mut BytesMut,
    ) -> Result<(), AMQPError> {
        trace!("Encoding frame: {:?}", frame);
        let encoded = frame.encode()?;
        buf.reserve(encoded.len());
        buf.put(encoded);
        Ok(())
    }
}
