//! Binary wire protocol codec.
//!
//! Frame format (all integers little-endian):
//!
//! ```text
//! +----------+----------+------------------+
//! | u32 len  | u8 type  | payload (len-1)  |
//! +----------+----------+------------------+
//! ```
//!
//! `len` is the total payload length including the 1-byte `type` tag.
//! This is a minimal, allocation-light framing layer — application
//! messages (orders, cancels, execution reports) are encoded/decoded
//! by `session.rs`, which maps them to/from `core_types::Command`/`Event`.

use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;

/// Maximum allowed frame payload size. Guards against a misbehaving
/// or malicious client sending an absurd length prefix and causing
/// unbounded buffer growth.
pub const MAX_FRAME_LEN: u32 = 64 * 1024;

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("frame length {0} exceeds maximum {MAX_FRAME_LEN}")]
    FrameTooLarge(u32),
    #[error("frame too short: need at least 1 byte for message type")]
    FrameTooShort,
    #[error("unknown message type tag: {0}")]
    UnknownMessageType(u8),
}

/// A decoded, but not-yet-interpreted, wire frame: a message type tag
/// plus its raw payload bytes. `session.rs` is responsible for
/// further decoding `payload` based on `msg_type`.
#[derive(Debug, Clone)]
pub struct Frame {
    pub msg_type: u8,
    pub payload: BytesMut,
}

/// Stateless length-prefixed framer/deframer.
///
/// Holds no buffers itself — callers own the `BytesMut` read buffer
/// and call `decode` repeatedly as more bytes arrive. This keeps the
/// codec cheap to construct per-connection and easy to test.
#[derive(Debug, Default, Clone, Copy)]
pub struct Codec;

impl Codec {
    pub const HEADER_LEN: usize = 4; // u32 length prefix

    /// Attempts to decode a single frame from the front of `buf`.
    ///
    /// Returns:
    /// - `Ok(Some(frame))` if a complete frame was decoded; `buf` is
    ///   advanced past the consumed bytes.
    /// - `Ok(None)` if `buf` does not yet contain a complete frame
    ///   (caller should read more bytes and retry). `buf` is left
    ///   untouched in this case.
    /// - `Err(_)` if the frame is malformed (caller should close the
    ///   connection).
    pub fn decode(&self, buf: &mut BytesMut) -> Result<Option<Frame>, CodecError> {
        if buf.len() < Self::HEADER_LEN {
            return Ok(None);
        }

        // Peek the length prefix without consuming, in case the body
        // hasn't arrived yet.
        let len = u32::from_le_bytes(buf[..4].try_into().unwrap());

        if len == 0 {
            return Err(CodecError::FrameTooShort);
        }
        if len > MAX_FRAME_LEN {
            return Err(CodecError::FrameTooLarge(len));
        }

        let total_len = Self::HEADER_LEN + len as usize;
        if buf.len() < total_len {
            // Not enough data yet; reserve so the next read can fill
            // the rest of the frame in one go.
            buf.reserve(total_len - buf.len());
            return Ok(None);
        }

        buf.advance(Self::HEADER_LEN);
        let mut payload = buf.split_to(len as usize);

        if payload.is_empty() {
            return Err(CodecError::FrameTooShort);
        }
        let msg_type = payload[0];
        payload.advance(1);

        Ok(Some(Frame { msg_type, payload }))
    }

    /// Encodes `msg_type` + `payload` into `out`, prefixed with the
    /// total length. `out` is typically the connection's write buffer.
    pub fn encode(&self, msg_type: u8, payload: &[u8], out: &mut BytesMut) -> Result<(), CodecError> {
        let len = 1u32.checked_add(payload.len() as u32)
            .ok_or(CodecError::FrameTooLarge(u32::MAX))?;
        if len > MAX_FRAME_LEN {
            return Err(CodecError::FrameTooLarge(len));
        }

        out.reserve(Self::HEADER_LEN + len as usize);
        out.put_u32_le(len);
        out.put_u8(msg_type);
        out.put_slice(payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_then_decode_roundtrip() {
        let codec = Codec;
        let mut buf = BytesMut::new();
        codec.encode(0x01, b"hello", &mut buf).unwrap();

        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.msg_type, 0x01);
        assert_eq!(&frame.payload[..], b"hello");
        assert!(buf.is_empty());
    }

    #[test]
    fn decode_incomplete_header_returns_none() {
        let codec = Codec;
        let mut buf = BytesMut::from(&b"\x05\x00"[..]); // only 2 of 4 header bytes
        assert!(codec.decode(&mut buf).unwrap().is_none());
        // buf untouched
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn decode_incomplete_body_returns_none_and_reserves() {
        let codec = Codec;
        let mut buf = BytesMut::new();
        // Claim a 10-byte payload but only provide 4.
        buf.put_u32_le(10);
        buf.put_slice(&[1, 2, 3, 4]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 8); // untouched
        assert!(buf.capacity() >= 4 + 10);
    }

    #[test]
    fn decode_rejects_oversized_frame() {
        let codec = Codec;
        let mut buf = BytesMut::new();
        buf.put_u32_le(MAX_FRAME_LEN + 1);
        let err = codec.decode(&mut buf).unwrap_err();
        matches!(err, CodecError::FrameTooLarge(_));
    }

    #[test]
    fn decode_rejects_zero_length() {
        let codec = Codec;
        let mut buf = BytesMut::new();
        buf.put_u32_le(0);
        let err = codec.decode(&mut buf).unwrap_err();
        matches!(err, CodecError::FrameTooShort);
    }

    #[test]
    fn multiple_frames_in_one_buffer() {
        let codec = Codec;
        let mut buf = BytesMut::new();
        codec.encode(0x01, b"a", &mut buf).unwrap();
        codec.encode(0x02, b"bb", &mut buf).unwrap();

        let f1 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(f1.msg_type, 0x01);
        assert_eq!(&f1.payload[..], b"a");

        let f2 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(f2.msg_type, 0x02);
        assert_eq!(&f2.payload[..], b"bb");

        assert!(buf.is_empty());
    }
}