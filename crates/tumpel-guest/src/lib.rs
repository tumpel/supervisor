pub mod codex_login;
pub mod credential_file;
pub mod disk;
pub mod setup_output;

use std::fmt;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tumpel_api::types::{GuestInput, GuestMessage};

pub const GUEST_PORT: u32 = 1024;
pub const READY_PORT: u32 = 1025;
pub const GUEST_CID: u32 = 3;
pub const MAX_FRAME: usize = 4 * 1024 * 1024;

#[must_use]
pub fn tail(text: &str, limit: usize) -> &str {
    let start = text.len().saturating_sub(limit);
    let start = (start..text.len())
        .find(|index| text.is_char_boundary(*index))
        .unwrap_or(text.len());
    &text[start..]
}

#[derive(Debug, PartialEq, Eq)]
pub enum FrameError {
    TooLarge(usize),
    Json(String),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge(size) => write!(
                f,
                "a frame of {size} bytes exceeds the {MAX_FRAME}-byte limit of the guest protocol"
            ),
            Self::Json(reason) => write!(
                f,
                "a frame does not match the guest protocol in supervisor.tsp: {reason}"
            ),
        }
    }
}

impl std::error::Error for FrameError {}

pub fn encode<T: Serialize>(message: &T) -> Result<Vec<u8>, FrameError> {
    let body = serde_json::to_vec(message).map_err(|error| FrameError::Json(error.to_string()))?;
    if body.len() > MAX_FRAME {
        return Err(FrameError::TooLarge(body.len()));
    }
    let length = u32::try_from(body.len()).map_err(|_| FrameError::TooLarge(body.len()))?;
    let mut frame = Vec::with_capacity(body.len() + 4);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

pub trait Frame: DeserializeOwned {
    fn unrecognised(&self) -> Option<&Value>;
}

impl Frame for GuestMessage {
    fn unrecognised(&self) -> Option<&Value> {
        match self {
            Self::Unrecognised(message) => Some(message),
            _ => None,
        }
    }
}

impl Frame for GuestInput {
    fn unrecognised(&self) -> Option<&Value> {
        match self {
            Self::Unrecognised(input) => Some(input),
            _ => None,
        }
    }
}

impl Frame for Value {
    fn unrecognised(&self) -> Option<&Value> {
        None
    }
}

#[derive(Default)]
pub struct Decoder {
    buffer: Vec<u8>,
}

impl Decoder {
    pub fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    #[must_use]
    pub fn buffered(&self) -> &[u8] {
        &self.buffer
    }

    pub fn take<T: Frame>(&mut self) -> Result<Option<T>, FrameError> {
        let Some(header) = self.buffer.first_chunk::<4>() else {
            return Ok(None);
        };
        let length = u32::from_be_bytes(*header) as usize;
        if length > MAX_FRAME {
            return Err(FrameError::TooLarge(length));
        }
        if self.buffer.len() < length + 4 {
            return Ok(None);
        }
        let body: Vec<u8> = self.buffer.drain(..length + 4).skip(4).collect();
        let frame: T = serde_json::from_slice(&body).map_err(|error| FrameError::Json(error.to_string()))?;
        if let Some(unknown) = frame.unrecognised() {
            return Err(FrameError::Json(format!("{unknown} is no message this side knows")));
        }
        Ok(Some(frame))
    }
}

#[cfg(test)]
mod tests;
