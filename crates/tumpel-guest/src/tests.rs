use serde_json::{json, Value};
use tumpel_api::types::GuestMessage;

use super::{encode, Decoder, FrameError, MAX_FRAME};

fn line(seq: i64, text: &str) -> Value {
    json!({ "type": "TurnLine", "turnId": "turn-1", "lineSeq": seq, "text": text })
}

#[test]
fn a_frame_is_a_big_endian_length_and_the_json_body() {
    let frame = encode(&line(1, "hi")).expect("encodes");
    let body = serde_json::to_vec(&line(1, "hi")).expect("json");
    assert_eq!(frame[..4], u32::try_from(body.len()).expect("fits").to_be_bytes());
    assert_eq!(frame[4..], body[..]);
}

#[test]
fn frames_split_at_any_byte_decode_in_order() {
    let mut wire = encode(&line(1, "one")).expect("encodes");
    wire.extend(encode(&line(2, "two")).expect("encodes"));
    let mut decoder = Decoder::default();
    let mut texts = Vec::new();
    for byte in wire {
        decoder.push(&[byte]);
        while let Some(message) = decoder.take::<GuestMessage>().expect("decodes") {
            let GuestMessage::TurnLine(line) = message else {
                panic!("expected a TurnLine, got {message:?}");
            };
            texts.push((line.line_seq.0, line.text.to_string()));
        }
    }
    assert_eq!(texts, vec![(1, "one".to_string()), (2, "two".to_string())]);
}

#[test]
fn a_partial_header_waits_for_more_bytes() {
    let mut decoder = Decoder::default();
    decoder.push(&[0, 0]);
    assert_eq!(decoder.take::<Value>(), Ok(None));
}

#[test]
fn a_length_over_the_limit_is_refused_before_the_body_arrives() {
    let mut decoder = Decoder::default();
    let length = u32::try_from(MAX_FRAME + 1).expect("fits");
    decoder.push(&length.to_be_bytes());
    assert_eq!(decoder.take::<Value>(), Err(FrameError::TooLarge(MAX_FRAME + 1)));
}

#[test]
fn a_body_that_is_no_guest_message_names_the_contract() {
    let mut decoder = Decoder::default();
    decoder.push(&encode(&json!({ "type": "Nonsense" })).expect("encodes"));
    let error = decoder.take::<GuestMessage>().expect_err("refused");
    assert!(error.to_string().contains("supervisor.tsp"), "{error}");
}
