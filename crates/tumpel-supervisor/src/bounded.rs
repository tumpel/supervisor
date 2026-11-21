use tumpel_api::types::error::ConversionError;
use tumpel_api::types::Detail;

pub fn bounded<T>(what: &str, value: impl Into<String>) -> Result<T, String>
where
    T: TryFrom<String, Error = ConversionError>,
{
    value
        .into()
        .try_into()
        .map_err(|error: ConversionError| format!("{what}: {error}"))
}

#[expect(
    clippy::expect_used,
    reason = "the notice is a fixed sentence around a short name and the limit, far under the Detail bound"
)]
pub fn reported(what: &str, detail: &str) -> Detail {
    Detail::try_from(detail).unwrap_or_else(|error| {
        eprintln!("tumpel-supervisor: {what} did not fit the protocol ({error}); in full: {detail}");
        Detail::try_from(format!(
            "{what} did not fit the protocol ({error}); the supervisor log on this host holds it in full"
        ))
        .expect("the notice fits Detail")
    })
}
