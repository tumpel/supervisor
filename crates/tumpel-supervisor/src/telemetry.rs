use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt::Debug;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::time::Instant;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Instrument, Span, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::{LookupSpan, Registry};
use tumpel_api::types::{
    ChannelHostMessage, EntityId, GuestMetric, MetricBatch, MetricInstrument, MetricKind, MetricName, SamplePoint,
    ShortText, TurnPoint,
};

use crate::bounded::bounded;
use crate::usage::{CpuTimes, HostStats};

pub const BUFFER_POINTS: usize = 5_000;
pub const BATCH_POINTS: usize = 500;

const TARGET: &str = "tumpel_supervisor::telemetry";
const TURN: &str = "turn";
const PHASE: &str = "phase";
const OUTCOME: &str = "outcome";
const REACHED: &str = "reached";

const INSTRUMENTS: [(&str, &str, MetricKind, &str); 12] = [
    (
        "turn.phase",
        "s",
        MetricKind::Turn,
        "How long one phase of a turn ran on the host",
    ),
    (
        "turn.startup",
        "s",
        MetricKind::Turn,
        "From the host taking a turn to its harness's first line",
    ),
    (
        "turn.end",
        "{turn}",
        MetricKind::Turn,
        "A turn the host finished, by outcome",
    ),
    (
        "host.cpu.busy",
        "1",
        MetricKind::Sample,
        "The share of CPU time the host spent busy",
    ),
    (
        "host.load.1m",
        "1",
        MetricKind::Sample,
        "The host's one-minute load average",
    ),
    (
        "host.load.5m",
        "1",
        MetricKind::Sample,
        "The host's five-minute load average",
    ),
    (
        "host.load.15m",
        "1",
        MetricKind::Sample,
        "The host's fifteen-minute load average",
    ),
    (
        "host.memory.available",
        "MiB",
        MetricKind::Sample,
        "Memory the host can still give out",
    ),
    ("host.memory.total", "MiB", MetricKind::Sample, "The host's memory"),
    (
        "host.boxes.running",
        "{box}",
        MetricKind::Sample,
        "Boxes the host runs or holds paused",
    ),
    (
        "host.boxes.warm",
        "{box}",
        MetricKind::Sample,
        "Paused boxes the host keeps warm",
    ),
    (
        "telemetry.dropped",
        "{point}",
        MetricKind::Sample,
        "Points the host dropped because its buffer was full",
    ),
];

#[derive(Clone, Copy, Debug)]
pub enum Phase {
    Claim,
    ImageFetch,
    Restore,
    Boot,
    Clone,
    FirstOutput,
}

impl Phase {
    fn named(self) -> &'static str {
        match self {
            Self::Claim => "Claim",
            Self::ImageFetch => "ImageFetch",
            Self::Restore => "Restore",
            Self::Boot => "Boot",
            Self::Clone => "Clone",
            Self::FirstOutput => "FirstOutput",
        }
    }
}

pub fn turn(turn_id: &str, session_id: &str, harness: &str, harness_version: &str, model: &str) -> Span {
    tracing::info_span!(
        target: TARGET,
        TURN,
        turn_id,
        session_id,
        harness,
        harness_version,
        model,
        start_kind = tracing::field::Empty
    )
}

pub fn start_kind(turn: &Span, warm: bool) {
    turn.record("start_kind", if warm { "Warm" } else { "Cold" });
}

pub fn phase(phase: Phase) -> Span {
    tracing::info_span!(target: TARGET, PHASE, phase = phase.named(), reached = tracing::field::Empty)
}

pub fn reached(phase: &Span) {
    phase.record(REACHED, true);
}

pub async fn timed<T>(phase: Phase, work: impl Future<Output = T>, done: impl FnOnce(&T) -> bool) -> T {
    let span = self::phase(phase);
    let result = work.instrument(span.clone()).await;
    if done(&result) {
        reached(&span);
    }
    result
}

pub fn ended(message: &ChannelHostMessage) {
    let outcome = match message {
        ChannelHostMessage::TurnComplete(_) => "Completed".to_string(),
        ChannelHostMessage::TurnFailed(failed) => failed.reason.to_string(),
        ChannelHostMessage::TurnRepositoryCloneFailed(_) => "CloneFailed".to_string(),
        _ => return,
    };
    tracing::info!(target: TARGET, outcome);
}

pub fn withdrawn() {
    tracing::info!(target: TARGET, outcome = "Withdrawn");
}

#[derive(Clone, Debug, PartialEq)]
enum Point {
    Sample(SamplePoint),
    Turn(TurnPoint),
}

struct Buffer {
    points: VecDeque<Point>,
    capacity: usize,
    dropped: u64,
    cpu: Option<CpuTimes>,
    declared: HashMap<MetricName, MetricInstrument>,
}

#[derive(Clone)]
pub struct Telemetry(Arc<Mutex<Buffer>>);

fn metric(name: &str) -> Option<MetricName> {
    MetricName::try_from(name).ok()
}

fn instrument(metric: &MetricName) -> Option<MetricInstrument> {
    let (_, unit, kind, description) = INSTRUMENTS.iter().find(|(name, ..)| *name == metric.as_str())?;
    Some(MetricInstrument {
        metric: metric.clone(),
        unit: (*unit).try_into().ok()?,
        kind: kind.clone(),
        description: (*description).try_into().ok()?,
    })
}

fn now() -> chrono::DateTime<chrono::FixedOffset> {
    chrono::Utc::now().fixed_offset()
}

impl Telemetry {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self(Arc::new(Mutex::new(Buffer {
            points: VecDeque::with_capacity(capacity),
            capacity,
            dropped: 0,
            cpu: None,
            declared: HashMap::new(),
        })))
    }

    fn buffer(&self) -> std::sync::MutexGuard<'_, Buffer> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn push(&self, point: Point) {
        let mut buffer = self.buffer();
        if buffer.points.len() >= buffer.capacity {
            buffer.points.pop_front();
            buffer.dropped += 1;
        }
        buffer.points.push_back(point);
    }

    fn sample(&self, name: &str, value: f64) {
        if let Some(metric) = metric(name) {
            self.push(Point::Sample(SamplePoint {
                at: now(),
                dimensions: HashMap::new(),
                metric,
                value,
            }));
        }
    }

    #[allow(clippy::cast_precision_loss)]
    pub fn host(&self, stats: &HostStats, running: u32) {
        let (busy, dropped) = {
            let mut buffer = self.buffer();
            let busy = buffer.cpu.and_then(|before| stats.cpu.busy_since(before));
            buffer.cpu = Some(stats.cpu);
            (busy, std::mem::take(&mut buffer.dropped))
        };
        if let Some(busy) = busy {
            self.sample("host.cpu.busy", busy);
        }
        let [one, five, fifteen] = stats.loads;
        self.sample("host.load.1m", one);
        self.sample("host.load.5m", five);
        self.sample("host.load.15m", fifteen);
        self.sample("host.memory.available", stats.usage.memory_available_mib as f64);
        self.sample("host.memory.total", stats.memory_total_mib as f64);
        self.sample("host.boxes.running", f64::from(running));
        self.sample("host.boxes.warm", f64::from(stats.boxes_warm));
        self.sample("telemetry.dropped", dropped as f64);
    }

    #[must_use]
    pub fn heartbeat_batch(&self) -> MetricBatch {
        let (points, declared) = {
            let mut buffer = self.buffer();
            let take = buffer.points.len().min(BATCH_POINTS);
            let points: Vec<Point> = buffer.points.drain(..take).collect();
            (points, buffer.declared.clone())
        };
        let (samples, turns) = split(points);
        metric_batch(samples, turns, &declared)
    }
}

fn split(points: Vec<Point>) -> (Vec<SamplePoint>, Vec<TurnPoint>) {
    points
        .into_iter()
        .fold((Vec::new(), Vec::new()), |(mut samples, mut turns), point| {
            match point {
                Point::Sample(sample) => samples.push(sample),
                Point::Turn(turn) => turns.push(turn),
            }
            (samples, turns)
        })
}

fn metric_batch(
    samples: Vec<SamplePoint>,
    turns: Vec<TurnPoint>,
    declared: &HashMap<MetricName, MetricInstrument>,
) -> MetricBatch {
    let metrics: BTreeSet<&MetricName> = samples
        .iter()
        .map(|point| &point.metric)
        .chain(turns.iter().map(|point| &point.metric))
        .collect();
    MetricBatch {
        instruments: metrics
            .into_iter()
            .filter_map(|metric| instrument(metric).or_else(|| declared.get(metric).cloned()))
            .collect(),
        samples,
        turns,
    }
}

#[derive(Default)]
struct Fields(BTreeMap<&'static str, String>);

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0.insert(field.name(), format!("{value:?}"));
    }
}

struct Opened {
    at: Instant,
    fields: BTreeMap<&'static str, String>,
}

const DIMENSIONS: [(&str, &str); 5] = [
    ("harness", "harness"),
    ("harness_version", "harnessVersion"),
    ("model", "model"),
    ("start_kind", "startKind"),
    (OUTCOME, "outcome"),
];

const GUEST_DIMENSIONS: [(&str, &str); 2] = [("harness", "harness"), ("model", "model")];

impl Opened {
    fn dimensions(&self) -> HashMap<String, String> {
        self.dimensions_of(&DIMENSIONS)
    }

    fn dimensions_of(&self, named: &[(&str, &str)]) -> HashMap<String, String> {
        named
            .iter()
            .filter_map(|(field, dimension)| Some(((*dimension).to_string(), self.fields.get(field)?.clone())))
            .collect()
    }

    fn point(&self, name: &str, value: f64, extra: Option<(&str, &str)>) -> Option<Point> {
        let mut dimensions = self.dimensions();
        if let Some((key, value)) = extra {
            dimensions.insert(key.to_string(), value.to_string());
        }
        self.point_with(name, value, dimensions)
    }

    fn point_with(&self, name: &str, value: f64, dimensions: HashMap<String, String>) -> Option<Point> {
        let dimensions = dimensions
            .into_iter()
            .map(|(key, value)| bounded(&format!("the {name} dimension {key}"), value).map(|value| (key, value)))
            .collect::<Result<HashMap<String, ShortText>, String>>()
            .inspect_err(|error| eprintln!("tumpel-supervisor: dropping a {name} point: {error}"))
            .ok()?;
        Some(Point::Turn(TurnPoint {
            at: now(),
            dimensions,
            metric: metric(name)?,
            session_id: EntityId::try_from(self.fields.get("session_id")?.as_str()).ok()?,
            turn_id: EntityId::try_from(self.fields.get("turn_id")?.as_str()).ok()?,
            value,
        }))
    }
}

struct Turn(Opened);
struct Phased(Opened);

struct TelemetryLayer(Telemetry);

fn declared(metric: &GuestMetric) -> Option<MetricInstrument> {
    Some(MetricInstrument {
        metric: metric.metric.clone(),
        unit: metric.unit.as_str().try_into().ok()?,
        kind: metric.kind.clone(),
        description: metric.description.as_str().try_into().ok()?,
    })
}

pub fn guest_metrics(metrics: &[GuestMetric]) {
    Span::current().with_subscriber(|(id, dispatch)| {
        let (Some(TelemetryLayer(telemetry)), Some(registry)) = (
            dispatch.downcast_ref::<TelemetryLayer>(),
            dispatch.downcast_ref::<Registry>(),
        ) else {
            return;
        };
        let Some(turn) = registry
            .span(id)
            .and_then(|span| span.scope().find(|span| span.name() == TURN))
        else {
            return;
        };
        let extensions = turn.extensions();
        let Some(Turn(opened)) = extensions.get::<Turn>() else {
            return;
        };
        for metric in metrics {
            if matches!(metric.kind, MetricKind::Unrecognised(_)) {
                tumpel_channel::update_required();
                continue;
            }
            let Some(instrument) = declared(metric) else {
                continue;
            };
            let mut dimensions = opened.dimensions_of(&GUEST_DIMENSIONS);
            dimensions.extend(
                metric
                    .dimensions
                    .iter()
                    .map(|(key, value)| (key.clone(), value.to_string())),
            );
            let Some(point) = opened.point_with(metric.metric.as_str(), metric.value, dimensions) else {
                continue;
            };
            telemetry
                .buffer()
                .declared
                .insert(instrument.metric.clone(), instrument);
            telemetry.push(point);
        }
    });
}

pub fn install() -> Result<Telemetry, String> {
    let telemetry = Telemetry::new(BUFFER_POINTS);
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(TelemetryLayer(telemetry.clone())))
        .map_err(|error| {
            format!("installing the telemetry layer failed: {error}; start the supervisor once per process")
        })?;
    Ok(telemetry)
}

impl<S> Layer<S> for TelemetryLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let metadata = attrs.metadata();
        if metadata.target() != TARGET {
            return;
        }
        let Some(span) = ctx.span(id) else {
            return;
        };
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        let opened = Opened {
            at: Instant::now(),
            fields: fields.0,
        };
        match metadata.name() {
            TURN => span.extensions_mut().insert(Turn(opened)),
            PHASE => span.extensions_mut().insert(Phased(opened)),
            _ => {}
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        let mut fields = Fields::default();
        values.record(&mut fields);
        let mut extensions = span.extensions_mut();
        if let Some(Turn(opened)) = extensions.get_mut::<Turn>() {
            opened.fields.extend(fields.0);
        } else if let Some(Phased(opened)) = extensions.get_mut::<Phased>() {
            opened.fields.extend(fields.0);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if event.metadata().target() == "tumpel::compatibility" {
            eprintln!("{}", tumpel_channel::UPDATE_REQUIRED);
            return;
        }
        if event.metadata().target() != TARGET {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        let Some(outcome) = fields.0.remove(OUTCOME) else {
            return;
        };
        let Some(turn) = ctx
            .event_scope(event)
            .and_then(|mut scope| scope.find(|span| span.name() == TURN))
        else {
            return;
        };
        let mut extensions = turn.extensions_mut();
        if let Some(Turn(opened)) = extensions.get_mut::<Turn>() {
            opened.fields.entry(OUTCOME).or_insert(outcome);
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };
        let extensions = span.extensions();
        if let Some(Turn(opened)) = extensions.get::<Turn>() {
            if let Some(point) = opened.point("turn.end", 1.0, None) {
                self.0.push(point);
            }
            return;
        }
        let Some(Phased(phase)) = extensions.get::<Phased>() else {
            return;
        };
        if !phase.fields.contains_key(REACHED) {
            return;
        }
        let Some(turn) = span.scope().skip(1).find(|parent| parent.name() == TURN) else {
            return;
        };
        let parent = turn.extensions();
        let Some(Turn(opened)) = parent.get::<Turn>() else {
            return;
        };
        let Some(named) = phase.fields.get(PHASE) else {
            return;
        };
        let points = [
            opened.point("turn.phase", phase.at.elapsed().as_secs_f64(), Some((PHASE, named))),
            (named == Phase::FirstOutput.named())
                .then(|| opened.point("turn.startup", opened.at.elapsed().as_secs_f64(), None))
                .flatten(),
        ];
        for point in points.into_iter().flatten() {
            self.0.push(point);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tracing::Instrument;
    use tracing_subscriber::layer::SubscriberExt;
    use tumpel_api::types::{
        ChannelTurnComplete, ChannelTurnCompleteType, SamplePoint, Sequence, ShortText, TurnPoint,
    };

    use super::{instrument, metric, Phase, Point, Telemetry, TelemetryLayer, INSTRUMENTS};

    #[test]
    fn guest_metrics_land_as_turn_points_with_the_turn_dimensions_and_declare_their_instruments() {
        let telemetry = Telemetry::new(100);
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(TelemetryLayer(telemetry.clone())));
        let metrics: Vec<tumpel_api::types::GuestMetric> = serde_json::from_value(serde_json::json!([
            { "metric": "turn.tokens", "unit": "{token}", "kind": "Turn", "description": "Tokens", "value": 12.0, "dimensions": { "tokenKind": "Input" } },
            { "metric": "turn.cost", "unit": "uUSD", "kind": "Turn", "description": "Cost", "value": 233.0, "dimensions": {} }
        ]))
        .expect("guest metrics");
        super::turn("turn-1", "session-1", "ClaudeCode", "1.0.0", "claude-test").in_scope(|| {
            let _phase = super::phase(Phase::FirstOutput).entered();
            super::guest_metrics(&metrics);
        });

        let points = turn_points(&telemetry);
        let tokens = points
            .iter()
            .find(|point| point.metric.as_str() == "turn.tokens")
            .expect("turn.tokens");
        assert_eq!(tokens.value, 12.0);
        assert_eq!(tokens.dimensions.get("tokenKind").map(ShortText::as_str), Some("Input"));
        assert_eq!(
            tokens.dimensions.get("model").map(ShortText::as_str),
            Some("claude-test")
        );
        assert_eq!(tokens.dimensions.len(), 3, "{:?}", tokens.dimensions);
        assert_eq!(tokens.turn_id.as_str(), "turn-1");
        let batch = telemetry.heartbeat_batch();
        let mut declared: Vec<(String, String)> = batch
            .instruments
            .iter()
            .map(|instrument| (instrument.metric.to_string(), instrument.unit.to_string()))
            .collect();
        declared.sort();
        assert_eq!(
            declared,
            [
                ("turn.cost".to_string(), "uUSD".to_string()),
                ("turn.end".to_string(), "{turn}".to_string()),
                ("turn.tokens".to_string(), "{token}".to_string())
            ]
        );
    }

    fn sample(value: f64) -> Point {
        Point::Sample(SamplePoint {
            at: super::now(),
            dimensions: std::collections::HashMap::new(),
            metric: metric("host.load.1m").expect("metric"),
            value,
        })
    }

    fn values(telemetry: &Telemetry) -> Vec<f64> {
        telemetry
            .buffer()
            .points
            .iter()
            .map(|point| match point {
                Point::Sample(point) => point.value,
                Point::Turn(point) => point.value,
            })
            .collect()
    }

    #[test]
    fn every_instrument_converts_to_its_typespec_shape() {
        for (name, ..) in INSTRUMENTS {
            assert!(instrument(&metric(name).expect(name)).is_some(), "{name}");
        }
    }

    #[test]
    fn a_full_buffer_drops_its_oldest_point_and_counts_the_drop() {
        let telemetry = Telemetry::new(3);
        for value in 1..=5 {
            telemetry.push(sample(f64::from(value)));
        }
        assert_eq!(values(&telemetry), [3.0, 4.0, 5.0]);
        assert_eq!(telemetry.buffer().dropped, 2);
    }

    fn turn(value: f64) -> Point {
        Point::Turn(TurnPoint {
            at: super::now(),
            dimensions: std::collections::HashMap::new(),
            metric: metric("turn.end").expect("metric"),
            session_id: "session-1".try_into().expect("session id"),
            turn_id: "turn-1".try_into().expect("turn id"),
            value,
        })
    }

    #[test]
    fn heartbeats_drain_known_points_in_batches_of_five_hundred() {
        let telemetry = Telemetry::new(5000);
        for value in 0..1200 {
            telemetry.push(sample(f64::from(value)));
            telemetry.push(turn(f64::from(value)));
        }
        let batches: Vec<_> = (0..6)
            .map(|_| {
                let batch = telemetry.heartbeat_batch();
                (batch.samples.len(), batch.turns.len())
            })
            .collect();
        assert_eq!(
            batches,
            [(250, 250), (250, 250), (250, 250), (250, 250), (200, 200), (0, 0)]
        );
    }

    fn turn_points(telemetry: &Telemetry) -> Vec<TurnPoint> {
        telemetry
            .buffer()
            .points
            .iter()
            .filter_map(|point| match point {
                Point::Turn(point) => Some(point.clone()),
                Point::Sample(_) => None,
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn a_turn_span_yields_reached_phases_its_startup_and_its_end_with_the_outcome() {
        let telemetry = Telemetry::new(100);
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(TelemetryLayer(telemetry.clone())));
        let root = super::turn("turn-1", "session-1", "ClaudeCode", "1.0.0", "claude-test");
        super::start_kind(&root, false);
        async {
            super::timed(Phase::Boot, tokio::time::sleep(Duration::from_secs(2)), |()| true).await;
            super::timed(Phase::Restore, tokio::time::sleep(Duration::from_secs(1)), |()| false).await;
            let first_output = super::phase(Phase::FirstOutput);
            tokio::time::advance(Duration::from_secs(3)).await;
            super::reached(&first_output);
            drop(first_output);
            super::ended(
                &ChannelTurnComplete {
                    completed_at: super::now(),
                    credential_file: None,
                    last_line_seq: Sequence(1),
                    turn_id: "turn-1".try_into().expect("turn id"),
                    type_: ChannelTurnCompleteType::TurnComplete,
                    warning: None,
                }
                .into(),
            );
        }
        .instrument(root)
        .await;

        let points = turn_points(&telemetry);
        let summary: Vec<(String, Option<String>, f64)> = points
            .iter()
            .map(|point| {
                (
                    point.metric.to_string(),
                    point.dimensions.get("phase").map(ToString::to_string),
                    point.value.round(),
                )
            })
            .collect();
        assert_eq!(
            summary,
            [
                ("turn.phase".to_string(), Some("Boot".to_string()), 2.0),
                ("turn.phase".to_string(), Some("FirstOutput".to_string()), 3.0),
                ("turn.startup".to_string(), None, 6.0),
                ("turn.end".to_string(), None, 1.0),
            ]
        );
        let end = points.last().expect("turn.end");
        assert_eq!(end.dimensions.get("outcome").map(ShortText::as_str), Some("Completed"));
        assert_eq!(end.dimensions.get("startKind").map(ShortText::as_str), Some("Cold"));
        assert_eq!(
            end.dimensions.get("harnessVersion").map(ShortText::as_str),
            Some("1.0.0")
        );
        assert_eq!(end.turn_id.as_str(), "turn-1");
        assert_eq!(end.session_id.as_str(), "session-1");
    }
}
