#[derive(Clone, Default)]
pub(crate) struct UpdateLogs(pub std::sync::Arc<std::sync::Mutex<Vec<String>>>);

impl tracing::field::Visit for UpdateLogs {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0.lock().expect("logs").push(format!("{value:?}"));
        }
    }
}

impl tracing_subscriber::Layer<tracing_subscriber::Registry> for UpdateLogs {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _: tracing_subscriber::layer::Context<'_, tracing_subscriber::Registry>,
    ) {
        if event.metadata().target() == "tumpel::compatibility" {
            event.record(&mut self.clone());
        }
    }
}
