use std::num::NonZeroU32;
use std::time::Duration;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{sleep_until, timeout, Instant};
use tumpel_api::types::{
    ChannelClaudeSetupBoxFailed, ChannelClaudeSetupBoxFailedType, ChannelClaudeSetupBoxOfferTokenCredential,
    ChannelClaudeSetupBoxUrl, ChannelClaudeSetupBoxUrlType, ClaudeSetupCodeText, ClaudeSetupFailureReason, EntityId,
    GuestClaudeSetupCode, GuestClaudeSetupCodeType, GuestClaudeSetupFailureReason, GuestClaudeSetupStart,
    GuestClaudeSetupStartTokenCredential, GuestClaudeSetupStartType, GuestInput, GuestMessage, HttpUrl,
};

use super::{emit, end_box, mismatch, now, BoxEnd, Guest, Machine, Runner, Vmm};
use crate::boot::{BootOrder, Hold};
use crate::bounded::reported;
use crate::pool::Tap;
use crate::store::Store;

const MINT_GRACE: Duration = Duration::from_secs(10);

fn url_missing(id: &EntityId, url: Duration) -> Stop {
    stop(
        ClaudeSetupFailureReason::SetupUrlMissing,
        format!(
            "the box of Claude setup {} printed no sign-in URL within urlSeconds {} of its offer; check that the host fetched the pinned image and can reach its sign-in page, then start the Claude setup again",
            **id,
            url.as_secs()
        ),
    )
}

fn code_missing(id: &EntityId, code: Duration) -> Stop {
    stop(
        ClaudeSetupFailureReason::SetupTimedOut,
        format!(
            "the sign-in of Claude setup {} did not finish within codeSeconds {} of its sign-in URL; start the Claude setup again, open the new URL and enter the new code",
            **id,
            code.as_secs()
        ),
    )
}

fn mint_missing(id: &EntityId, seconds: NonZeroU32) -> Stop {
    stop(
        ClaudeSetupFailureReason::SetupTimedOut,
        format!(
            "the box of Claude setup {} reported nothing within mintBudgetSeconds {seconds} plus {} s of the code; start the Claude setup again",
            **id,
            MINT_GRACE.as_secs()
        ),
    )
}

pub struct SetupOrder {
    pub claude_setup_id: EntityId,
    pub tenant_id: EntityId,
    pub box_id: EntityId,
    pub image: BootOrder,
    pub token_url: HttpUrl,
    pub token_credential: ChannelClaudeSetupBoxOfferTokenCredential,
    pub url: Duration,
    pub code: Duration,
}

pub enum SetupCommand {
    Start(Box<SetupOrder>),
    Code {
        claude_setup_id: EntityId,
        code: ClaudeSetupCodeText,
        mint_budget_seconds: NonZeroU32,
    },
    Discard {
        claude_setup_id: EntityId,
    },
}

pub enum SetupInput {
    Code {
        code: ClaudeSetupCodeText,
        mint_budget_seconds: NonZeroU32,
    },
    Discard,
}

struct Stop {
    reason: ClaudeSetupFailureReason,
    detail: String,
}

fn stop(reason: ClaudeSetupFailureReason, detail: impl Into<String>) -> Stop {
    Stop {
        reason,
        detail: detail.into(),
    }
}

enum Next {
    Deadline,
    Input(Option<SetupInput>),
    Received(Result<GuestMessage, String>),
}

enum Ended {
    Stored,
    Discarded,
    Failed(Stop),
}

pub(crate) fn then(detail: String, stopped: Result<(), String>) -> String {
    match stopped {
        Ok(()) => detail,
        Err(error) => format!("{detail}; then {error}"),
    }
}

fn failed_in_guest(reason: GuestClaudeSetupFailureReason, detail: String) -> Ended {
    let reason =
        match reason {
            GuestClaudeSetupFailureReason::Unrecognised(reason) => return Ended::Failed(stop(
                ClaudeSetupFailureReason::ClaudeCodeExited,
                format!(
                    "the guest failed the setup with reason {reason}, which this supervisor does not know: {detail}"
                ),
            )),
            GuestClaudeSetupFailureReason::UrlNotPrinted => ClaudeSetupFailureReason::SetupUrlMissing,
            GuestClaudeSetupFailureReason::CodeRejected => ClaudeSetupFailureReason::CodeRejected,
            GuestClaudeSetupFailureReason::ClaudeCodeExited => ClaudeSetupFailureReason::ClaudeCodeExited,
            GuestClaudeSetupFailureReason::TokenNotStored => ClaudeSetupFailureReason::TokenNotStored,
            GuestClaudeSetupFailureReason::MintTimedOut => ClaudeSetupFailureReason::SetupTimedOut,
        };
    Ended::Failed(stop(reason, detail))
}

impl<V: Vmm, S: Store> Runner<V, S> {
    pub(super) async fn claude_setup(
        &self,
        tap: Tap,
        order: SetupOrder,
        inputs: UnboundedReceiver<SetupInput>,
    ) -> (Tap, Result<(), String>) {
        let id = order.claude_setup_id.clone();
        let offered = Instant::now();
        let (failure, stopped) = match self.boot_setup(&tap, &order).await {
            Ok((machine, guest, hold)) => {
                let (machine, ended) = self.follow_setup(machine, guest, &order, inputs, offered).await;
                let tail = machine.console_tail();
                let stopped = end_box(machine, tap.index(), &id, &order.box_id, BoxEnd::SetupEnded).await;
                drop(hold);
                let failure = match ended {
                    Ended::Stored | Ended::Discarded => None,
                    Ended::Failed(failed) => Some(Stop {
                        detail: format!("{} Console: {tail}", failed.detail),
                        ..failed
                    }),
                };
                (failure, stopped)
            }
            Err(failed) => (Some(failed), Ok(())),
        };
        if let Some(failed) = failure {
            emit(
                &self.out,
                ChannelClaudeSetupBoxFailed {
                    claude_setup_id: id,
                    detail: reported("the Claude setup failure detail", &failed.detail),
                    failed_at: now(),
                    reason: failed.reason,
                    type_: ChannelClaudeSetupBoxFailedType::ClaudeSetupBoxFailed,
                },
            );
        }
        (tap, stopped)
    }

    async fn boot_setup(
        &self,
        tap: &Tap,
        order: &SetupOrder,
    ) -> Result<(V::Machine, <V::Machine as Machine>::Guest, Hold), Stop> {
        let missing = |detail: String| stop(ClaudeSetupFailureReason::SetupUrlMissing, detail);
        let (image, hold) = self
            .boot(&order.image, |_| {})
            .await
            .map_err(|refusal| missing(refusal.detail))?;
        let mut machine = self
            .vmm
            .machine(&order.claude_setup_id, tap)
            .map_err(|error| missing(format!("preparing setup box {}: {error}", *order.box_id)))?;
        let seconds = self.settings.budgets.boot.as_secs();
        let failed = match timeout(self.settings.budgets.boot, machine.start(&image, None)).await {
            Ok(Ok((guest, hello))) => match mismatch(&hello, &image) {
                None => {
                    return self
                        .hand_setup(machine, guest, tap.index(), order)
                        .await
                        .map(|(machine, guest)| (machine, guest, hold))
                }
                Some(mismatch) => mismatch,
            },
            Ok(Err(error)) => format!("booting setup box {} failed: {error}", *order.box_id),
            Err(_) => format!(
                "setup box {} did not say GuestHello within {seconds} s of booting; start the Claude setup again",
                *order.box_id
            ),
        };
        let tail = machine.console_tail();
        let stopped = end_box(
            machine,
            tap.index(),
            &order.claude_setup_id,
            &order.box_id,
            BoxEnd::SetupBootFailed,
        )
        .await;
        Err(missing(then(format!("{failed}. Console: {tail}"), stopped)))
    }

    async fn hand_setup(
        &self,
        machine: V::Machine,
        mut guest: <V::Machine as Machine>::Guest,
        index: u8,
        order: &SetupOrder,
    ) -> Result<(V::Machine, <V::Machine as Machine>::Guest), Stop> {
        let token_credential: Result<GuestClaudeSetupStartTokenCredential, _> =
            order.token_credential.to_string().try_into();
        let Ok(token_credential) = token_credential else {
            let stopped = end_box(
                machine,
                index,
                &order.claude_setup_id,
                &order.box_id,
                BoxEnd::SetupNotHanded,
            )
            .await;
            return Err(stop(
                ClaudeSetupFailureReason::SetupUrlMissing,
                then(format!("the offer of Claude setup {} carried a token credential the guest protocol refuses; update the supervisor and the control plane together", *order.claude_setup_id), stopped),
            ));
        };
        let start = GuestInput::ClaudeSetupStart(GuestClaudeSetupStart {
            claude_setup_id: order.claude_setup_id.clone(),
            tenant_id: order.tenant_id.clone(),
            token_credential,
            token_url: order.token_url.clone(),
            type_: GuestClaudeSetupStartType::ClaudeSetupStart,
        });
        if let Err(error) = guest.send(&start).await {
            let tail = machine.console_tail();
            let stopped = end_box(
                machine,
                index,
                &order.claude_setup_id,
                &order.box_id,
                BoxEnd::SetupNotHanded,
            )
            .await;
            return Err(stop(
                ClaudeSetupFailureReason::SetupUrlMissing,
                then(
                    format!(
                        "handing Claude setup {} to the guest failed: {error}. Console: {tail}",
                        *order.claude_setup_id
                    ),
                    stopped,
                ),
            ));
        }
        Ok((machine, guest))
    }

    async fn follow_setup(
        &self,
        machine: V::Machine,
        mut guest: <V::Machine as Machine>::Guest,
        order: &SetupOrder,
        mut inputs: UnboundedReceiver<SetupInput>,
        offered: Instant,
    ) -> (V::Machine, Ended) {
        let id = &order.claude_setup_id;
        let (mut phase_end, mut on_expiry) = (offered + order.url, url_missing(id, order.url));
        loop {
            let next = tokio::select! {
                () = sleep_until(phase_end) => Next::Deadline,
                input = inputs.recv() => Next::Input(input),
                received = guest.receive() => Next::Received(received),
            };
            let ended = match next {
                Next::Deadline => return (machine, Ended::Failed(on_expiry)),
                Next::Input(Some(SetupInput::Code {
                    code,
                    mint_budget_seconds,
                })) => {
                    phase_end = Instant::now() + Duration::from_secs(u64::from(mint_budget_seconds.get())) + MINT_GRACE;
                    on_expiry = mint_missing(id, mint_budget_seconds);
                    let message = GuestInput::ClaudeSetupCode(GuestClaudeSetupCode {
                        claude_setup_id: id.clone(),
                        code,
                        mint_budget_seconds,
                        type_: GuestClaudeSetupCodeType::ClaudeSetupCode,
                    });
                    guest.send(&message).await.err().map(|error| {
                        Ended::Failed(stop(
                            ClaudeSetupFailureReason::CodeRejected,
                            format!(
                                "handing the code of Claude setup {} to the guest failed: {error}; start the Claude setup again",
                                **id
                            ),
                        ))
                    })
                }
                Next::Input(Some(SetupInput::Discard) | None) => Some(Ended::Discarded),
                Next::Received(Ok(GuestMessage::ClaudeSetupUrl(printed))) if printed.claude_setup_id == *id => {
                    phase_end = Instant::now() + order.code;
                    on_expiry = code_missing(id, order.code);
                    emit(
                        &self.out,
                        ChannelClaudeSetupBoxUrl {
                            claude_setup_id: id.clone(),
                            sign_in_url: printed.sign_in_url,
                            type_: ChannelClaudeSetupBoxUrlType::ClaudeSetupBoxUrl,
                            user_code: printed.user_code.and_then(|code| code.to_string().try_into().ok()),
                        },
                    );
                    None
                }
                Next::Received(Ok(GuestMessage::ClaudeSetupStored(stored))) if stored.claude_setup_id == *id => {
                    Some(Ended::Stored)
                }
                Next::Received(Ok(GuestMessage::ClaudeSetupFailed(failed))) if failed.claude_setup_id == *id => {
                    Some(failed_in_guest(failed.reason, failed.detail.into()))
                }
                Next::Received(Ok(_)) => None,
                Next::Received(Err(error)) => Some(Ended::Failed(stop(
                    ClaudeSetupFailureReason::SetupUrlMissing,
                    format!(
                        "the guest connection of Claude setup {} dropped: {error}; start the Claude setup again",
                        **id
                    ),
                ))),
            };
            if let Some(ended) = ended {
                return (machine, ended);
            }
        }
    }
}
