use std::num::NonZeroU32;

use chrono::{DateTime, FixedOffset};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{sleep_until, timeout};
use tumpel_api::types::{
    ChannelClaudeSetupBoxFailed, ChannelClaudeSetupBoxFailedType, ChannelClaudeSetupBoxOfferTokenCredential,
    ChannelClaudeSetupBoxUrl, ChannelClaudeSetupBoxUrlType, ClaudeSetupCodeText, ClaudeSetupFailureReason, EntityId,
    GuestClaudeSetupCode, GuestClaudeSetupCodeType, GuestClaudeSetupFailureReason, GuestClaudeSetupStart,
    GuestClaudeSetupStartTokenCredential, GuestClaudeSetupStartType, GuestInput, GuestMessage,
};

use super::{emit, instant_at, mismatch, now, Guest, Machine, Runner, Vmm};
use crate::boot::{BootOrder, Hold};
use crate::pool::Tap;
use crate::store::Store;

pub struct SetupOrder {
    pub claude_setup_id: EntityId,
    pub tenant_id: EntityId,
    pub box_id: EntityId,
    pub image: BootOrder,
    pub token_url: String,
    pub token_credential: ChannelClaudeSetupBoxOfferTokenCredential,
    pub expires_at: DateTime<FixedOffset>,
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

fn failed_in_guest(reason: GuestClaudeSetupFailureReason, detail: String) -> Ended {
    let reason = match reason {
        GuestClaudeSetupFailureReason::UrlNotPrinted => ClaudeSetupFailureReason::SetupUrlMissing,
        GuestClaudeSetupFailureReason::CodeRejected => ClaudeSetupFailureReason::CodeRejected,
        GuestClaudeSetupFailureReason::ClaudeCodeExited => ClaudeSetupFailureReason::ClaudeCodeExited,
        GuestClaudeSetupFailureReason::TokenNotStored => ClaudeSetupFailureReason::TokenNotStored,
        GuestClaudeSetupFailureReason::MintTimedOut => ClaudeSetupFailureReason::SetupTimedOut,
    };
    Ended::Failed(stop(reason, detail))
}

impl<V: Vmm, S: Store> Runner<V, S> {
    pub(super) async fn claude_setup(&self, tap: Tap, order: SetupOrder, inputs: UnboundedReceiver<SetupInput>) -> Tap {
        let id = order.claude_setup_id.clone();
        let failure = match self.boot_setup(&tap, &order).await {
            Ok((machine, guest, hold)) => {
                let (machine, ended) = self.follow_setup(machine, guest, &order, inputs).await;
                let tail = machine.console_tail();
                machine.stop().await;
                drop(hold);
                match ended {
                    Ended::Stored | Ended::Discarded => None,
                    Ended::Failed(failed) => Some(Stop {
                        detail: format!("{} Console: {tail}", failed.detail),
                        ..failed
                    }),
                }
            }
            Err(failed) => Some(failed),
        };
        if let Some(failed) = failure {
            emit(
                &self.out,
                ChannelClaudeSetupBoxFailed {
                    claude_setup_id: id,
                    detail: failed.detail,
                    failed_at: now(),
                    reason: failed.reason,
                    type_: ChannelClaudeSetupBoxFailedType::ClaudeSetupBoxFailed,
                },
            );
        }
        tap
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
                        .hand_setup(machine, guest, order)
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
        machine.stop().await;
        Err(missing(format!("{failed}. Console: {tail}")))
    }

    async fn hand_setup(
        &self,
        machine: V::Machine,
        mut guest: <V::Machine as Machine>::Guest,
        order: &SetupOrder,
    ) -> Result<(V::Machine, <V::Machine as Machine>::Guest), Stop> {
        let token_credential: Result<GuestClaudeSetupStartTokenCredential, _> =
            order.token_credential.to_string().try_into();
        let Ok(token_credential) = token_credential else {
            machine.stop().await;
            return Err(stop(
                ClaudeSetupFailureReason::SetupUrlMissing,
                format!("the offer of Claude setup {} carried a token credential the guest protocol refuses; update the supervisor and the control plane together", *order.claude_setup_id),
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
            machine.stop().await;
            return Err(stop(
                ClaudeSetupFailureReason::SetupUrlMissing,
                format!(
                    "handing Claude setup {} to the guest failed: {error}. Console: {tail}",
                    *order.claude_setup_id
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
    ) -> (V::Machine, Ended) {
        let deadline = instant_at(order.expires_at);
        let id = &order.claude_setup_id;
        loop {
            let next = tokio::select! {
                () = sleep_until(deadline) => Next::Deadline,
                input = inputs.recv() => Next::Input(input),
                received = guest.receive() => Next::Received(received),
            };
            let ended = match next {
                Next::Deadline => Some(Ended::Failed(stop(
                    ClaudeSetupFailureReason::SetupTimedOut,
                    format!(
                        "Claude setup {} ran past {}, the end of its box; start the Claude setup again",
                        **id,
                        order.expires_at.to_rfc3339()
                    ),
                ))),
                Next::Input(Some(SetupInput::Code {
                    code,
                    mint_budget_seconds,
                })) => {
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
                    Some(failed_in_guest(failed.reason, failed.detail))
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
