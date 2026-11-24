# Client-first compatibility

Clients ship before the control plane. A supervisor must keep doing known work when the control plane sends an enum, failure reason, pause reason or field it does not recognise. Decode received messages with the generated protocol types. Keep request validation strict.

An unknown value logs one plain line: `the control plane sent something this supervisor doesn't recognise; update the supervisor`. Never panic or expose a decode error for it. Ignore work that needs an unknown variant. Consume a sequenced unknown command and persist its cursor so the next known command can run. Preserve free-form failure and pause reasons and their known sequencing and pacing effects.

Test every received channel family through the real receiver with a fake clock. Put unknown variants and extra fields in visible test inputs. Assert the update line, acknowledgement progress and continued known work.

Dispatch a release candidate to deploy only after publication, uploaded assets, a tag naming the exact source SHA and successful checks for that SHA. Use only DEPLOY_DISPATCH_TOKEN for the dispatch. When it is empty, log a notice and skip successfully. A dispatch failure must never fail release publication. Read source evidence with the workflow's same-repository token. The release workflow starts only when the checks workflow completes successfully for a main-branch push in this repository. Check out and release that run's exact head SHA; release scripts never poll Actions.
