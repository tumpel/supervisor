# Writing

## Text a user reads

This covers the dashboard, CLI output, error messages, emails and docs pages.

1. Say what happened and what to do next, in plain words a newcomer understands on first read.
2. Never show internal references: no requirement, gap, EDD, ADR or RFC ids, no issue numbers, no migration or operation names.
3. Never turn a limit of the implementation into an instruction for the user. Fix the product instead.
4. Write short sentences, with no hedging and no jargon the user did not bring.
5. `npm run prose:check` catches internal ids and "until X is built" caveats in user-facing code.

## Docs, issues and PR bodies

1. `.claude/agents/writer.md` is the full standard: voice, word budgets, banned words and the cut passes.
2. Write short declarative sentences in the active voice. Name the actor.
3. Use `must`, `never` or `always` for an obligation. Never write `should` or `could`.
4. No em dashes, en dashes, emoji or decorative bold.
5. Describe what the system does now. History belongs in the changelog.
6. Repo docs describe the design on its own terms. They never reference the projects it came from.
7. Never hard-wrap a paragraph.
