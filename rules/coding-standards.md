# Coding standards

1. Make the smallest change that delivers the task. Add no extra options, alarms or hardening.
2. No comments in code. Names carry the meaning. `npm run comments:check` enforces it. Design rationale goes in the commit message.
3. Return early. Never nest where a guard clause works.
4. Never re-implement what the repo, the standard library or the platform already provides. Search before you add a helper.
5. Inline machinery with one caller: an interface with one implementation, a wrapper that only delegates, a config nobody sets.
6. Fix a bug in the shared function every caller uses. Never patch only the call site where it surfaced.
7. Fix the root cause. Never layer a workaround on an earlier workaround.
8. Run tools through `npm run <script>`. Add a script to `package.json` when one is missing.
9. Repo utilities are Node `.mjs` scripts in `npm-scripts/`, one job each. Never write Python.
10. Never commit a token, key or secret. `npm run secrets:check` scans for them.
11. Keep durable state in a local database: anything that must survive a restart or a handover, such as what a box holds, what was backed up and what was asked of the control plane. Never keep it in process memory or in a flat JSON or text file. Process memory dies with a restart or a handover, and a flat file has no atomic updates or queries.
