# Commits and pull requests

1. Start from the latest `origin/main` on a new branch.
2. A pull request does one thing.
3. `npm run check` passes before every commit. `npm install` enables the pre-commit hook that runs it. Never bypass the hook.
4. A commit subject and a PR title are conventional commits: `type(scope): summary`, imperative, lower case, 72 characters or fewer, no full stop. The type is feat, fix, chore, docs, refactor, perf, test, build, ci, style or revert.
5. A commit body gives the reason for the change. Skip it when the diff is obvious.
6. Open the PR as a draft. The body is exactly this, one or two sentences a section:

   ```text
   ### What?
   ### Why?
   ### How?

   Closes #123
   ```

7. A PR body never names a file, a function or a line number. It names the behaviour that changed.
8. Never change `.github/workflows/` without explicit permission.
9. When CI fails, fix the code. Never add a workaround to CI and never skip a test to turn a build green.
10. When a security audit fails on a CVE, upgrade to the fixed version, through `overrides` for a transitive dependency. Waive the advisory only when no fixed version exists.
11. Keep CI fast. Measure the minutes per job before and after any change to CI.
