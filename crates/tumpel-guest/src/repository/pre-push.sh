#!/bin/sh
default=$(git symbolic-ref --quiet --short refs/remotes/origin/HEAD) || {
  echo "tumpel: the clone names no default branch; run git remote set-head origin --auto, then push again" >&2
  exit 1
}
default=${default#origin/}
while read -r local_ref local_sha remote_ref remote_sha; do
  if [ "$remote_ref" = "refs/heads/$default" ]; then
    echo "Tumpel pushes to a branch and opens a draft pull request; the default branch is not pushed from a box" >&2
    exit 1
  fi
  case "$local_sha" in *[!0]*) ;; *) continue ;; esac
  case "$remote_sha" in *[!0]*) ;; *) continue ;; esac
  if ! git merge-base --is-ancestor "$remote_sha" "$local_sha" 2>/dev/null; then
    echo "Tumpel does not force-push from a box; $remote_ref holds commits this push would drop, so push your commits to a new branch" >&2
    exit 1
  fi
done
exit 0
