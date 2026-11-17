#!/bin/sh
here=$(dirname "$0")
real=
IFS=:
for dir in $PATH; do
  [ "$dir" = "$here" ] && continue
  [ -x "$dir/gh" ] && real=$dir/gh && break
done
unset IFS
[ -n "$real" ] || { echo "tumpel: the guest image has no gh on its PATH" >&2; exit 127; }
[ -n "$TUMPEL_GITHUB_TOKENS" ] || exec "$real" "$@"
repo=
previous=
for arg in "$@"; do
  case $previous in -R | --repo) repo=$arg ;; esac
  case $arg in
    --repo=*) repo=${arg#--repo=} ;;
    -R?*) repo=${arg#-R} ;;
  esac
  previous=$arg
done
if [ -z "$repo" ]; then
  case $1 in api) named=$2 ;; *) named=$3 ;; esac
  case $1:$named in
    *:https://github.com/*/?*) repo=${named#https://github.com/} ;;
    api:https://api.github.com/repos/*/?*) repo=${named#https://api.github.com/repos/} ;;
    api:repos/*/?* | api:/repos/*/?*) repo=${named#/} && repo=${repo#repos/} ;;
    repo:*/?*) repo=$named ;;
  esac
  case $repo in '{'*) repo= ;; esac
fi
[ -n "$repo" ] || repo=$GH_REPO
if [ -z "$repo" ]; then
  url=$(git remote get-url origin 2>/dev/null)
  case $url in *github.com[:/]*) repo=${url#*github.com[:/]} ;; esac
fi
owner=$(printf '%s' "${repo%%/*}" | tr '[:upper:]' '[:lower:]')
for pair in $TUMPEL_GITHUB_TOKENS; do
  if [ -n "$owner" ] && [ "${pair%%=*}" = "$owner" ]; then
    GH_TOKEN=${pair#*=}
    GITHUB_TOKEN=$GH_TOKEN
    export GH_TOKEN GITHUB_TOKEN
    break
  fi
done
exec "$real" "$@"
