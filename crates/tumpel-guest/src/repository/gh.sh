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
owner=${GH_REPO%%/*}
previous=
for arg in "$@"; do
  case $previous in -R | --repo) owner=${arg%%/*} ;; esac
  case $arg in
    --repo=*) repo=${arg#--repo=} && owner=${repo%%/*} ;;
    -R?*) repo=${arg#-R} && owner=${repo%%/*} ;;
  esac
  previous=$arg
done
if [ -z "$owner" ]; then
  for arg in "$@"; do
    target=
    case $1:$arg in
      *:https://github.com/?*) target=${arg#https://github.com/} ;;
      api:https://api.github.com/repos/?* | api:https://api.github.com/orgs/?* | api:https://api.github.com/users/?*)
        target=${arg#https://api.github.com/*/} ;;
      api:repos/?* | api:orgs/?* | api:users/?*) target=${arg#*/} ;;
      api:/repos/?* | api:/orgs/?* | api:/users/?*) target=${arg#/*/} ;;
      repo:-*) ;;
      repo:?*/?*) target=$arg ;;
    esac
    owner=${target%%/*}
    case $owner in '{'*) owner= ;; esac
    [ -n "$owner" ] && break
  done
fi
if [ -z "$owner" ]; then
  url=$(git remote get-url origin 2>/dev/null)
  case $url in *github.com[:/]*) repo=${url#*github.com[:/]} && owner=${repo%%/*} ;; esac
fi
[ -n "$owner" ] || exec "$real" "$@"
owner=$(printf '%s' "$owner" | tr '[:upper:]' '[:lower:]')
for pair in $TUMPEL_GITHUB_TOKENS; do
  if [ "${pair%%=*}" = "$owner" ]; then
    GH_TOKEN=${pair#*=}
    GITHUB_TOKEN=$GH_TOKEN
    export GH_TOKEN GITHUB_TOKEN
    exec "$real" "$@"
  fi
done
echo "tumpel: no GitHub App installation of this tenant covers github.com/$owner; install the App on $owner, then send the turn again" >&2
exit 1
