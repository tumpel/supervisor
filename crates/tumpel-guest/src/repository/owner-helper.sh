!f() {
  test "$1" = get || return 0
  path=
  while IFS= read -r line; do
    [ -z "$line" ] && break
    case $line in path=*) path=${line#path=} ;; esac
  done
  owner=$(printf '%s' "${path%%/*}" | tr '[:upper:]' '[:lower:]')
  for pair in $TUMPEL_GITHUB_TOKENS; do
    if [ "${pair%%=*}" = "$owner" ]; then
      printf 'username=x-access-token\npassword=%s\n' "${pair#*=}"
      return 0
    fi
  done
  echo "tumpel: no GitHub App installation of this tenant covers github.com/$owner; install the App on $owner, then send the turn again" >&2
  return 1
}; f
