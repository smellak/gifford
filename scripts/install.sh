#!/bin/sh
# Binary bootstrap. System-user, tmux privileges and service activation belong
# to the reviewed machine installer; this script does not grant those powers.
set -eu

fail() { printf 'gifford install: %s\n' "$*" >&2; exit 1; }
usage() {
    printf '%s\n' \
        'Usage: sh install.sh --version vX.Y.Z [--bin-dir DIR] [--replace] [--dry-run]' \
        'Installs the Linux release binary after verifying checksums.txt.' \
        'No user, sudoers rule, configuration or service is changed.' \
        'Machine-wide installation (--system) is not implemented yet.'
}

release_tag=
bin_dir="${HOME:?HOME must be set}/.local/bin"
replace=false
dry_run=false
while [ "$#" -gt 0 ]; do
    case "$1" in
        --version|--bin-dir)
            [ "$#" -ge 2 ] || fail "$1 needs a value"
            case "$1" in
                --version) release_tag=$2 ;;
                --bin-dir) bin_dir=$2 ;;
            esac
            shift 2 ;;
        --replace) replace=true; shift ;;
        --dry-run) dry_run=true; shift ;;
        --system) fail 'system installation is not implemented; no changes made' ;;
        --help|-h) usage; exit 0 ;;
        *) fail "unknown argument: $1" ;;
    esac
done

# Pin a release, never silently turn a build of main into an installed version.
case "$release_tag" in
    v[0-9]*) ;;
    *) fail '--version must name a release tag such as v0.1.0' ;;
esac
case "$release_tag" in
    *[!a-zA-Z0-9.+-]*) fail 'invalid release tag' ;;
esac
case "$bin_dir" in
    /*) ;;
    *) fail '--bin-dir must be an absolute directory' ;;
esac
[ "$(uname -s)" = Linux ] || fail 'release binaries currently support Linux only'
case "$(uname -m)" in
    x86_64|amd64) arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    *) fail 'supported architectures: amd64, arm64' ;;
esac

archive="gifford_${release_tag#v}_linux_${arch}.tar.gz"
release_url="https://github.com/smellak/gifford/releases/download/$release_tag"
printf 'Release: %s\nBinary:  %s/gifford\n' "$release_tag" "$bin_dir"
if "$dry_run"; then
    printf 'Would download %s/%s and verify checksums.txt. No changes made.\n' "$release_url" "$archive"
    exit 0
fi
for tool in curl sha256sum tar mktemp install awk; do
    command -v "$tool" >/dev/null 2>&1 || fail "required tool not found: $tool"
done
[ ! -d "$bin_dir/gifford" ] || fail 'destination is a directory'
if ! "$replace" && { [ -e "$bin_dir/gifford" ] || [ -L "$bin_dir/gifford" ]; }; then
    fail 'binary already exists; use --replace to update it explicitly'
fi

staging=$(mktemp -d)
dest_staging=
cleanup() {
    # These paths are created only by mktemp in this invocation.
    [ -z "$dest_staging" ] || rm -f -- "$dest_staging"
    [ -z "$staging" ] || rm -rf -- "$staging"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' \
    "$release_url/$archive" -o "$staging/$archive"
curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' \
    "$release_url/checksums.txt" -o "$staging/checksums.txt"
awk -v name="$archive" '$2 == name { print $1 "  " name; found++ } END { if (found != 1) exit 1 }' \
    "$staging/checksums.txt" > "$staging/selected-checksum.txt" || fail 'release checksum missing or duplicated'
(cd "$staging" && sha256sum --check --status selected-checksum.txt) || fail 'release checksum mismatch'
# Extract only the binary, not arbitrary paths from the archive.
tar -xzf "$staging/$archive" -C "$staging" -- gifford
[ -f "$staging/gifford" ] && [ ! -L "$staging/gifford" ] || fail 'archive has no regular gifford binary'
mkdir -p -- "$bin_dir"
dest_staging=$(mktemp "$bin_dir/.gifford-install.XXXXXXXX")
install -m 0755 -- "$staging/gifford" "$dest_staging"
mv -fT -- "$dest_staging" "$bin_dir/gifford"
dest_staging=
printf 'Binary installed. Add %s to PATH.\n' "$bin_dir"
printf '%s\n' 'Machine service setup is not included yet; no agents or services were restarted.'
