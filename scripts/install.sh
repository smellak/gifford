#!/bin/sh
# Binary bootstrap. --system-node additionally installs the reviewed machine
# service; bridge accounts and tmux privileges remain deployment concerns.
set -eu

fail() { printf 'gifford install: %s\n' "$*" >&2; exit 1; }
usage() {
    printf '%s\n' \
        'Usage: sh install.sh --version vX.Y.Z [--bin-dir DIR] [--replace] [--dry-run]' \
        '                     [--system-node [--node-id ID]]' \
        'Installs the Linux release binary after verifying checksums.txt.' \
        'By default no user, configuration or service is changed.' \
        '--system-node creates a fresh gifford account, configuration and service.' \
        '--node-id defaults to node-<hostname>.' \
        'Bridge accounts, agent processes and tmux privileges are never changed.'
}

release_tag=
bin_dir="${HOME:?HOME must be set}/.local/bin"
bin_dir_set=false
replace=false
dry_run=false
system_node=false
node_id=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --version|--bin-dir|--node-id)
            [ "$#" -ge 2 ] || fail "$1 needs a value"
            case "$1" in
                --version) release_tag=$2 ;;
                --bin-dir) bin_dir=$2; bin_dir_set=true ;;
                --node-id) node_id=$2 ;;
            esac
            shift 2 ;;
        --replace) replace=true; shift ;;
        --dry-run) dry_run=true; shift ;;
        --system-node) system_node=true; shift ;;
        --system) fail 'system installation is not implemented; no changes made' ;;
        --help|-h) usage; exit 0 ;;
        *) fail "unknown argument: $1" ;;
    esac
done

if "$system_node" && ! "$bin_dir_set"; then
    bin_dir=/usr/local/bin
fi
[ -z "$node_id" ] || "$system_node" || fail '--node-id requires --system-node'

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
if "$system_node"; then
    [ "$bin_dir" = /usr/local/bin ] ||
        fail '--system-node requires --bin-dir /usr/local/bin'
    if [ -z "$node_id" ]; then
        node_id="node-$(uname -n)"
    fi
    case "$node_id" in
        ""|[!a-z0-9]*|*[!a-z0-9._-]*)
            fail '--node-id must start with a lowercase letter or digit and contain only lowercase letters, digits, dot, underscore or hyphen' ;;
    esac
    [ "${#node_id}" -le 64 ] || fail '--node-id must be at most 64 characters'
fi
[ "$(uname -s)" = Linux ] || fail 'release binaries currently support Linux only'
case "$(uname -m)" in
    x86_64|amd64) arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    *) fail 'supported architectures: amd64, arm64' ;;
esac

archive="gifford_${release_tag#v}_linux_${arch}.tar.gz"
release_url="https://github.com/smellak/gifford/releases/download/$release_tag"
printf 'Release: %s\nBinary:  %s/gifford\n' "$release_tag" "$bin_dir"
if "$system_node"; then
    printf 'Node:    %s (system service)\n' "$node_id"
fi
if "$dry_run"; then
    printf 'Would download %s/%s and verify checksums.txt. No changes made.\n' "$release_url" "$archive"
    if "$system_node"; then
        printf '%s\n' 'Would create the gifford system account, configuration and service. No changes made.'
    fi
    exit 0
fi
for tool in curl sha256sum tar mktemp install awk mkdir mv rm; do
    command -v "$tool" >/dev/null 2>&1 || fail "required tool not found: $tool"
done
if "$system_node"; then
    for tool in cat chmod chown getent groupdel id ln rmdir systemctl useradd userdel; do
        command -v "$tool" >/dev/null 2>&1 || fail "required tool not found: $tool"
    done
    [ "$(id -u)" -eq 0 ] || fail '--system-node requires root; run the installer with sudo'
fi
if ! "$system_node"; then
    [ ! -d "$bin_dir/gifford" ] || fail 'destination is a directory'
    if ! "$replace" && { [ -e "$bin_dir/gifford" ] || [ -L "$bin_dir/gifford" ]; }; then
        fail 'binary already exists; use --replace to update it explicitly'
    fi
fi

path_exists() {
    [ -e "$1" ] || [ -L "$1" ]
}

preflight_system_node() {
    getent passwd gifford >/dev/null 2>&1 &&
        fail 'system user gifford already exists; no changes made'
    getent group gifford >/dev/null 2>&1 &&
        fail 'system group gifford already exists; no changes made'
    [ -d "$bin_dir" ] && [ ! -L "$bin_dir" ] ||
        fail '/usr/local/bin must already be a directory, not a symlink'

    for path in \
        "$bin_dir/gifford" \
        /etc/gifford \
        /var/lib/gifford \
        /run/gifford \
        /etc/systemd/system/gifford.service \
        /etc/systemd/system/gifford.service.d \
        /etc/systemd/system/multi-user.target.wants/gifford.service \
        /run/systemd/system/gifford.service \
        /run/systemd/system/gifford.service.d \
        /usr/local/lib/systemd/system/gifford.service \
        /usr/local/lib/systemd/system/gifford.service.d \
        /usr/lib/systemd/system/gifford.service \
        /usr/lib/systemd/system/gifford.service.d \
        /lib/systemd/system/gifford.service \
        /lib/systemd/system/gifford.service.d
    do
        ! path_exists "$path" || fail "system-node target already exists: $path; no changes made"
    done

    unit_state=$(systemctl show --property=LoadState --value gifford.service 2>/dev/null) ||
        fail 'cannot query systemd for gifford.service; no changes made'
    [ "$unit_state" = not-found ] ||
        fail "gifford.service already has systemd load state $unit_state; no changes made"
}

write_system_unit() {
    cat <<'EOF'
# One Gifford node per machine, run by the system user "gifford", never by
# root. docs/system-node.md is the install recipe.
[Unit]
Description=Gifford node
Documentation=https://github.com/smellak/gifford/blob/main/docs/system-node.md
After=network.target

[Service]
Type=exec
User=gifford
Group=gifford
# gifford resolves its three directories from these variables: config in
# /etc/gifford, data in /var/lib/gifford, runtime in /run/gifford/gifford.
Environment=XDG_CONFIG_HOME=/etc XDG_DATA_HOME=/var/lib XDG_RUNTIME_DIR=/run/gifford
# /run/gifford is 0755 so that every local user can reach the node socket
# there when config.yaml sets node_socket. The node's own sockets stay in
# /run/gifford/gifford, 0700.
RuntimeDirectory=gifford
RuntimeDirectoryMode=0755
StateDirectory=gifford
StateDirectoryMode=0700
UMask=0077
ExecStart=/usr/local/bin/gifford daemon
Restart=on-failure
RestartSec=2s
# Kept out on purpose:
# - NoNewPrivileges= and the settings that imply it (SystemCallFilter=,
#   RestrictSUIDSGID=, PrivateDevices=, ProtectKernelTunables=, ...) turn
#   off sudo for the node and its children;
# - PrivateTmp= hides /tmp, where tmux keeps its sockets;
# - ProtectProc= hides other users' /proc entries.

[Install]
WantedBy=multi-user.target
EOF
}

if "$system_node"; then
    preflight_system_node
fi

staging=$(mktemp -d)
dest_staging=
config_staging=
unit_staging=
system_account_created=false
system_binary_created=false
system_config_dir_created=false
system_service_start_attempted=false
system_unit_created=false
system_install_complete=false
cleanup() {
    status=$?
    set +e
    if "$system_node" && ! "$system_install_complete"; then
        if "$system_unit_created"; then
            systemctl disable --now gifford.service >/dev/null 2>&1
            rm -f -- /etc/systemd/system/gifford.service
            systemctl daemon-reload >/dev/null 2>&1
        fi
        if "$system_binary_created"; then
            rm -f -- "$bin_dir/gifford"
        fi
        if "$system_config_dir_created"; then
            rm -f -- /etc/gifford/config.yaml
            rmdir -- /etc/gifford >/dev/null 2>&1
        fi
        if "$system_service_start_attempted"; then
            # The preflight proved these systemd-managed directories absent.
            rm -rf -- /run/gifford /var/lib/gifford
        fi
        if "$system_account_created"; then
            userdel gifford >/dev/null 2>&1
            getent group gifford >/dev/null 2>&1 &&
                groupdel gifford >/dev/null 2>&1
        fi
    fi
    [ -z "$config_staging" ] || rm -f -- "$config_staging"
    [ -z "$unit_staging" ] || rm -f -- "$unit_staging"
    # These paths are created only by mktemp in this invocation.
    [ -z "$dest_staging" ] || rm -f -- "$dest_staging"
    [ -z "$staging" ] || rm -rf -- "$staging"
    trap - EXIT
    exit "$status"
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
if "$system_node"; then
    useradd --system --user-group --home-dir /var/lib/gifford --no-create-home \
        --shell /usr/sbin/nologin gifford
    system_account_created=true

    mkdir -m 0700 -- /etc/gifford
    system_config_dir_created=true
    chown gifford:gifford /etc/gifford

    config_staging=$(mktemp /etc/gifford/.config.yaml.XXXXXXXX)
    (umask 077; printf 'schema_version: 1\nnode_id: %s\n' "$node_id" > "$config_staging")
    chown gifford:gifford "$config_staging"
    chmod 0600 "$config_staging"
    ln -- "$config_staging" /etc/gifford/config.yaml ||
        fail '/etc/gifford/config.yaml appeared during installation; rolled back'
    rm -f -- "$config_staging"
    config_staging=
else
    mkdir -p -- "$bin_dir"
fi

dest_staging=$(mktemp "$bin_dir/.gifford-install.XXXXXXXX")
install -m 0755 -- "$staging/gifford" "$dest_staging"
if "$system_node"; then
    chown root:root "$dest_staging"
    ln -- "$dest_staging" "$bin_dir/gifford" ||
        fail "$bin_dir/gifford appeared during installation; rolled back"
    system_binary_created=true
    rm -f -- "$dest_staging"
else
    mv -fT -- "$dest_staging" "$bin_dir/gifford"
fi
dest_staging=

if "$system_node"; then
    unit_staging=$(mktemp /etc/systemd/system/.gifford.service.XXXXXXXX)
    write_system_unit > "$unit_staging"
    printf '%s  %s\n' \
        7aa7456d702f3ba0dda53ae1410d618d75a44ebc1897487dedcbbdecd0fd9f29 \
        "$unit_staging" | sha256sum --check --status ||
        fail 'embedded gifford.service does not match the reviewed unit'
    chown root:root "$unit_staging"
    chmod 0644 "$unit_staging"
    ln -- "$unit_staging" /etc/systemd/system/gifford.service ||
        fail '/etc/systemd/system/gifford.service appeared during installation; rolled back'
    system_unit_created=true
    rm -f -- "$unit_staging"
    unit_staging=

    systemctl daemon-reload
    system_service_start_attempted=true
    systemctl enable --now gifford.service
    systemctl is-enabled --quiet gifford.service
    systemctl is-active --quiet gifford.service
    system_install_complete=true
fi

printf 'Binary installed. Add %s to PATH.\n' "$bin_dir"
if "$system_node"; then
    printf 'System node %s installed and active as gifford.\n' "$node_id"
    printf '%s\n' 'Bridge accounts, agent processes and tmux privileges were not changed.'
else
    printf '%s\n' 'Machine service setup is not included yet; no agents or services were restarted.'
fi
