#!/bin/sh

set -u
LC_ALL=C
export LC_ALL

usage() {
	printf 'usage: %s --role origin|destination --data-dir DIR --message-id ULID \\\n' "$0" >&2
	printf '%s\n' "  --origin-node NODE --destination-node NODE --sender-agent AGENT --recipient-agent AGENT" >&2
	printf '%s\n' "  [--endpoint HTTPS_ORIGIN --fingerprint SHA256]  # required for origin" >&2
	exit 2
}

role=
data_dir=
message_id=
origin_node=
destination_node=
sender_agent=
recipient_agent=
endpoint=
fingerprint=

while [ "$#" -gt 0 ]; do
	case "$1" in
	--role | --data-dir | --message-id | --origin-node | --destination-node | --sender-agent | --recipient-agent | --endpoint | --fingerprint)
		[ "$#" -ge 2 ] || usage
		case "$1" in
		--role) role=$2 ;;
		--data-dir) data_dir=$2 ;;
		--message-id) message_id=$2 ;;
		--origin-node) origin_node=$2 ;;
		--destination-node) destination_node=$2 ;;
		--sender-agent) sender_agent=$2 ;;
		--recipient-agent) recipient_agent=$2 ;;
		--endpoint) endpoint=$2 ;;
		--fingerprint) fingerprint=$2 ;;
		esac
		shift 2
		;;
	*) usage ;;
	esac
done

[ "$role" = origin ] || [ "$role" = destination ] || usage
[ -n "$data_dir" ] && [ -n "$message_id" ] && [ -n "$origin_node" ] &&
	[ -n "$destination_node" ] && [ -n "$sender_agent" ] && [ -n "$recipient_agent" ] || usage

valid_id() {
	case "$1" in
	[a-z0-9]*) ;;
	*) return 1 ;;
	esac
	case "$1" in
	*[!a-z0-9._-]*) return 1 ;;
	*) [ "${#1}" -le 64 ] ;;
	esac
}

valid_ulid() {
	[ "${#1}" -eq 26 ] || return 1
	case "$1" in
	*[!0-9A-HJKMNP-TV-Z]*) return 1 ;;
	*) return 0 ;;
	esac
}

valid_id "$origin_node" && valid_id "$destination_node" &&
	valid_id "$sender_agent" && valid_id "$recipient_agent" && valid_ulid "$message_id" || usage

if [ "$role" = origin ]; then
	[ -n "$endpoint" ] && [ "${#fingerprint}" -eq 64 ] || usage
	case "$fingerprint" in
	*[!0-9a-f]*) usage ;;
	esac
fi

if ! command -v sqlite3 >/dev/null 2>&1; then
	printf '%s\n' "FAIL prerequisite: sqlite3 is not installed"
	exit 1
fi

database=$data_dir/gifford.db
if [ ! -f "$database" ]; then
	printf '%s\n' "FAIL prerequisite: gifford.db is not a regular file"
	exit 1
fi

failures=0

check_one() {
	label=$1
	query=$2
	if ! value=$(sqlite3 -readonly "$database" "PRAGMA query_only=ON; $query" 2>/dev/null); then
		printf 'FAIL %s: query failed\n' "$label"
		failures=$((failures + 1))
		return
	fi
	if [ "$value" = 1 ]; then
		printf 'OK %s\n' "$label"
	else
		printf 'FAIL %s: expected one durable row, got %s\n' "$label" "${value:-none}"
		failures=$((failures + 1))
	fi
}

if [ "$role" = origin ]; then
	check_one local-node "SELECT count(*) FROM nodes WHERE node_id='$origin_node' AND local=1 AND status='ACTIVE';"
	check_one trusted-peer "SELECT count(*) FROM nodes WHERE node_id='$destination_node' AND local=0 AND status='TRUSTED' AND length(certificate_fingerprint)=64;"
	check_one sender-agent "SELECT count(*) FROM agents WHERE agent_id='$sender_agent' AND node_id='$origin_node';"
	check_one stored-remote-receipt "SELECT count(*) FROM federation_outbox AS o JOIN federation_receipts AS r ON r.outbox_id=o.outbox_id WHERE o.object_type='message' AND o.object_id='$message_id' AND o.destination_node='$destination_node' AND o.state='STORED_REMOTE' AND r.state='STORED_REMOTE';"
	check_one acknowledged-message "SELECT count(*) FROM messages WHERE message_id='$message_id' AND origin_node='$origin_node' AND destination_node='$destination_node' AND from_agent='$sender_agent' AND state='ACKNOWLEDGED';"
	check_one acknowledged-inbox "SELECT count(*) FROM federation_ack_inbox WHERE origin_node='$destination_node' AND message_id='$message_id' AND agent_id='$recipient_agent' AND delivery_generation=1 AND status='ACKNOWLEDGED';"
	if GIFFORD_FEDPROBE_ENDPOINT="$endpoint" \
		GIFFORD_FEDPROBE_FINGERPRINT="$fingerprint" \
		GIFFORD_FEDPROBE_DATA_DIR="$data_dir" \
		go test -count=1 -tags fedprobe ./test/fedprobe -v; then
		printf 'OK live-fedprobe\n'
	else
		printf 'FAIL live-fedprobe: tagged suite failed\n'
		failures=$((failures + 1))
	fi
else
	ack_object=$message_id:$recipient_agent:1:ACKNOWLEDGED
	check_one local-node "SELECT count(*) FROM nodes WHERE node_id='$destination_node' AND local=1 AND status='ACTIVE';"
	check_one trusted-peer "SELECT count(*) FROM nodes WHERE node_id='$origin_node' AND local=0 AND status='TRUSTED' AND length(certificate_fingerprint)=64;"
	check_one recipient-agent "SELECT count(*) FROM agents WHERE agent_id='$recipient_agent' AND node_id='$destination_node';"
	check_one received-message "SELECT count(*) FROM messages WHERE message_id='$message_id' AND origin_node='$origin_node' AND destination_node='$destination_node' AND from_agent='$sender_agent' AND state='PENDING';"
	check_one local-acknowledgement "SELECT count(*) FROM acks WHERE message_id='$message_id' AND agent_id='$recipient_agent' AND delivery_generation=1 AND status='ACKNOWLEDGED';"
	check_one acknowledged-outbox "SELECT count(*) FROM federation_outbox AS o JOIN federation_ack_receipts AS r ON r.outbox_id=o.outbox_id WHERE o.object_type='ack' AND o.object_id='$ack_object' AND o.destination_node='$origin_node' AND o.state='STORED_REMOTE';"
fi

[ "$failures" -eq 0 ] || exit 1
