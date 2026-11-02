# One node per machine

[`gifford.service`](gifford.service) runs one Gifford node per machine as the
system user `gifford`, never as root.

## Layout

| Path | Owner and mode | Holds | Made by |
|---|---|---|---|
| `/etc/gifford` | `gifford`, `0700` | `config.yaml` (`0600`) and `policies/` | the administrator |
| `/var/lib/gifford` | `gifford`, `0700` | the database, the node keys and the artifacts | systemd, `StateDirectory=` |
| `/run/gifford` | `gifford`, `0755` | `node.sock` (`0666`) when `node_socket` is set, and the runtime directory `gifford/` (`0700`): `hub.sock`, `daemon.sock` and `hub.lock` | systemd, `RuntimeDirectory=`; removed when the service stops |

The unit sets `XDG_CONFIG_HOME=/etc`, `XDG_DATA_HOME=/var/lib` and
`XDG_RUNTIME_DIR=/run/gifford`. Gifford resolves its config, data and runtime
directories from those variables, as it does for a node run by a user, so no
code path is specific to the service.

Keep anything else the node needs, such as per-pane lock files, out of
`/var/lib/gifford`. The node refuses a data directory that is not exactly
`0700`, and systemd resets the mode of its `StateDirectory=` at every start,
so a directory there cannot carry access for other users.

## Install

Build the commit you install with `task --force build`, and record
`sha256sum bin/gifford` next to the commit. Then, as root, from that checkout:

```sh
install -m 0755 bin/gifford /usr/local/bin/gifford
useradd --system --user-group --home-dir /var/lib/gifford --no-create-home \
  --shell /usr/sbin/nologin gifford
install -d -m 0700 -o gifford -g gifford /etc/gifford
# The node id takes lowercase letters, digits, '.', '_' and '-'.
(umask 077; printf 'schema_version: 1\nnode_id: %s\n' node-<name> > /etc/gifford/config.yaml)
chown gifford:gifford /etc/gifford/config.yaml
install -m 0644 docs/gifford.service /etc/systemd/system/gifford.service
systemctl daemon-reload
systemctl enable --now gifford.service
```

`journalctl -u gifford` then shows `gifford hub: ready: serving
/run/gifford/gifford/hub.sock and /run/gifford/gifford/daemon.sock`.

## The node socket

`node_socket` in `config.yaml` opens a socket that every local user can
connect to:

```yaml
node_socket:
  path: /run/gifford/node.sock
  bridge_uid: <uid>
```

The node classifies each connection once, when it accepts it, from the
kernel's SO_PEERCRED and never from anything the client sends:

- `bridge_uid` reaches only the bridge methods. It is a decimal uid, never a
  user name, and it must be neither 0 nor the node's own uid;
- root and the node's own uid are closed: administrators use the private
  control socket;
- any other uid must descend from the process of a live agent session that
  runs as that same uid. It reaches that agent's methods, and never
  registration;
- anything else is closed before the node reads a byte.

## Operate

Run every gifford command for this node as `gifford`, with the same three
variables:

```sh
sudo -u gifford env XDG_CONFIG_HOME=/etc XDG_DATA_HOME=/var/lib \
  XDG_RUNTIME_DIR=/run/gifford gifford status
```

To upgrade, install the new binary and run `systemctl restart gifford`.

The unit leaves out some hardening settings on purpose. Its comments say
which ones, and why.
