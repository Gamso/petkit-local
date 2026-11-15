"""Push notifications to the owner's phone through a self-hosted ntfy server.

    settings.py   where they go (server, token, topic) and which kinds are on,
                  persisted to `{data_dir}/ntfy.json`, set only from the panel
    rules.py      what is worth a notification and what it says (pure)
    notifier.py   the hub subscriber, its gates (kind, dedupe, rate limit) and
                  the bounded queue the ntfy publisher drains
    push.py       the second sink: a six-key JSON message POSTed to the
                  UnifiedPush endpoint each signed-in phone registered
    failurelog.py the rate-limited failure warning both sinks share

The phone receives them through UnifiedPush, with the ntfy Android app (or any
distributor) on the phone (see DOCS.md). The only outbound calls are POSTs to
the configured ntfy server and to the endpoints the phones registered — never
anything of PetKit's.
"""
