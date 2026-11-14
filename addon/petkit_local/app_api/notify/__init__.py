"""Push notifications to the owner's phone through a self-hosted ntfy server.

    settings.py   where they go (server, token, topic) and which kinds are on,
                  persisted to `{data_dir}/ntfy.json`, set only from the panel
    rules.py      what is worth a notification and what it says (pure)
    notifier.py   the hub subscriber, its gates (kind, dedupe, rate limit) and
                  the bounded queue the ntfy publisher drains

The phone receives them through UnifiedPush, with the ntfy Android app as the
distributor (see DOCS.md). The only outbound call is a POST to the configured
ntfy server — never anything of PetKit's.
"""
