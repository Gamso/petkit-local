"""Notifications to the owner's phones, straight from petkit-local.

    settings.py   which kinds are on (the panel's switches), persisted to
                  `{data_dir}/notify_kinds.json`, set only from the panel
    rules.py      what is worth a notification and what it says (pure)
    notifier.py   the hub subscriber and its gates (kind, dedupe, rate limit),
                  which hands what passes to the two sinks below
    push.py       a six-key JSON message POSTed to the UnifiedPush endpoint
                  each signed-in phone registered
    channel.py    that same message, with an id, written to the app API's
                  notification socket, and a bounded catch-up log
    failurelog.py the rate-limited failure warning the notifier and the push
                  endpoints share

The phone receives them over the app's own connection to the app API, with
nothing else to run, or through the UnifiedPush distributor it registered (see
DOCS.md). The only outbound calls are POSTs to the endpoints the phones
registered — never anything of PetKit's.
"""
