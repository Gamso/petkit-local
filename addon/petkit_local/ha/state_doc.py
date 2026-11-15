"""The state document an entity's `value_path` is read out of, outside HA.

`HAPublisher._build_state` publishes this document to Home Assistant, and every
`EntityDef.value_path` is a dotted path into it. The panel and the app API
resolve the same paths against the same document, so neither can show a value
that differs from what HA shows. Built here, beside the publisher, rather than
in either of them: both are readers, and a reader does not own the shape.
"""
from __future__ import annotations

from typing import Any

from petkit_local.devices.base import Device
from petkit_local.devices.state_parsers import apply_consumable_state
from petkit_local.events.normalize import apply_feed_plan_state
from petkit_local.ha.commands import LOCAL_DEFAULTS, multi_config_texts


def entity_state_doc(d: Device) -> dict[str, Any]:
    """Same shape the HA publisher publishes, for value resolution.

    Recomputes the consumable countdowns first, for the same reason
    `HAPublisher._build_state` does: they move with the calendar, and the N50's
    has no device input to trigger it.
    """
    apply_consumable_state(d)
    apply_feed_plan_state(d)
    settings = d.config.get("settings") or {}
    enabled = d.enabled_capabilities()
    return {
        "state": d.state or {},
        "settings": settings,
        "schedule": d.config.get("schedule", []),
        "feed_schedule": d.config.get("feed_schedule", {}),
        "capabilities": {ct: (ct in enabled) for ct in d.CAPABILITY_TYPES},
        # Without it every `local.` control reads empty here while HA shows
        # its value -- and the paused-plan switch would read as off.
        "local": {**LOCAL_DEFAULTS, **(d.config.get("local") or {})},
        "multi_config": multi_config_texts(d),
    }
