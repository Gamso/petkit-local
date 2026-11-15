"""What the app lists as a device: the WiFi devices, and the Bluetooth accessories they relay.

A W5/W4/CTW2/CTW3 fountain or a K3 Pura Air has no network identity of its own
(`devices/ble/registry.py`), so it is not in the `DeviceRegistry`. Home Assistant
still shows it as a device of its own, under its own PetKit id, with the
entities `ha/entities/ble.py` declares — and the app does the same. Both
registries share one id space: the command router and the panel resolve an id
the device registry does not hold as an accessory's, and so does this module.
A real device wins a clash, as it does there.

Only `/devices`, `/devices/{id}`, its actions and entities, and `/ws` know about
accessories. Every other `/devices/{id}/...` route (feeding, live video) is about
hardware an accessory does not have and keeps answering 404 for one.

The link is stated from both ends, so neither screen needs the whole list: an
accessory names its `parent_id`, and its parent lists it in `accessories`. That
is the K3 too, which the firmware is told about through `dev_device_info`'s
`k3Device` rather than the relay list — but which is linked by the same
`link_with` either way. The parent's list is `BLERegistry.get_linked`, in the
order `get_linked_k3` picks from, so the first purifier listed is the one the
box holds in its single K3 slot.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from petkit_local.app_api import serializers
from petkit_local.ha.categories import get_entities_for_device
from petkit_local.ha.entities.ble import get_ble_entities
from petkit_local.ha.state_doc import entity_state_doc

if TYPE_CHECKING:
    from petkit_local.devices.base import Device
    from petkit_local.devices.ble import BLEDevice, BLERegistry
    from petkit_local.devices.registry import DeviceRegistry
    from petkit_local.ha.discovery import EntityDef


def linked(ble: BLERegistry | None, parent_id: int) -> list[BLEDevice]:
    """The accessories this device relays."""
    return ble.get_linked(parent_id) if ble is not None else []


def parent_of(registry: DeviceRegistry, accessory: BLEDevice) -> Device | None:
    """The registered device relaying this accessory; None when it has none."""
    return registry.get(accessory.link_with) if accessory.link_with else None


def accessory(registry: DeviceRegistry, ble: BLERegistry | None,
              node_id: int) -> BLEDevice | None:
    """The accessory under this id, unless a real device holds it."""
    if ble is None or registry.get(node_id) is not None:
        return None
    return ble.get(node_id)


def ids(registry: DeviceRegistry, ble: BLERegistry | None) -> set[int]:
    """Every id the app may be shown."""
    found = {d.petkit_id for d in registry.all()}
    if ble is not None:
        found |= {a.petkit_id for a in ble.all()}
    return found


def describe(registry: DeviceRegistry, ble: BLERegistry | None, node_id: int,
             ) -> tuple[dict[str, Any], list[EntityDef], dict[str, Any]] | None:
    """`(summary, entities, state document)` for one id, or None if nothing holds it."""
    device = registry.get(node_id)
    if device is not None:
        return (serializers.device_summary(device, linked(ble, node_id)),
                get_entities_for_device(device), entity_state_doc(device))
    acc = accessory(registry, ble, node_id)
    if acc is not None:
        return (serializers.accessory_summary(acc, parent_of(registry, acc)),
                get_ble_entities(acc.ble_type), acc.state)
    return None


def summary(registry: DeviceRegistry, ble: BLERegistry | None,
            node_id: int) -> dict[str, Any] | None:
    """`DeviceSummary` for one id."""
    described = describe(registry, ble, node_id)
    return described[0] if described is not None else None


def detail(registry: DeviceRegistry, ble: BLERegistry | None,
           node_id: int) -> dict[str, Any] | None:
    """`DeviceDetail` for one id."""
    device = registry.get(node_id)
    if device is not None:
        return serializers.device_detail(device, get_entities_for_device(device),
                                         entity_state_doc(device), linked(ble, node_id))
    acc = accessory(registry, ble, node_id)
    if acc is not None:
        return serializers.accessory_detail(acc, parent_of(registry, acc),
                                            get_ble_entities(acc.ble_type))
    return None


def all_summaries(registry: DeviceRegistry, ble: BLERegistry | None) -> list[dict[str, Any]]:
    """Every device and accessory as `DeviceSummary`, ordered by id."""
    return [s for s in (summary(registry, ble, i) for i in sorted(ids(registry, ble)))
            if s is not None]


def all_details(registry: DeviceRegistry, ble: BLERegistry | None) -> list[dict[str, Any]]:
    """Every device and accessory as `DeviceDetail`, ordered by id."""
    return [d for d in (detail(registry, ble, i) for i in sorted(ids(registry, ble)))
            if d is not None]
