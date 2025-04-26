"""Platform for sensor integration."""
from __future__ import annotations

from datetime import datetime, timedelta

from homeassistant.core import callback
from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import (
    UnitOfTemperature,
    UnitOfEnergy
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.config_entries import ConfigEntry

import random
from homeassistant.helpers.update_coordinator import (
    CoordinatorEntity,
    DataUpdateCoordinator,
    UpdateFailed,
)

from .coordinator import BjarekraftCoordinator

class BjarekraftSensor(SensorEntity):
    """Amber Base Sensor."""

    _attr_attribution = "Data collected by me"

    def __init__(
        self,
        description: SensorEntityDescription,
    ) -> None:
        """Initialize the Sensor."""
        super().__init__()
        self.entity_description = description

        self._attr_unique_id = (
            "123-sdfd"
        )

async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up the Tibber sensor."""
    coordinator = BjarekraftCoordinator(hass, entry)
    async_add_entities([ExampleSensor(coordinator, 11235)], True)



def setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None
) -> None:
    """Set up the sensor platform."""
    add_entities([ExampleSensor()])


class ExampleSensor(CoordinatorEntity, SensorEntity):
    """Representation of a Sensor."""

    _attr_name = "Grid consumption"
    _attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
    _attr_device_class = SensorDeviceClass.ENERGY
    _attr_state_class = SensorStateClass.TOTAL_INCREASING
    _attr_unique_id = "gridConsumption"

    def __init__(self, coordinator, idx):
        """Pass coordinator to CoordinatorEntity."""
        super().__init__(coordinator, context=idx)
        self.idx = idx

    @callback
    def _handle_coordinator_update(self) -> None:
        print('callbacked')


    def update(self) -> None:
        """Fetch new state data for the sensor.

        This is the only method that should fetch new data for Home Assistant.
        """

        self._attr_native_value = int(self._attr_native_value or 0) + random.randint(1,2)