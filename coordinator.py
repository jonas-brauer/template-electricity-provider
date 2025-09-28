"""Example integration using DataUpdateCoordinator."""

from datetime import timedelta
import logging
from datetime import datetime, timedelta

import aiohttp
import asyncio

import async_timeout

from homeassistant.components.light import LightEntity
from homeassistant.core import callback
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import (
    CoordinatorEntity,
    DataUpdateCoordinator,
    UpdateFailed,
)

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics,
    statistics_during_period,
)

from homeassistant.const import (
    UnitOfTemperature,
    UnitOfEnergy
)
from homeassistant.util import dt as dt_util
from .const import DOMAIN, CONF_TOKEN, UTILITY_ID, BASE_URL

_LOGGER = logging.getLogger(__name__)

class ApiAuthError(Exception):
    """Exception raised for authentication errors."""
    pass

class ApiError(Exception):
    """Exception raised for general API errors."""
    pass

class BjarekraftCoordinator(DataUpdateCoordinator):
    """My custom coordinator."""

    def __init__(self, hass, config_entry):
        """Initialize my coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            # Name of the data. For logging purposes.
            name="My sensor",
            config_entry=config_entry,
            # Polling interval. Will only be polled if there are subscribers.
            update_interval=timedelta(minutes=15),
            # Set always_update to `False` if the data returned from the
            # api can be compared via `__eq__` to avoid duplicate updates
            # being dispatched to listeners
            always_update=True
        )

    async def _async_setup(self):
        """Set up the coordinator

        This is the place to set up your coordinator,
        or to load data, that only needs to be loaded once.

        This method will be called automatically during
        coordinator.async_config_entry_first_refresh.
        """

        # self._device = await self.my_api.get_device()

    async def _async_update_data(self):
        """Fetch data from API endpoint.

        This is the place to pre-process the data to lookup tables
        so entities can quickly look up their data.
        """
        
        try:

            # Note: asyncio.TimeoutError and aiohttp.ClientError are already
            # handled by the data update coordinator.
            async with async_timeout.timeout(10):
                # Grab active context variables to limit data required to be fetched from API
                # Note: using context is not required if there is no need or ability to limit
                # data retrieved from API.
                listening_idx = set(self.async_contexts())
                headers = {
                    "Authorization": "Bearer " + CONF_TOKEN,
                    "User-Agent": "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
                }
                async with aiohttp.ClientSession(headers=headers) as session:
                    print(self.config_entry.as_dict)
                    dateLower = datetime.now() - timedelta(hours=49, minutes=0)
                    dateUpper = datetime.now() + timedelta(hours=25)
                    url = BASE_URL + UTILITY_ID + "/BJR/1/" + dateLower.strftime("%Y-%m-%d") + "/" + dateUpper.strftime("%Y-%m-%d") + "/1/1"
                    _LOGGER.debug("Calling")
                    _LOGGER.debug(url)
                    async with session.get(url) as response:

                        json = await response.json()
                        # print(json)

                        statistic_id = "bjarekraft:grid_consumption"

                        last_stats = await get_instance(self.hass).async_add_executor_job(
                            get_last_statistics, self.hass, 1, statistic_id, True, set()
                        )

                        keepSum = 0
                        if last_stats and statistic_id in last_stats:
                            if last_stats[statistic_id] and len(last_stats[statistic_id]) > 0:
                                if "sum" in last_stats[statistic_id][0]:
                                    keepSum = last_stats[statistic_id][0]["sum"] or 0

                        

                        # Only process new data points that haven't been stored yet
                        # Get the timestamp of the last stored statistic
                        last_timestamp = None
                        if last_stats and statistic_id in last_stats:
                            if last_stats[statistic_id] and len(last_stats[statistic_id]) > 0:
                                if "start" in last_stats[statistic_id][0]:
                                    last_timestamp = last_stats[statistic_id][0]["start"]

                        for d in json['consumptionValues']:

                            statistics = []
                            from_time = dt_util.parse_datetime(d['date']+'+0100') - timedelta(hours=1)

                            # Skip data points that are already stored (older than or equal to last timestamp)
                            if last_timestamp and from_time <= last_timestamp:
                                continue

                            keepSum += d['consumption']

                            statistics.append(
                                StatisticData(
                                    start=from_time,
                                    state=d['consumption'],
                                    sum=keepSum,
                                )
                            )

                            # Only add statistics if we have new data to add
                            if statistics:
                                metadata = StatisticMetaData(
                                        mean_type=StatisticMeanType.NONE,
                                        has_sum=True,
                                        name=f"1",
                                        source="bjarekraft",
                                        statistic_id=statistic_id,
                                        unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
                                    )
                                async_add_external_statistics(self.hass, metadata, statistics)

                            # if date < fifteenMinutesAgo and date > thirtyMinutesAgo:
                            #     print("Adding: ")
                            #     print(d["consumption"])
                            #     print(d["date"])
                            #     # self._attr_native_value = int(self._attr_native_value or 0) + d['consumption']
                            #     # self.async_write_ha_state()
                            # else:
                            #     print('not in window')


                # print(response)
                return json
                # return await self.my_api.fetch_data(listening_idx)
        except ApiAuthError as err:
            # Raising ConfigEntryAuthFailed will cancel future updates
            # and start a config flow with SOURCE_REAUTH (async_step_reauth)
            raise ConfigEntryAuthFailed from err
        except ApiError as err:
            raise UpdateFailed(f"Error communicating with API: {err}")
