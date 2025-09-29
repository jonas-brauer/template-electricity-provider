"""Example integration using DataUpdateCoordinator."""

from datetime import timedelta
import logging
from datetime import datetime, timedelta

import aiohttp
import asyncio
import random

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
        # Load initial data from beginning of year if no data exists
        statistic_id = "bjarekraft:grid_consumption"

        # Check if we have existing statistics
        last_stats = await get_instance(self.hass).async_add_executor_job(
            get_last_statistics, self.hass, 1, statistic_id, True, set()
        )

        # Only load historical data if this is first setup (no existing statistics)
        if not last_stats or statistic_id not in last_stats or not last_stats[statistic_id]:
            _LOGGER.info("First setup detected, loading historical data from beginning of year")
            await self._load_historical_data()
        else:
            _LOGGER.error("Existing statistics found, skipping historical data load")

    async def _load_historical_data(self):
        """Load all historical data from beginning of year."""
        try:
            async with async_timeout.timeout(600):  # 10 minute timeout for initial load
                headers = {
                    "Authorization": "Bearer " + CONF_TOKEN,
                    "User-Agent": "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
                }
                

                async with aiohttp.ClientSession(headers=headers) as session:
                    statistic_id = "bjarekraft:grid_consumption"
                    keepSum = 0

                    # Fetch all data from beginning of year
                    #start_date = datetime(datetime.now().year, 1, 1)
                    start_date = datetime.now() - timedelta(days=30)
                    end_date = datetime.now()
                    

                    # Create metadata once, it's the same for all statistics
                    metadata = StatisticMetaData(
                        mean_type=StatisticMeanType.NONE,
                        has_sum=True,
                        name=f"1",
                        source="bjarekraft",
                        statistic_id=statistic_id,
                        unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
                    )

                    # Collect all statistics in one list to add them in chronological order
                    all_statistics = []

                    # Fetch historical data day by day (API requires dates to be on same day)
                    _LOGGER.error(f"Fetching historical data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

                    current_date = start_date.date() if hasattr(start_date, 'date') else start_date
                    end = end_date.date() if hasattr(end_date, 'date') else end_date

                    while current_date <= end:
                        # Use start of day (00:00) and end of day (23:59) for the same date
                        dateLower = datetime.combine(current_date, datetime.min.time())
                        dateUpper = datetime.combine(current_date, datetime.max.time().replace(microsecond=0))

                        # API expects format like "2025-01-01" for both dates
                        url = BASE_URL + UTILITY_ID + "/BJR/1/" + current_date.strftime("%Y-%m-%d") + "/" + current_date.strftime("%Y-%m-%d") + "/1/1"
                        _LOGGER.error(f"Fetching data for {current_date.strftime('%Y-%m-%d')}")

                        try:
                            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                                if response.status == 200:
                                    response_text = await response.text()
                                    _LOGGER.error(f"Response length for {current_date}: {len(response_text)} chars")

                                    # Parse JSON from text
                                    import json as json_module
                                    json_day = json_module.loads(response_text)

                                    _LOGGER.error(f"Response keys: {list(json_day.keys()) if json_day else 'None'}")

                                    if 'consumptionValues' in json_day:
                                        _LOGGER.error(f"Found {len(json_day['consumptionValues'])} consumption values for {current_date}")
                                        if json_day['consumptionValues']:
                                            # Process all consumption values for this day
                                            day_count = 0
                                            for d in json_day['consumptionValues']:
                                                # Parse the date - API returns like "2025-09-01T00:00:00"
                                                from_time = dt_util.parse_datetime(d['date'])
                                                if from_time is None:
                                                    _LOGGER.error(f"Failed to parse date: {d['date']}")
                                                    continue
                                                keepSum += d['consumption']

                                                all_statistics.append(
                                                    StatisticData(
                                                        start=from_time,
                                                        state=d['consumption'],
                                                        sum=keepSum,
                                                    )
                                                )
                                                day_count += 1

                                            _LOGGER.error(f"Added {day_count} values for {current_date} (total: {len(all_statistics)})")
                                        else:
                                            _LOGGER.warning(f"consumptionValues is empty for {current_date}")
                                    else:
                                        _LOGGER.warning(f"No consumptionValues key in response for {current_date}")
                                else:
                                    _LOGGER.error(f"API returned status {response.status} for {current_date}")
                        except Exception as e:
                            _LOGGER.error(f"Failed to fetch historical data for {current_date}: {e}")

                        # Move to next day (current_date is a date object)
                        current_date = current_date + timedelta(days=1)
                        # Add a small delay to avoid overwhelming the API
                        await asyncio.sleep(0.5)

                    # Add all statistics in one batch
                    _LOGGER.error(f"Total statistics collected: {len(all_statistics)}")
                    if all_statistics:
                        _LOGGER.info(f"Adding {len(all_statistics)} historical statistics to database")
                        await get_instance(self.hass).async_add_executor_job(
                            async_add_external_statistics, self.hass, metadata, all_statistics
                        )
                    else:
                        _LOGGER.error("No historical statistics to add")

                    _LOGGER.error(f"Historical data load completed")

        except asyncio.TimeoutError:
            _LOGGER.error("Timeout while loading historical data")
            raise UpdateFailed("Timeout loading historical data")
        except Exception as err:
            _LOGGER.error(f"Error loading historical data: {err}")
            raise UpdateFailed(f"Error loading historical data: {err}")

    async def _async_update_data(self):
        """Fetch data from API endpoint.

        This is the place to pre-process the data to lookup tables
        so entities can quickly look up their data.
        """

        try:

            # Note: asyncio.TimeoutError and aiohttp.ClientError are already
            # handled by the data update coordinator.
            async with async_timeout.timeout(300):
                # Grab active context variables to limit data required to be fetched from API
                # Note: using context is not required if there is no need or ability to limit
                # data retrieved from API.
                listening_idx = set(self.async_contexts())
                headers = {
                    "Authorization": "Bearer " + CONF_TOKEN,
                    "User-Agent": "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
                }
                async with aiohttp.ClientSession(headers=headers) as session:
                    statistic_id = "bjarekraft:grid_consumption"

                    # Get the last statistics to continue from where we left off
                    last_stats = await get_instance(self.hass).async_add_executor_job(
                        get_last_statistics, self.hass, 1, statistic_id, True, set()
                    )

                    # If no previous data exists, load historical data first
                    if not last_stats or statistic_id not in last_stats or not last_stats[statistic_id]:
                        _LOGGER.info("No existing statistics found in _async_update_data, loading historical data")
                        await self._load_historical_data()

                        # Get the updated last statistics after loading historical data
                        last_stats = await get_instance(self.hass).async_add_executor_job(
                            get_last_statistics, self.hass, 1, statistic_id, True, set()
                        )

                    keepSum = 0
                    last_timestamp = None

                    if last_stats and statistic_id in last_stats:
                        if last_stats[statistic_id] and len(last_stats[statistic_id]) > 0:
                            if "sum" in last_stats[statistic_id][0]:
                                keepSum = last_stats[statistic_id][0]["sum"] or 0
                            if "start" in last_stats[statistic_id][0]:
                                last_timestamp = last_stats[statistic_id][0]["start"]

                    # Regular update - fetch only recent data
                    dateLower = datetime.now() - timedelta(hours=49, minutes=0)
                    dateUpper = datetime.now() + timedelta(hours=25)
                    url = BASE_URL + UTILITY_ID + "/BJR/1/" + dateLower.strftime("%Y-%m-%d") + "/" + dateUpper.strftime("%Y-%m-%d") + "/1/1"
                    _LOGGER.error(url)
                    _LOGGER.error("Calling")
                    _LOGGER.error(url)

                    async with session.get(url) as response:
                        json_data = await response.json()

                    # Process only new data points
                    for d in json_data['consumptionValues']:
                        statistics = []
                        # Parse the date - API returns like "2025-09-01T00:00:00"
                        from_time = dt_util.parse_datetime(d['date'])
                        if from_time is None:
                            _LOGGER.error(f"Failed to parse date: {d['date']}")
                            continue

                        # Skip data points that are already stored (older than or equal to last timestamp)
                        # Convert datetime to timestamp for comparison (last_timestamp is a float)
                        if last_timestamp and from_time.timestamp() <= last_timestamp:
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
                            await get_instance(self.hass).async_add_executor_job(
                                async_add_external_statistics, self.hass, metadata, statistics
                            )

                return json_data
                # return await self.my_api.fetch_data(listening_idx)
        except ApiAuthError as err:
            # Raising ConfigEntryAuthFailed will cancel future updates
            # and start a config flow with SOURCE_REAUTH (async_step_reauth)
            raise ConfigEntryAuthFailed from err
        except ApiError as err:
            raise UpdateFailed(f"Error communicating with API: {err}")
