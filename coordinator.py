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
            get_last_statistics, self.hass, 1, statistic_id, True, {"sum"}
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
            async with async_timeout.timeout(1200):  # 20 minute timeout for initial load
                headers = {
                    "Authorization": "Bearer " + CONF_TOKEN,
                    "User-Agent": "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
                }


                async with aiohttp.ClientSession(headers=headers) as session:
                    statistic_id = "bjarekraft:grid_consumption"

                    # Check for existing statistics to continue from where we left off
                    last_stats = await get_instance(self.hass).async_add_executor_job(
                        get_last_statistics, self.hass, 1, statistic_id, True, {"sum"}
                    )

                    keepSum = 0
                    start_date = datetime(datetime.now().year, 1, 1)

                    # If we have existing data, start from there
                    if last_stats and statistic_id in last_stats and last_stats[statistic_id]:
                        if "sum" in last_stats[statistic_id][0]:
                            keepSum = last_stats[statistic_id][0]["sum"] or 0
                        if "start" in last_stats[statistic_id][0]:
                            last_timestamp = last_stats[statistic_id][0]["start"]
                            # Start from the day after the last timestamp
                            start_date = datetime.fromtimestamp(last_timestamp).date() + timedelta(days=1)
                            start_date = datetime.combine(start_date, datetime.min.time())
                            _LOGGER.info(f"Continuing from existing data - last sum: {keepSum}, starting from: {start_date}")

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

                    # Fetch historical data day by day (API requires dates to be on same day)
                    _LOGGER.error(f"Fetching historical data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

                    current_date = start_date.date() if hasattr(start_date, 'date') else start_date
                    end = end_date.date() if hasattr(end_date, 'date') else end_date

                    total_processed = 0

                    while current_date <= end:
                        # Use start of day (00:00) and end of day (23:59) for the same date
                        dateLower = datetime.combine(current_date, datetime.min.time())
                        dateUpper = datetime.combine(current_date, datetime.max.time().replace(microsecond=0)) + timedelta(days=1)

                        # API expects format like "2025-01-01" for both dates
                        url = BASE_URL + UTILITY_ID + "/BJR/1/" + dateLower.strftime("%Y-%m-%d") + "/" + dateUpper.strftime("%Y-%m-%d") + "/1/1"
                        _LOGGER.error(f"URL: {url}")
                        _LOGGER.error(f"Fetching data for {dateLower.strftime('%Y-%m-%d')} - {dateUpper.strftime('%Y-%m-%d')}")

                        try:
                            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                                if response.status == 200:
                                    response_text = await response.text()
                                    _LOGGER.error(f"Response length for {current_date}: {len(response_text)} chars")

                                    # Parse JSON from text
                                    import json as json_module
                                    json_day = json_module.loads(response_text)

                                    _LOGGER.error(f"Response keys: {list(json_day.keys()) if json_day else 'None'}")
                                    _LOGGER.error(f"Response: {json_day}")
                                    _LOGGER.error(f"consumptionValues: {json_day.get('consumptionValues', 'Not found')}")

                                    if 'consumptionValues' in json_day:
                                        _LOGGER.error(f"Found {len(json_day['consumptionValues'])} consumption values for {current_date}")
                                        if json_day['consumptionValues']:
                                            # Process all consumption values for this day
                                            day_statistics = []
                                            day_count = 0

                                            for d in json_day['consumptionValues']:
                                                # Skip data points with non-zero status (incomplete/unavailable data)
                                                # Status 0 = valid data, status 4 = not yet available
                                                if d.get('status', 0) != 0:
                                                    continue

                                                # Parse the date - API returns like "2025-09-01T00:00:00"
                                                from_time = dt_util.parse_datetime(d['date'])
                                                if from_time is None:
                                                    _LOGGER.error(f"Failed to parse date: {d['date']}")
                                                    continue

                                                # Ensure datetime has timezone info (assume local timezone if none)
                                                if from_time.tzinfo is None:
                                                    from_time = dt_util.as_local(from_time)
                                                keepSum += d['consumption']

                                                day_statistics.append(
                                                    StatisticData(
                                                        start=from_time,
                                                        state=d['consumption'],
                                                        sum=keepSum,
                                                    )
                                                )
                                                day_count += 1

                                            # Write this day's statistics to database immediately
                                            if day_statistics:
                                                await get_instance(self.hass).async_add_executor_job(
                                                    async_add_external_statistics, self.hass, metadata, day_statistics
                                                )
                                                total_processed += day_count
                                                _LOGGER.error(f"Wrote {day_count} values for {current_date} to database (total processed: {total_processed})")
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

                    _LOGGER.error(f"Historical data load completed - total processed: {total_processed}")

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
                        get_last_statistics, self.hass, 1, statistic_id, True, {"sum"}
                    )

                    # If no previous data exists, load historical data first
                    if not last_stats or statistic_id not in last_stats or not last_stats[statistic_id]:
                        _LOGGER.info("No existing statistics found in _async_update_data, loading historical data")
                        await self._load_historical_data()

                        # Get the updated last statistics after loading historical data
                        last_stats = await get_instance(self.hass).async_add_executor_job(
                            get_last_statistics, self.hass, 1, statistic_id, True, {"sum"}
                        )

                    keepSum = 0
                    last_timestamp = None

                    if last_stats and statistic_id in last_stats:
                        if last_stats[statistic_id] and len(last_stats[statistic_id]) > 0:
                            if "sum" in last_stats[statistic_id][0]:
                                keepSum = last_stats[statistic_id][0]["sum"] or 0
                            if "start" in last_stats[statistic_id][0]:
                                last_timestamp = last_stats[statistic_id][0]["start"]

                    # Regular update - fetch recent data for validation
                    # Fetch last 3 days to catch any retroactive changes in the API
                    today = datetime.now().date()
                    start_fetch_date = today - timedelta(days=2)  # Fetch last 3 days including today

                    all_recent_data = []

                    # Fetch data for the last few days
                    current_fetch = start_fetch_date
                    _LOGGER.info(f"Fetching data from {start_fetch_date} to {today} (3 days)")

                    while current_fetch <= today:
                        dateLower = datetime.combine(current_fetch, datetime.min.time())
                        dateUpper = datetime.combine(current_fetch, datetime.max.time().replace(microsecond=0)) + timedelta(days=1)

                        # API expects format like "2025-01-01" for both dates
                        url = BASE_URL + UTILITY_ID + "/BJR/1/" + dateLower.strftime("%Y-%m-%d") + "/" + dateUpper.strftime("%Y-%m-%d") + "/1/1"
                        _LOGGER.info(f"Fetching recent data for {current_fetch}, URL: {url}")

                        try:
                            async with session.get(url) as response:
                                _LOGGER.info(f"API response status for {current_fetch}: {response.status}")
                                if response.status == 200:
                                    json_data = await response.json()
                                    _LOGGER.info(f"API response keys for {current_fetch}: {list(json_data.keys()) if json_data else 'None'}")
                                    if 'consumptionValues' in json_data and json_data['consumptionValues']:
                                        all_recent_data.extend(json_data['consumptionValues'])
                                        _LOGGER.info(f"Found {len(json_data['consumptionValues'])} values for {current_fetch}")
                                    elif 'consumptionValues' in json_data:
                                        _LOGGER.warning(f"consumptionValues exists but is empty for {current_fetch}")
                                    else:
                                        _LOGGER.warning(f"No consumptionValues in response for {current_fetch}")
                                else:
                                    _LOGGER.warning(f"API returned status {response.status} for {current_fetch}")
                        except Exception as e:
                            _LOGGER.error(f"Failed to fetch recent data for {current_fetch}: {e}", exc_info=True)

                        current_fetch += timedelta(days=1)

                    _LOGGER.info(f"Total data points fetched from API: {len(all_recent_data)}")

                    # Get existing statistics for the period to compare
                    start_dt = dt_util.as_local(datetime.combine(start_fetch_date, datetime.min.time()))
                    end_dt = dt_util.as_local(datetime.combine(today, datetime.max.time()))

                    existing_stats = await get_instance(self.hass).async_add_executor_job(
                        statistics_during_period,
                        self.hass,
                        start_dt,
                        end_dt,
                        {statistic_id},
                        "hour",
                        None,
                        {"sum", "state"}
                    )

                    # Create a dict of existing statistics by timestamp for quick lookup
                    existing_stats_map = {}
                    if statistic_id in existing_stats:
                        for stat in existing_stats[statistic_id]:
                            existing_stats_map[stat['start']] = stat

                    # Process data points - both new and potentially changed existing ones
                    statistics = []
                    updates_count = 0
                    new_count = 0

                    # Sort all_recent_data by date to process in chronological order
                    all_recent_data_sorted = sorted(
                        [d for d in all_recent_data if d.get('status', 0) == 0],
                        key=lambda x: dt_util.parse_datetime(x['date']) or datetime.min
                    )

                    _LOGGER.info(f"Processing {len(all_recent_data_sorted)} valid data points from API (filtered {len(all_recent_data) - len(all_recent_data_sorted)} with non-zero status)")

                    # Find the earliest point in the fetched data
                    earliest_timestamp = None
                    if all_recent_data_sorted:
                        earliest_dt = dt_util.parse_datetime(all_recent_data_sorted[0]['date'])
                        if earliest_dt:
                            if earliest_dt.tzinfo is None:
                                earliest_dt = dt_util.as_local(earliest_dt)
                            earliest_timestamp = earliest_dt.timestamp()

                    # Get the sum just before the earliest fetched data point
                    # This is our starting point for recalculation
                    if earliest_timestamp:
                        # Get statistics just before our fetch period to get the correct starting sum
                        # Convert earliest_timestamp to datetime
                        earliest_dt_calc = datetime.fromtimestamp(earliest_timestamp)
                        if earliest_dt_calc.tzinfo is None:
                            earliest_dt_calc = dt_util.as_local(earliest_dt_calc)

                        # Get from beginning of time to just before earliest point
                        pre_period_stats = await get_instance(self.hass).async_add_executor_job(
                            statistics_during_period,
                            self.hass,
                            datetime.fromtimestamp(0),  # Unix epoch
                            earliest_dt_calc - timedelta(seconds=1),
                            {statistic_id},
                            "hour",
                            None,
                            {"sum"}
                        )

                        if statistic_id in pre_period_stats and pre_period_stats[statistic_id]:
                            # Get the last sum before our period
                            keepSum = pre_period_stats[statistic_id][-1].get('sum', 0) or 0
                            _LOGGER.info(f"Starting recalculation from sum={keepSum} at timestamp {earliest_timestamp}")
                        else:
                            keepSum = 0
                            _LOGGER.info(f"No previous statistics found, starting from sum=0")
                    else:
                        _LOGGER.warning("Could not determine earliest timestamp, using keepSum from last_stats")

                    for d in all_recent_data_sorted:
                        # Parse the date - API returns like "2025-09-01T00:00:00"
                        from_time = dt_util.parse_datetime(d['date'])
                        if from_time is None:
                            _LOGGER.error(f"Failed to parse date: {d['date']}")
                            continue

                        # Ensure datetime has timezone info (assume local timezone if none)
                        if from_time.tzinfo is None:
                            from_time = dt_util.as_local(from_time)

                        ts = from_time.timestamp()
                        keepSum += d['consumption']

                        # Check if this data point exists and if the value has changed
                        if ts in existing_stats_map:
                            existing_state = existing_stats_map[ts].get('state', 0) or 0
                            if abs(existing_state - d['consumption']) > 0.0001:  # Allow for floating point errors
                                _LOGGER.info(f"Value changed for {d['date']}: {existing_state} -> {d['consumption']} (diff: {d['consumption'] - existing_state})")
                                updates_count += 1
                            # Always include existing points in the recalculation
                        else:
                            _LOGGER.debug(f"New data point: date={d['date']}, consumption={d['consumption']}")
                            new_count += 1

                        statistics.append(
                            StatisticData(
                                start=from_time,
                                state=d['consumption'],
                                sum=keepSum,
                            )
                        )

                    _LOGGER.info(f"Found {new_count} new data points and {updates_count} updates in {len(statistics)} total points")

                    # Only add statistics if we have new or updated data
                    if statistics and (new_count > 0 or updates_count > 0):
                        _LOGGER.info(f"Writing {len(statistics)} statistics to database ({new_count} new, {updates_count} updates, final sum: {keepSum})")
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
                    else:
                        _LOGGER.info(f"No new or changed statistics to add (processed {len(statistics)} existing unchanged values)")

                return json_data
                # return await self.my_api.fetch_data(listening_idx)
        except ApiAuthError as err:
            # Raising ConfigEntryAuthFailed will cancel future updates
            # and start a config flow with SOURCE_REAUTH (async_step_reauth)
            raise ConfigEntryAuthFailed from err
        except ApiError as err:
            raise UpdateFailed(f"Error communicating with API: {err}")
