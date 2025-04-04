from collections import namedtuple
from datetime import datetime
from datetime import timedelta
from datetime import timezone

__all__ = ("Conditions", "Timestamps", "days_to_seconds")


class _Config(namedtuple("_Config", ("stale_warning_offset_delta", "culled_offset_delta"))):
    @classmethod
    def from_config(cls, config):
        return cls(config.culling_stale_warning_offset_delta, config.culling_culled_offset_delta)


class _WithConfig:
    def __init__(self, config):
        self.config = config

    @classmethod
    def from_config(cls, config):
        config = _Config.from_config(config)
        return cls(config)


class Timestamps(_WithConfig):
    @staticmethod
    def _add_time(timestamp, delta):
        return timestamp + delta

    def stale_timestamp(self, stale_timestamp, stale_seconds):
        return self._add_time(stale_timestamp, timedelta(seconds=stale_seconds))

    def stale_warning_timestamp(self, stale_timestamp, stale_warning_seconds):
        return self._add_time(stale_timestamp, timedelta(seconds=stale_warning_seconds))

    def culled_timestamp(self, stale_timestamp, culled_seconds):
        return self._add_time(stale_timestamp, timedelta(seconds=culled_seconds))


class Conditions:
    def __init__(self, staleness, host_type):
        self.now = datetime.now(timezone.utc)
        self.host_type = host_type

        self.staleness_host_type = {
            None: {
                "stale": staleness["conventional_time_to_stale"],
                "warning": staleness["conventional_time_to_stale_warning"],
                "culled": staleness["conventional_time_to_delete"],
            },
            "edge": {
                "stale": staleness["immutable_time_to_stale"],
                "warning": staleness["immutable_time_to_stale_warning"],
                "culled": staleness["immutable_time_to_delete"],
            },
        }

    def fresh(self):
        return self._stale_timestamp(), None

    def stale(self):
        return self._stale_warning_timestamp(), self._stale_timestamp()

    def stale_warning(self):
        return self._culled_timestamp(), self._stale_warning_timestamp()

    def culled(self):
        return None, self._culled_timestamp()

    def not_culled(self):
        return self._culled_timestamp(), None

    def _stale_timestamp(self):
        offset = timedelta(seconds=self.staleness_host_type[self.host_type]["stale"])
        return self.now - offset

    def _stale_warning_timestamp(self):
        offset = timedelta(seconds=self.staleness_host_type[self.host_type]["warning"])
        return self.now - offset

    def _culled_timestamp(self):
        offset = timedelta(seconds=self.staleness_host_type[self.host_type]["culled"])
        return self.now - offset

    @staticmethod
    def find_host_state(stale_timestamp, stale_warning_timestamp):
        now = datetime.now(timezone.utc)
        if now < stale_timestamp:
            return "fresh"
        if now >= stale_timestamp and now < stale_warning_timestamp:
            return "stale"
        else:
            return "stale warning"


def days_to_seconds(n_days: int) -> int:
    factor = 86400
    return n_days * factor
