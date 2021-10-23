from abc import ABCMeta, abstractmethod
from typing import AnyStr, List

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType, IntegerType


class BaseEvent(metaclass=ABCMeta):
    """Abstract event
    """

    @property
    @abstractmethod
    def event_type(self) -> AnyStr:
        pass

    @property
    @abstractmethod
    def columns(self) -> List:
        pass

    def _extract_events(self, df: DataFrame) -> DataFrame:
        """Extract events from df based on event type

        Args:
            df(DataFrame):

        Returns:
            DataFrame
        """
        return df.filter(df.event == self.event_type)


class RegisteredEvent(BaseEvent):
    def transform(self, df) -> DataFrame:
        """Transform event

        Args:
            df:

        Returns:

        """
        events = self._extract_events(df)

        events = events \
            .withColumn('event', events.event.cast(StringType())) \
            .withColumn('time', F.to_timestamp(F.col('timestamp'))) \
            .withColumn('initiator_id',
                        events.initiator_id.cast(IntegerType())) \
            .withColumn('channel', events.channel.cast(StringType()))

        return events.select(self.columns)

    @property
    def event_type(self):
        return 'registered'

    @property
    def columns(self):
        return [
            F.col('event'),
            F.col('time'),
            F.col('initiator_id'),
            F.col('channel'),
        ]


class AppLoadEvent(BaseEvent):
    def transform(self, df):
        events = self._extract_events(df)

        events = events \
            .withColumn('event', events.event.cast(StringType())) \
            .withColumn('time', F.to_timestamp(F.col('timestamp'))) \
            .withColumn('initiator_id',
                        events.initiator_id.cast(IntegerType())) \
            .withColumn('device_type', events.device_type.cast(StringType()))

        return events.select(self.columns)

    @property
    def event_type(self):
        return 'app_loaded'

    @property
    def columns(self):
        return [
            F.col('event'),
            F.col('time'),
            F.col('initiator_id'),
            F.col('device_type')
        ]
