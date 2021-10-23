from pyspark.sql import functions as F
from pyspark.sql.window import *


class MetricActiveUserAfterRegistration:

    def __init__(self, total_users: int, active_users: int):
        self.active_users = active_users
        self.total_users = total_users

    @property
    def percentage_active_users(self):
        return int(round((self.active_users / self.total_users) * 100, 2))


class DataStatistics:

    def get_active_user_after_registration(
            self,registered_events_df, app_loaded_events_df) \
            -> MetricActiveUserAfterRegistration:

        for column in [column for column in app_loaded_events_df.columns if
                       column not in registered_events_df.columns]:
            registered_events_df = registered_events_df\
                .withColumn(column, F.lit(None))

        for column in [column for column in registered_events_df.columns if
                       column not in app_loaded_events_df.columns]:
            app_loaded_events_df = app_loaded_events_df\
                .withColumn(column, F.lit(None))

        # merge two events to a single DF
        events_df = registered_events_df.unionByName(app_loaded_events_df)

        # calculate user_events_sequence based on the time event has occurred
        events_df = events_df\
            .withColumn("user_events_sequence", F.row_number()
                        .over(Window
                              .partitionBy("initiator_id")
                              .orderBy(["time"]))
                        )

        # calculate the week number in the year
        events_df = events_df.withColumn("week", F.weekofyear(
            F.col("time")))

        # for each event set the sequence number of the next event
        events_df = events_df\
            .withColumn('next_event_week',
                        F.lead('week')
                        .over(Window.partitionBy('initiator_id')
                            .orderBy('user_events_sequence')))

        # calculate the difference between current event and the next event in
        # weeks
        events_df = events_df\
            .withColumn('week_diff_with_next_event',
                        F.col('next_event_week') - F.col('week'))

        # select only users with app_loaded event that happened 1 week
        # after the registered event
        count_active_users = events_df.filter(
            "user_events_sequence =1 "
            "AND event ='registered' "
            "AND week_diff_with_next_event = 1").count()

        # count all registered users
        count_all_users = events_df.filter(
                "user_events_sequence =1 "
                "AND event ='registered' "
                "AND week_diff_with_next_event is not null").count()

        # create a metric about active users
        metric = MetricActiveUserAfterRegistration(
                active_users=count_active_users,
                total_users=count_all_users)

        return metric
