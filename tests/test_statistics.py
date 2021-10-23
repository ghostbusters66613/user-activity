"""
{"event":"registered", "timestamp":"2020-02-11T06:21:14.000Z",
"initiator_id":1} //week#1: Monday
{"event":"registered", "timestamp":"2020-02-11T07:00:14.000Z",
"initiator_id":2} //week#1: Monday

{"event":"app_loaded", "timestamp":"2020-03-11T06:24:42.000Z",
"initiator_id":1, "device_type":"desktop"} //week#1: Tuesday

{"event":"registered", "timestamp":"2020-03-11T07:00:14.000Z",
"initiator_id":3} //week#1: Wednesday

{"event":"app_loaded", "timestamp":"2020-11-11T10:13:42.000Z",
"initiator_id":2, "device_type":"desktop"} //week#2: Wednesday

{"event":"app_loaded", "timestamp":"2020-12-11T11:08:42.000Z",
"initiator_id":2, "device_type":"desktop"} //week#2: Thursday

{"event":"app_loaded", "timestamp":"2020-17-11T11:08:42.000Z",
"initiator_id":3, "device_type":"mobile"} //week#3: Tuesday
"""
from app.metrics_builder import DataStatistics
import pytest


@pytest.mark.parametrize('registered_data,app_loaded_data,expected', [
    ([('registered', "2020-11-02T06:21:14.000Z", 1),
      ('registered', "2020-11-02T07:00:14.000Z", 2),
      ('registered', "2020-11-03T07:00:14.000Z", 3)],
        [('app_loaded', "2020-11-03T06:24:42.000Z", 1),
         ('app_loaded', "2020-11-11T10:13:42.000Z", 2),
         ('app_loaded', "2020-11-12T11:08:42.000Z", 2),
         ('app_loaded', "2020-11-17T11:08:42.000Z", 3)],
        33
     ),
    ([('registered', "2020-11-02T06:21:14.000Z", 1),
      ('registered', "2020-11-02T07:00:14.000Z", 2),
      ('registered', "2020-11-03T07:00:14.000Z", 3)],
     [('app_loaded', "2020-11-03T06:24:42.000Z", 1),
      ('app_loaded', "2020-11-19T10:13:42.000Z", 2),
      ('app_loaded', "2020-11-20T11:08:42.000Z", 2),
      ('app_loaded', "2020-11-17T11:08:42.000Z", 3)],
     0
     )
])
def test_metric_get_active_user_after_registration(
        registered_data, app_loaded_data, expected, spark):
    registered_df = spark\
        .createDataFrame(registered_data, ['event', 'time', 'initiator_id'],
    )
    app_loaded_df = spark\
        .createDataFrame(app_loaded_data, ['event', 'time', 'initiator_id'],
    )

    stat = DataStatistics()
    res = stat\
        .get_active_user_after_registration(registered_df, app_loaded_df)

    assert expected == res.percentage_active_users
