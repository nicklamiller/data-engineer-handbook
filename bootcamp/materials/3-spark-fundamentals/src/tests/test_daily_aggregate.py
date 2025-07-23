from collections import namedtuple
from datetime import date

from chispa import assert_df_equality

from ..jobs.daily_aggregate import do_daily_aggregate_transformation



Event = namedtuple("Event", ["host", "event_time", "user_id"])
DailyAggregate = namedtuple("DailyAggregate", ["host", "date", "hits", "unique_visitors"])

def test_daily_aggregate(spark):
    ds = "2023-01-31"
    input_data = [
        Event(host='a.com', event_time='2023-01-31 10:00:00', user_id='1'),
        Event(host='a.com', event_time='2023-01-31 11:00:00', user_id='1'), # Duplicate user, should not increment unique_visitors
        Event(host='a.com', event_time='2023-01-31 12:00:00', user_id='2'),

        Event(host='b.com', event_time='2023-01-31 14:00:00', user_id='1'),

        Event(host='a.com', event_time='2023-02-01 10:00:00', user_id='1'),

        Event(host='b.com', event_time='2023-01-31 15:00:00', user_id=None),
    ]
    source_df = spark.createDataFrame(input_data)
    actual_df = do_daily_aggregate_transformation(spark, source_df, ds)
    expected_data = [
        DailyAggregate(
            host='a.com',
            date=date(2023, 1, 31), # The function casts event_time to a date
            hits=3,
            unique_visitors=2
        ),
        DailyAggregate(
            host='b.com',
            date=date(2023, 1, 31),
            hits=2, # 1 from user '1' and 1 from the null user
            unique_visitors=1 # COUNT(DISTINCT) ignores nulls
        )
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_nullable=True
    )
