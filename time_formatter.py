import pandas as pd


class TimeFormatter:
    @staticmethod
    def format_datetime(df):
        """
        Formats datetime
        Returns new DataFrame with "start_time", "hour", "day", "weekofyear", "month", "year", "weekday"
        """

        t = pd.to_datetime(df['ts'], unit='ms')

        time_data = pd.concat(
            [t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year,
             t.dt.weekday], axis=1)

        column_labels = (
            "start_time",
            "hour",
            "day",
            "weekofyear",
            "month",
            "year",
            "weekday")

        time_df = pd.DataFrame(data=time_data.values, columns=column_labels)

        return time_df
