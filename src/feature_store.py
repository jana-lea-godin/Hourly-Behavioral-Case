import pandas as pd


class FeatureStore:

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def clean_percent_columns(self):
        percent_cols = [
            "Rate_View_Without_Purchase",
            "CR_Products_Added_to_Cart",
            "CR_Orders_Created",
            "CR_Ready_To_Ship",
            "CR_Ready_To_Ship_over_Orders_Created"
        ]

        for col in percent_cols:
            self.df[col] = (
                self.df[col]
                .str.replace("%", "", regex=False)
                .str.replace(",", ".", regex=False)
                .astype(float) / 100
            )

        return self

    def prepare_datetime(self):
        self.df["Datetime"] = pd.to_datetime(self.df["Datetime"])
        self.df = self.df.sort_values("Datetime")
        self.df.set_index("Datetime", inplace=True)
        return self

    def rolling_baseline(self, column, window_hours=168):
        rolling_mean = self.df[column].rolling(window=window_hours, min_periods=window_hours).mean()
        rolling_std = self.df[column].rolling(window=window_hours, min_periods=window_hours).std()

        z_score = (self.df[column] - rolling_mean) / rolling_std

        self.df[f"{column}_rolling_mean"] = rolling_mean
        self.df[f"{column}_rolling_std"] = rolling_std
        self.df[f"{column}_zscore"] = z_score

        return self

    def get_df(self):
        return self.df