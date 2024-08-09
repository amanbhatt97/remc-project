import pandas as pd

class FeatureEngineer:
    def __init__(self, lags=None, rolling_windows=None):
        if lags is None:
            self.lags = [1, 2, 3, 4, 6, 12, 24, 48, 94, 95, 96, 97, 98, 96*2, 96*3, 96*5]
        else:
            self.lags = lags
        
        if rolling_windows is None:
            self.rolling_windows = [3, 6]
        else:
            self.rolling_windows = rolling_windows

    def engineer_features(self, df, include_holidays=False):
        """
        Perform feature engineering on the solar load data.

        Parameters:
        - df (DataFrame): Input DataFrame with 'datetime' and 'power' columns.
        - include_holidays (bool): Whether to include holiday indicators.

        Returns:
        - DataFrame: Transformed DataFrame with engineered features.
        """
        df = self._resample_fill_missing(df)
        df = self._create_lag_features(df)
        df = self._create_rolling_statistics(df)
        df = self._extract_datetime_features(df)

        if include_holidays:
            df = self._include_holiday_indicators(df)

        df.dropna(inplace=True)
        return df

    def _resample_fill_missing(self, df):
        """Resample data to fill missing time intervals."""
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.set_index('datetime').sort_index()
        df = df.resample('15T').asfreq()
        return df

    def _create_lag_features(self, df):
        """Create lag features for autocorrelation."""
        for lag in self.lags:
            df[f'power_t-{lag}'] = df['power'].shift(lag)
        return df

    def _create_rolling_statistics(self, df):
        """Create rolling mean and std features."""
        for window in self.rolling_windows:
            df[f'rolling_mean_{window}h'] = df['power'].rolling(f'{window}H').mean()
            df[f'rolling_std_{window}h'] = df['power'].rolling(f'{window}H').std()
        return df

    def _extract_datetime_features(self, df):
        """Extract datetime components as features."""
        df['hour'] = df.index.hour
        df['minute'] = df.index.minute
        df['day_of_week'] = df.index.dayofweek
        df['day_of_month'] = df.index.day
        df['month'] = df.index.month
        return df

    def _include_holiday_indicators(self, df):
        """Include holiday indicators if needed."""
        # Add holiday indicators based on your specific dataset or external holiday data
        return df