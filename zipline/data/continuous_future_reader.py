import numpy as np
from zipline.data.session_bars import SessionBarReader


class ContinuousFutureSessionBarReader(SessionBarReader):

    def __init__(self, bar_reader, roll_finders):
        self._bar_reader = bar_reader
        self._roll_finders = roll_finders

    def load_raw_arrays(self, columns, start_date, end_date, assets):
        """
        Parameters
        ----------
        fields : list of str
           'open', 'high', 'low', 'close', or 'volume'
        start_dt: Timestamp
           Beginning of the window range.
        end_dt: Timestamp
           End of the window range.
        sids : list of int
           The asset identifiers in the window.

        Returns
        -------
        list of np.ndarray
            A list with an entry per field of ndarrays with shape
            (minutes in range, sids) with a dtype of float64, containing the
            values for the respective field over start and end dt range.
        """
        rolls = {}
        for asset in assets:
            rf = self._roll_finders[asset.roll]
            rolls[asset] = rf.get_rolls(
                asset, start_date, end_date)
        num_sessions = len(
            self.trading_calendar.sessions_in_range(start_date, end_date))
        shape = num_sessions, len(assets)

        results = []

        for column in columns:
            if column != 'volume':
                out = np.full(shape, np.nan)
            else:
                out = np.zeros(shape, dtype=np.uint32)
            for i, asset in enumerate(assets):
                # TODO Rolls here.
                out[i] = self._bar_reader.load_raw_arrays(
                    [column], start_date, end_date)
        return results

    @property
    def last_available_dt(self):
        """
        Returns
        -------
        dt : pd.Timestamp
            The last session for which the reader can provide data.
        """
        return self._bar_reader.last_available_dt

    @property
    def trading_calendar(self):
        """
        Returns the zipline.utils.calendar.trading_calendar used to read
        the data.  Can be None (if the writer didn't specify it).
        """
        return self._bar_reader.trading_calendar

    @property
    def first_trading_day(self):
        """
        Returns
        -------
        dt : pd.Timestamp
            The first trading day (session) for which the reader can provide
            data.
        """
        return self._bar_reader.first_trading_day

    def get_value(self, continuous_future, dt, field):
        """
        Retrieve the value at the given coordinates.

        Parameters
        ----------
        sid : int
            The asset identifier.
        dt : pd.Timestamp
            The timestamp for the desired data point.
        field : string
            The OHLVC name for the desired data point.

        Returns
        -------
        value : float|int
            The value at the given coordinates, ``float`` for OHLC, ``int``
            for 'volume'.

        Raises
        ------
        NoDataOnDate
            If the given dt is not a valid market minute (in minute mode) or
            session (in daily mode) according to this reader's tradingcalendar.
        """
        rf = self._roll_finders[continuous_future.roll]
        sid = (rf.get_contract_center(continuous_future.root_symbol,
                                      dt,
                                      continuous_future.offset))
        return self._bar_reader.get_value(sid, dt, field)

    def get_last_traded_dt(self, asset, dt):
        """
        Get the latest minute on or before ``dt`` in which ``asset`` traded.

        If there are no trades on or before ``dt``, returns ``pd.NaT``.

        Parameters
        ----------
        asset : zipline.asset.Asset
            The asset for which to get the last traded minute.
        dt : pd.Timestamp
            The minute at which to start searching for the last traded minute.

        Returns
        -------
        last_traded : pd.Timestamp
            The dt of the last trade for the given asset, using the input
            dt as a vantage point.
        """
        pass

    @property
    def sessions(self):
        """
        Returns
        -------
        sessions : DatetimeIndex
           All session labels (unionining the range for all assets) which the
           reader can provide.
        """
        pass
