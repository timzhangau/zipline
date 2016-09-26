#
# Copyright 2016 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import numpy as np


class OrderedContracts(object):

    def __init__(self, root_symbol, asset_finder):
        contract_info = asset_finder.get_contract_info(root_symbol)
        size = len(contract_info)
        sids = np.full(size, 0, dtype=np.int64)
        start_dates = np.full(size, 0, dtype=np.int64)
        auto_close_dates = np.full(size, 0, dtype=np.int64)
        self._size = size
        for i, info in enumerate(contract_info):
            sid, start_date, auto_close_date = info
            sids[i] = sid
            start_dates[i] = start_date
            auto_close_dates[i] = auto_close_date
        self._sids = sids
        self._start_dates = start_dates
        self._auto_close_dates = auto_close_dates

    def contract_before_auto_close(self, dt_value):
        # TODO Cythonize
        for i, auto_close_date in enumerate(self._auto_close_dates):
            if auto_close_date > dt_value:
                break
        return self._sids[i]

    def contract_at_offset(self, sid, offset):
        # TODO Cythonize
        sids = self._sids
        for i in range(self._size):
            if sid == sids[i]:
                return sids[i + offset]

    def active_chain(self, starting_sid, dt_value):
        # TODO Cythonize
        left = right = 0
        sids = self._sids
        start_dates = self._start_dates

        for i in range(self._size):
            if starting_sid == sids[i]:
                left = i
                break

        for j in range(i, self._size):
            if start_dates[j] > dt_value:
                right = j
                break

        # TODO: Memory view?
        return sids[left:right+1]


class CalendarRollFinder(object):

    def __init__(self, trading_calendar, asset_finder):
        self.trading_calendar = trading_calendar
        self.asset_finder = asset_finder
        self._ordered_contracts = {}

    def _get_ordered_contracts(self, root_symbol):
        try:
            return self._ordered_contracts[root_symbol]
        except KeyError:
            oc = OrderedContracts(root_symbol, self.asset_finder)
            self._ordered_contracts[root_symbol] = oc
            return oc

    def get_contract_center(self, root_symbol, dt, offset):
        oc = self._get_ordered_contracts(root_symbol)
        primary_candidate = oc.contract_before_auto_close(dt.value)

        # Here is where a volume check would be.
        primary = primary_candidate
        return oc.contract_at_offset(primary, offset)
