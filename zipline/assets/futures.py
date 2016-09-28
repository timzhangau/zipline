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

# http://www.cmegroup.com/product-codes-listing/month-codes.html
CME_CODE_TO_MONTH = dict(zip('FGHJKMNQUVXZ', range(1, 13)))
MONTH_TO_CME_CODE = dict(zip(range(1, 13), 'FGHJKMNQUVXZ'))


class OrderedContracts(object):

    def __init__(self,
                 root_symbol,
                 contract_sids,
                 start_dates,
                 auto_close_dates):
        self.root_symbol = root_symbol
        self.contract_sids = contract_sids
        self.start_dates = start_dates
        self.auto_close_dates = auto_close_dates

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


class ContinuousFuture(object):

    def __init__(self, symbol, offset, roll, start_date, end_date):
        self.symbol = symbol
        self.offset = offset
        self.roll = roll
        self.start_date = start_date
        self.end_date = end_date
