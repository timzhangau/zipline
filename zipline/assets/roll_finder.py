class CalendarRollFinder(object):

    def __init__(self, trading_calendar, asset_finder):
        self.trading_calendar = trading_calendar
        self.asset_finder = asset_finder

    def get_rolls(self, root_symbol, offset, start, end, limit):
        min_auto_close_session = end + self.trading_calendar.day * 2
        contract_sids = self.asset_finder.get_active_contracts(
            root_symbol,
            start,
            min_auto_close_session)
        # TODO: Put in get_active_contracts
        contract_sids = contract_sids[(-limit + offset):]
        contracts = [self.asset_finder.retrieve_asset(sid)
                     for sid in contract_sids]
        return [(contract, contract.auto_close_date) for contract in contracts]
