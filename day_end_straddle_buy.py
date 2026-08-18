"""
start time 15:00 end time 15:10

buy sensex ATM straddle at 15:00 and exit the straddle at 15:10
each leg should have a stoploss of 25%

get sensex spot value using dhan api and execute in aliceblue account

Reuses execution_rolling_straddle's Dhan/AliceBlue REST plumbing (auth, contract master, option
chain, order placement, tick rounding) - importing it also runs its Dhan/AliceBlue auth checks.
Like that script, no MARKET orders are used: entry and exit are LIMIT orders priced 1% through the
option's LTP, and the resting stoploss is an SL (stop-loss LIMIT) order.
"""

import time as time_module
from datetime import datetime, time as dtime

import execution_rolling_straddle as ers

log = ers.log

SYMBOL = 'SENSEX'
CFG = ers.UNDERLYINGS[SYMBOL]
START_TIME = dtime(15, 0)
END_TIME = dtime(15, 10)
STOPLOSS_PCT = 0.25


def buy_leg_with_stoploss(instrument, quantity, ltp):
    """Mirror of ers.short_leg_with_stoploss, but to open a long: BUY to enter, SELL stoploss
    below entry (protects against the bought premium losing value)."""
    entry_price = ers._round_to_tick(ltp * (1 + ers.LIMIT_OFFSET_PCT), instrument.tick_size)
    tag = f'{"[DRY RUN] " if ers.DRY_RUN else ""}BUY {quantity} x {instrument.name} LIMIT @ {entry_price} (ltp {ltp})'
    log.info(tag)
    if ers.DRY_RUN:
        return

    entry = ers._place_order(
        'BUY', instrument, quantity, 'LIMIT', price=str(entry_price), order_tag='day_end_straddle_entry',
    )
    order_no = entry.get('brokerOrderId')
    if not order_no:
        raise RuntimeError(f'{instrument.name} entry order rejected: {entry}')

    entry_price = ers._wait_for_fill_price(order_no)
    trigger_price = round(entry_price * (1 - STOPLOSS_PCT), 1)
    sl_limit_price = ers._round_to_tick(trigger_price * (1 - ers.LIMIT_OFFSET_PCT), instrument.tick_size)
    log.info(f'{instrument.name} entered @ {entry_price}, SL trigger {trigger_price} limit {sl_limit_price}')

    ers._place_order(
        'SELL', instrument, quantity, 'SL', price=str(sl_limit_price),
        trigger_price=trigger_price, order_tag='day_end_straddle_sl',
    )


def sell_leg(instrument, quantity, ltp):
    """Mirror of ers.close_leg, but for squaring off a long: cancel the resting SL, then SELL."""
    exit_price = ers._round_to_tick(ltp * (1 - ers.LIMIT_OFFSET_PCT), instrument.tick_size)
    tag = f'{"[DRY RUN] " if ers.DRY_RUN else ""}SELL (square off) {quantity} x {instrument.name} LIMIT @ {exit_price} (ltp {ltp})'
    log.info(tag)
    if ers.DRY_RUN:
        return

    for o in ers._order_book():
        if str(o.get('instrumentId')) == str(instrument.token) and str(o.get('orderStatus', '')).lower() not in ers.TERMINAL_ORDER_STATUSES:
            ers._cancel_order(o['brokerOrderId'])

    ers._place_order(
        'SELL', instrument, quantity, 'LIMIT', price=str(exit_price), order_tag='day_end_straddle_exit',
    )


def _load_contracts_and_option_chain():
    contracts = ers.load_current_week_options(SYMBOL, CFG['aliceblue_exchange'])
    expiry_date = datetime.fromtimestamp(contracts[0]['expiry_date'] / 1000, tz=ers.timezone.utc).strftime('%Y-%m-%d')
    option_chain = ers.get_option_chain(expiry_date, CFG['dhan_security_id'], CFG['dhan_segment'])
    return contracts, option_chain


def enter_straddle():
    spot = ers.get_spot_ltp(CFG['dhan_security_id'], CFG['dhan_segment'])
    strike = ers.atm_strike(spot, CFG['strike_interval'])
    log.info(f'{datetime.now():%H:%M:%S} {SYMBOL} spot={spot} atm_strike={strike}')

    contracts, option_chain = _load_contracts_and_option_chain()
    contracts_by_strike_type = {(int(float(c['strike_price'])), c['option_type']): c for c in contracts}

    for opt in ('CE', 'PE'):
        contract = contracts_by_strike_type[(strike, opt)]
        instrument = ers._to_instrument(contract)
        ltp = ers.option_ltp(option_chain, strike, opt)
        buy_leg_with_stoploss(instrument, instrument.lot_size * CFG['lots'], ltp)


def exit_straddle():
    contracts, option_chain = _load_contracts_and_option_chain()
    contracts_by_token = {int(c['token']): c for c in contracts}

    open_legs = ers.get_open_legs(contracts_by_token)
    if not open_legs:
        log.info(f'No open {SYMBOL} positions to exit')
        return

    for token, pos in open_legs.items():
        contract = contracts_by_token[token]
        ltp = ers.option_ltp(option_chain, int(float(contract['strike_price'])), contract['option_type'])
        sell_leg(ers._to_instrument(contract), abs(int(pos['netQuantity'])), ltp)


def _wait_until(target_time, what):
    now = datetime.now()
    target = datetime.combine(now.date(), target_time)
    wait = (target - now).total_seconds()
    if wait > 0:
        log.info(f'waiting until {target_time} to {what}...')
        time_module.sleep(wait)
    else:
        log.info(f'{target_time} already passed ({-wait:.0f}s ago) - {what} now')


def run_day():
    now = datetime.now().time()
    if now >= END_TIME:
        log.info(f'{END_TIME} already passed for today - nothing to do')
        return

    if now >= START_TIME:
        log.info(f'Started after {START_TIME} - entering the {SYMBOL} day-end straddle immediately')
        enter_straddle()
    else:
        _wait_until(START_TIME, f'enter the {SYMBOL} day-end straddle (BUY)')
        enter_straddle()

    _wait_until(END_TIME, f'exit the {SYMBOL} day-end straddle')
    exit_straddle()


if __name__ == '__main__':
    run_day()
