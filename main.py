import configparser
import csv
import datetime
import math
import os

import ccxt

_1_minute = 1000 * 60
_15_minutes = _1_minute * 15
_1_hour = _1_minute * 60
_4_hours = _1_hour * 4
_1_day = _1_hour * 24
_1_week = _1_day * 7
_1_month = _1_day * 30
_1_year = _1_month * 12


def tf2i(timeframe) -> int:
    if timeframe == '1m':
        return _1_minute
    elif timeframe == '15m':
        return _15_minutes
    elif timeframe == '1h':
        return _1_hour
    elif timeframe == '4h':
        return _4_hours
    elif timeframe == '1d':
        return _1_day


def to_kline(tfi: int, ms: int) -> int:
    return int(ms) - int(divmod(ms, tfi)[1])


def kline_count(exchange: object, tfi: int, since: int) -> int:
    kline = to_kline(tfi, exchange.milliseconds())
    # print('kline: ' + str(exchange.iso8601(kline)))
    # print('since: ' + str(exchange.iso8601(since)))
    return int((int(kline) - int(since)) / int(tfi))


def find_earliest_kline(exchange, symbol=None, timeframe='1d', tfi=_1_day, backwarddelta=_1_year, fromdate=None,
                        todate=None,
                        fdexists=None, tdexists=None) -> int:
    o_fd = 0
    o_td = 0
    o_fdexists = None
    o_tdexists = None

    if fromdate is None:
        i_fd = exchange.milliseconds()
        i_td = i_fd
        i_fdexists = True
        i_tdexists = True
    else:
        i_fd = fromdate
        i_td = todate
        i_fdexists = fdexists
        i_tdexists = tdexists

    # print(str(exchange.iso8601(i_fd)) + " " + str(exchange.iso8601(i_td)) + " " + (
    #     "None" if i_fdexists is None else ("True" if i_fdexists else "False")) + " " + (
    #           "None" if i_tdexists is None else ("True" if i_tdexists else "False")))

    if i_fdexists:
        if i_tdexists:

            o_fd = i_fd - backwarddelta

            o_fd -= divmod(o_fd, tfi)[1]

            ohlcv = exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=o_fd, limit=1)

            if len(ohlcv) and ohlcv[0][0] == o_fd:
                o_fd = o_fd
                o_td = o_fd
                o_fdexists = True
                o_tdexists = True
            else:
                o_fd = o_fd
                o_td = i_fd
                o_fdexists = False
                o_tdexists = True
        else:
            return 1
    else:
        if i_tdexists:

            if tfi <= i_td - i_fd < tfi * 2:
                return i_td

            md = int((i_fd + i_td) / 2)

            md -= divmod(md, tfi)[1]

            ohlcv = exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=md, limit=1)

            if len(ohlcv) and ohlcv[0][0] == md:
                o_fd = i_fd
                o_td = md
                o_fdexists = False
                o_tdexists = True
            else:
                o_fd = md
                o_td = i_td
                o_fdexists = False
                o_tdexists = True
        else:
            return 2

    return find_earliest_kline(exchange, symbol, timeframe, tfi, backwarddelta, o_fd, o_td, o_fdexists, o_tdexists)


def csv_filename(exchange, active, dirname, symbol=None, timeframe='1d') -> str:
    if not os.path.exists(dirname):
        os.mkdir(dirname)

    filename = dirname + exchange.name + "_" + symbol.replace('/', '_') + "_" + timeframe + ".csv"

    if active:
        return filename
    else:
        if os.path.exists(filename):
            os.remove(filename)
            return None


def fetch_klines(exchange, filename, symbol=None, timeframe='1d', klines_per_step: int = 100,
                 backward_delta_years: int = 1):
    tfi: int = tf2i(timeframe)
    start_kline = 0
    # print('Borsa zamanı: ' + str(exchange.iso8601(exchange.milliseconds())))
    exc_last_kl = to_kline(tfi, exchange.milliseconds())
    # print('exc_last_kl: ' + str(exchange.iso8601(exc_last_kl)))

    try:
        if os.path.exists(filename) and os.stat(filename).st_size > 0:
            f = open(filename, 'r+', newline='', encoding='utf-8')

            csv_reader = csv.reader(f, delimiter=',')

            # son satıra gel, son tarihi al
            for line in csv_reader:
                pass

            start_kline = int(line[0]) + int(tfi)

            # print("1 start_kline: " + str(exchange.iso8601(start_kline)))

        else:
            f = open(filename, 'w', newline='', encoding='utf-8')

            start_kline = find_earliest_kline(exchange, symbol, timeframe, tfi, _1_year * int(backward_delta_years))
            # start_kline = 1502928000000
            # print("2 last_kline: " + str(exchange.iso8601(start_kline)))

        # bulduğun tarihten şu ana kadarını çek dosyaya yaz
        kl_count: int = kline_count(exchange, tfi, int(start_kline))
        # print('kl_count: ' + str(kl_count))
        step_count = int(math.ceil(int(kl_count) / int(klines_per_step)))
        # print('step_count: ' + str(step_count))

        csv_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        for i in range(step_count):
            # print('i: ' + str(i))
            start_kline_at_step = int(start_kline) + int(i) * int(klines_per_step) * int(tfi)
            # print('start_kline_at_step: ' + str(exchange.iso8601(start_kline_at_step)))
            rows = exchange.fetch_ohlcv(symbol, timeframe, since=start_kline_at_step, limit=int(klines_per_step))
            # print('Silmeden önceki len(rows): ' + str(len(rows)) + ', ' + str(exchange.iso8601(rows[0][0])) + ' - ' + str(exchange.iso8601(rows[-1][0])))
            # son kline kapanmamış olabilir, çıkaralım
            if len(rows) > 0:
                if rows[-1][0] == exc_last_kl:
                    # print('Silmeden önceki son kline' + str(exchange.iso8601(rows[-1][0])))
                    del rows[-1]
                    # print('Sildikten sonraki son kline' + str(exchange.iso8601(rows[-1][0])))
            # print('Sildikten sonraki len(rows): ' + str(len(rows)) + ', ' + str(exchange.iso8601(rows[0][0])) + ' - ' + str(exchange.iso8601(rows[-1][0])))

            if len(rows) > 0:
                csv_writer.writerows(rows)
    finally:
        f.close()


def main():
    exchange = ccxt.binance()
    markets = exchange.fetch_markets()

    config = configparser.ConfigParser()
    config.read('config.txt')

    # ohlcv_files_directory = "klines/"
    ohlcv_files_directory = str(config['settings']['ohlcv_files_directory'])
    # print(ohlcv_files_directory)
    # timeframes_to_fetch = ['1h', '4h', '1d']
    timeframes_to_fetch = config['settings']['timeframes_to_fetch'].split(',')
    klines_per_step = config['settings']['klines_per_step']
    backward_delta_years = config['settings']['backward_delta_years']
    # for tf in range(len(timeframes_to_fetch)):
    #     print(timeframes_to_fetch[tf])
    whitelist = config['whitelist']['members'].split('\n')
    whitelist.remove('')
    # print('len(whitelist): ' + str(len(whitelist)))
    # print(whitelist)
    # whitelist = ['BTC/USDT', 'ETH/USDT']
    blacklist = config['blacklist']['members'].split('\n')
    blacklist.remove('')
    # print('len(blacklist): ' + str(len(blacklist)))
    # print(blacklist)
    # blacklist = ['DOGE/USDT', 'SHIB/USDT']

    market: object
    for market in markets:
        if (len(whitelist) == 0 or (len(whitelist) > 0 and market['symbol'] in whitelist)) and \
                (len(blacklist) == 0 or (len(blacklist) > 0 and market['symbol'] not in blacklist)):
            if market['quote'] == 'USDT' and 'SPOT' in market['info']['permissions'] and market['spot']:
                # print(market['symbol'])
                for tf in range(len(timeframes_to_fetch)):
                    filename = csv_filename(exchange, market['active'], ohlcv_files_directory, symbol=market['symbol'],
                                            timeframe=timeframes_to_fetch[tf])
                    if filename:
                        fetch_klines(exchange, filename, symbol=market['symbol'], timeframe=timeframes_to_fetch[tf],
                                     klines_per_step=klines_per_step, backward_delta_years=backward_delta_years)


if __name__ == '__main__':
    start=datetime.datetime.now()
    print('Başlangıç zamanı: ' + str(start))
    main()
    end=datetime.datetime.now()
    print('Bitiş zamanı: ' + str(end))
    print('Toplam süre: ' + str(end-start))
