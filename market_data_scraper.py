import yfinance
import pandas as pd
from bs4 import BeautifulSoup
import requests
import time
import datetime
import logging
import os
import traceback



def safe_float_conversion(val: any) -> float:
    if isinstance(val, str):

        acceptable = '1,2,3,4,5,6,7,8,9,0,.,-'.split(',')
        clean_str = ''
        for char in val:
            if char in acceptable:
                clean_str += char

        try:
            val = float(clean_str)
        except ValueError:
            print('ERROR couldn\'t convert to float: ', val)
            return 0.0

    if isinstance(val, float):
        return val

    if isinstance(val, int):
        return float(val)


def safe_int_conversion(v):
    if isinstance(v, str):
        if v[-1].isnumeric():
            return int(v)
    if isinstance(v, int):
        return v
    else:
        return 0


def convert_alphanumeric_volume(text_value):
    complete = False
    if 'M' in text_value:
        if '.' in text_value:
            val = text_value.split('.')
            mil = val[0]
            dec = val[1][:-1]
            if len(dec) == 2:
                dec = dec + '0'
            elif len(dec) == 1:
                dec = dec + '00'
            complete = f'{mil}{dec}000'

        else:
            val = text_value.split('M')
            complete = f'{val[0]}000000'


    elif 'B' in text_value:
        if '.' in text_value:
            val = text_value.split('.')
            mil = val[0]
            dec = val[1][:-1]
            if len(dec) == 2:
                dec = dec + '0'
            elif len(dec) == 1:
                dec = dec + '00'
            complete = f'{mil}{dec}000000'

        else:
            val = text_value.split('B')
            complete = f'{val[0]}000000000'

    elif 'T' in text_value:
        if '.' in text_value:
            val = text_value.split('.')
            mil = val[0]
            dec = val[1][:-1]
            if len(dec) == 2:
                dec = dec + '0'
            elif len(dec) == 1:
                dec = dec + '00'
            complete = f'{mil}{dec}000000000'

        else:
            val = text_value.split('B')
            complete = f'{val[0]}000000000000'

    else:
        return convert_data_strp_number(text_value)

    if complete is False:
        return 0

    return int(complete)


def convert_data_strp_number(value):
    """
    Strips all non-numeric characters, and checks to see if 'value' is numeric. If it is, will return a `float` type
    :param value: string type object
    :return: will return 'float' if value is numeric, will return 'none' if not.
    """
    value = value.strip()
    if len(value) > 2 and '-' in value:
        numbers = "1,2,3,4,5,6,7,8,9,0,.,-,+".split(',')
    else:
        numbers = "1,2,3,4,5,6,7,8,9,0,.,,+".split(',')
    value = list(value)
    data_actual = list()
    for char in value:
        if char in numbers:
            data_actual.append(char)
    if len(data_actual) > 0:
        if len(data_actual) > 1:
            data_actual = "".join(data_actual)
            if '.' in data_actual:
                return float(data_actual)
            else:
                return int(data_actual)
        if len(data_actual) == 1:
            data_actual = data_actual[0]
            return int(data_actual)
    else:
        return 0


class MarketDataScraper:

    def __init__(self):
        self._session = requests.Session()

        self._marketbeat_unusual_calls_vol_url = \
            'https://www.marketbeat.com/market-data/unusual-call-options-volume/'

        # This url is for unusual Put options activity
        self._marketbeat_unusual_puts_vol_url = \
            'https://www.marketbeat.com/market-data/unusual-put-options-volume/'

        # The url is for index futures, and commodity futures
        self._yf_futures_url = 'https://finance.yahoo.com/commodities'

        # This url is for the Index data table
        self._yf_index_data_url = 'https://finance.yahoo.com/world-indices'

        # This url is for Crypto Data table
        self._yf_crypto_data_url = 'https://finance.yahoo.com/cryptocurrencies'

        # This url is for Trending Tickers table (most searched)
        self._yf_trending_tickers_url = 'https://finance.yahoo.com/trending-tickers'

        # This url is for the Most Active tickers
        self._yf_most_active_url = 'https://finance.yahoo.com/most-active'

        # This url is for the top Gaining tickers
        self._yf_top_gainers_url = 'https://finance.yahoo.com/gainers'

        # This url is for the top Losing tickers
        self._yf_top_losers_url = 'https://finance.yahoo.com/losers'

        # This url is for the major currencies data table on yfinance
        self._yf_currencies_url = 'https://finance.yahoo.com/currencies'

        # url for sp 500 tickers sorted by index weight
        self._slick_charts_sp_500_url = 'https://www.slickcharts.com/sp500'

    def __str__(self):
        return 'market_data.MiscMarketData()'

    def make_soup(self, url):
        data = False
        try:
            raw_page = self._session.get(url)
            data = BeautifulSoup(raw_page.content, 'lxml')
        except Exception as e:
            logging.exception(f'{self.__str__()}.make_soup() - ERROR on {url}', exc_info=traceback.format_exc())
        finally:
            return data

    def _get_marketbeat_unusual_option_volume(self, call=False, put=False) -> list:
        """Return unusual option volume for either call or put"""

        def get_ticker_from_column(td_tag):
            """Returns the ticker str from the column data html"""
            d = False
            try:
                comp_name_data = td_tag.find_all('div')

                # Some of the html is fucked up, ticker will be in either 0 or 1
                zero = comp_name_data[0].text
                one = comp_name_data[1].text

                if len(zero) > 0:
                    if zero[-1].isupper():
                        d = zero

                else:
                    d = one

            except Exception as e:
                logging.exception(f'{self.__str__()}.get_unusual_call_vol().get_ticker_from_column() Error',
                                  exc_info=traceback.format_exc()
                                       )
            finally:
                return d

        def get_stock_price_data_from_column(td_tag):
            text = td_tag.text
            if '+' in text:
                text = text.split('+')
                pct = text[1]
                dllr = text[0]

            elif '-' in text:
                text = text.split('-')
                pct = text[1]
                dllr = text[0]

            else:
                return False

            d = {
                'curStockPrice': float(dllr[1:]),
                'stockPercentGain': float(pct[:-1])
            }
            return d

        def get_todays_vol_data_from_column(td_tag):
            d = False
            try:
                d = td_tag.text

                d = d.split(',')
                d = ''.join(d)
                d = {
                    'todaysOptionVolume': int(d),
                }


            except Exception:
                logging.exception(f'{self.__str__()}.get_unusual_call_vol().get_vol_data() Error',
                                  exc_info=traceback.format_exc()
                                       )
            finally:
                return d

        def get_avg_vol_data_from_column(td_tag):
            d = False
            try:
                avg_vol = td_tag.text
                avg_vol = avg_vol.split(',')
                avg_vol = ''.join(avg_vol)
                d = {'avgOptionVolume': int(avg_vol)}
            except Exception:
                logging.exception(f'{self.__str__()}.get_unusual_call_vol().get_avg_vol_data() Error',
                                  exc_info=traceback.format_exc()
                                       )
            finally:
                return d

        def get_rel_increase_data_from_column(td_tag):
            d = False
            try:
                r_inc = td_tag.text
                r_inc = r_inc[:-1]
                d = {'relativeVolumeIncrease': float(r_inc)}
            except Exception:
                logging.exception(f'{self.__str__()}.get_unusual_call_vol().get_rel_increase_data() Error',
                                  exc_info=traceback.format_exc()
                                       )
            finally:
                return d

        def get_avg_stock_volume(td_tag):
            d = False
            try:
                raw = td_tag.text

                # If the volume has a comma in it just remove comma and convert to int()
                if ',' in raw:
                    raw = raw.split(',')
                    raw = int(''.join(raw))

                # If it is in format 12.3 million, split it and add some zeros
                elif '.' in raw:

                    raw = raw.split('.')
                    second_split = raw[1].split(' ')
                    del raw[1]
                    raw.append(second_split[0])

                    # Check that it has 2 decimal places, if not add a zero
                    if len(raw[1]) != 2:
                        dec = raw[1] + '0'
                    else:
                        dec = raw[1]

                    # Delete the original incase it did not have 2 dec places, and the 'million'
                    del raw[1:]

                    # Add zeros to end of dec
                    dec = dec + '0000'
                    raw.append(dec)
                    raw = int(''.join(raw))

                else:
                    raw = int(raw)

                d = {'avgStockVolume': raw}

            except Exception:
                logging.exception(f'{self.__str__()}.get_unusual_call_vol().get_avg_stock_volume Error',
                                  exc_info=traceback.format_exc()
                                       )
            finally:
                return d

        def get_cause_of_spike(td_tag):

            d = False
            try:
                tags = td_tag.find_all('a')

                cause_data = list()

                for link in tags:
                    cause_data.append(link.text)

                d = {
                    'catalystEvents': cause_data
                }
            except Exception:
                logging.exception(f'{self.__str__()}.get_unusual_call_vol().get_cause_of_spike() Error',
                                  exc_info=traceback.format_exc()
                                       )
            finally:
                return d

        if call:
            soup = self.make_soup(self._marketbeat_unusual_calls_vol_url)
        elif put:
            soup = self.make_soup(self._marketbeat_unusual_puts_vol_url)
        else:
            return False
        # Table tag containing rows
        table = soup.find('tbody')

        # Row data
        rows = table.find_all('tr')

        # Loop through and build dataset
        df = list()
        for data_row in rows:

            data = dict()
            cols = data_row.find_all('td')
            if len(cols) != 7:
                continue
            # Get the ticker from the dataset
            tick_info = cols[0]
            ticker = get_ticker_from_column(tick_info)
            if ticker is False:
                continue
            data['ticker'] = ticker

            # Get cur stock price and stock % change
            stock_price_data = cols[1]
            stock_price_data = get_stock_price_data_from_column(stock_price_data)
            if stock_price_data is False:
                continue
            data.update(stock_price_data)

            # Get todays volume data
            vol_data = cols[2]
            vol_data = get_todays_vol_data_from_column(vol_data)
            if vol_data is False:
                continue
            data.update(vol_data)

            # Get avg volume data
            avg_vol_data = cols[3]
            a_vol_data = get_avg_vol_data_from_column(avg_vol_data)
            if a_vol_data is False:
                continue
            data.update(a_vol_data)

            # Relative % increase of op volume
            rel_incr = cols[4]
            rel_incr = get_rel_increase_data_from_column(rel_incr)
            if rel_incr is False:
                continue
            data.update(rel_incr)

            # Stock avg vol
            a_stk_vol = cols[5]
            a_stk_vol = get_avg_stock_volume(a_stk_vol)
            if a_stk_vol is False:
                continue
            data.update(a_stk_vol)

            # Get cause of vol spike
            cause = cols[6]
            cause = get_cause_of_spike(cause)
            if cause is False:
                continue
            data.update(cause)

            df.append(data)

        return df

    def get_unusual_option_volume_marketbeat(self, only_calls=False, only_puts=False) -> list or bool:
        """Returns unusual option volume from www.marketbeat.com.

        Can specify either only calls / only puts
        """
        data = False

        try:
            # Calls
            if only_calls:
                call_data = self._get_marketbeat_unusual_option_volume(call=True)
                data = call_data

            # Puts
            elif only_puts:
                put_data = self._get_marketbeat_unusual_option_volume(put=True)
                data = put_data

            # Default both
            else:

                call_data = self._get_marketbeat_unusual_option_volume(call=True)
                put_data = self._get_marketbeat_unusual_option_volume(put=True)

                full_df = call_data.append(put_data)
                full_df.sort_values(by='todaysOptionVolume', inplace=True)
                data = full_df
        except Exception:
            logging.exception(f'{self.__str__()}.get_unusual_option_volume() Unknown Error',
                              exc_info=traceback.format_exc())

        finally:
            return data

    def get_futures_data_yf(self) -> dict:
        """
        Return a dict type data set containing the futures and commodities data.
        :return: dict
        """
        soup = self.make_soup(self._yf_futures_url)
        # This is the containing table tag
        granddad_data_table = soup.find('section', {'data-test': "yfin-list-table"})
        # Each '<tr>' tag represents the entire data row for each index future
        data_rows = granddad_data_table.find_all('tr')
        data_actual = dict()
        temp_data = list()
        # headers for the data rows
        headers1 = ['symbol', 'name', 'currentPrice', 'marketTime',
                    'changeDollar', 'changePercent', 'volume', 'openInterest', '']
        # strip values from data row
        for row in data_rows[1:]:
            temp_value_data = list()
            for value in row:
                temp_value_data.append(value.text)
            temp_data.append(temp_value_data)
        # Add Data to data actual
        for row in temp_data:
            temp_row_data = dict()
            for n, i in enumerate(row):
                temp_row_data[headers1[n]] = i
            data_actual[temp_row_data['symbol']] = temp_row_data
        # Remove un-needed data
        for key in data_actual.keys():
            # data_actual.pop('Symbol')
            data_actual[key].pop('marketTime')
            data_actual[key].pop('')
            data_actual[key]['currentPrice'] = convert_data_strp_number(data_actual[key]['currentPrice'])
            data_actual[key]['changeDollar'] = convert_data_strp_number(data_actual[key]['changeDollar'])
            data_actual[key]['changePercent'] = convert_data_strp_number(data_actual[key]['changePercent'])
            data_actual[key]['volume'] = [
                data_actual[key]['volume'],
                convert_data_strp_number(data_actual[key]['volume'])]
        return data_actual

    def get_trending_tickers_yf(self) -> dict:
        """
        Return a dict type data set containing the 'Trending Tickers' (most searched) of the day
        :return: Dict type data set
        """
        soup = self.make_soup(self._yf_trending_tickers_url)
        # Master table tag
        grand_tag = soup.find('section', {'id': "yfin-list"})
        # Tags containing the row data
        row_data = grand_tag.table.find_all('tr')
        # Strip and convert data  and populate final data set
        data_actual = dict()
        headers = 'symbol,name,lastPrice,marketTime,changeDollar,changePercent,' \
                  'volume,avgVolumeThreeMonth,marketCap,_,_,_'.split(',')
        for row in row_data[1:]:
            stage_one_row_data = dict()
            for n, value in enumerate(row):
                stage_one_row_data[headers[n]] = value.text
            stage_one_row_data.pop('_')
            stage_one_row_data['lastPrice'] = convert_data_strp_number(stage_one_row_data['lastPrice'])
            stage_one_row_data['changeDollar'] = convert_data_strp_number(stage_one_row_data['changeDollar'])
            stage_one_row_data['changePercent'] = convert_data_strp_number(stage_one_row_data['changePercent'])
            stage_one_row_data['avgVolumeThreeMonth'] = convert_alphanumeric_volume(
                stage_one_row_data['avgVolumeThreeMonth'])
            data_actual[stage_one_row_data['symbol']] = stage_one_row_data
            stage_one_row_data['volume'] = convert_alphanumeric_volume(stage_one_row_data['volume'])
        keys_to_delete = list()
        for tick in data_actual.keys():
            if '-' in tick:
                keys_to_delete.append(tick)
            if '=' in tick:
                keys_to_delete.append(tick)
            if '^' in tick:
                keys_to_delete.append(tick)
            if '.' in tick:
                keys_to_delete.append(tick)

            del data_actual[tick]['marketCap']

        for k in keys_to_delete:
            del data_actual[k]
        return data_actual

    def get_top_volume_tickers_yf(self) -> dict:
        """
        Returns a dict type data set of the 'Most Traded Stocks' of the day
        :return: Dict type data set
        """

        soup = self.make_soup(self._yf_most_active_url)
        grand_tag = soup.find('div', {'id': "scr-res-table"})

        row_data = grand_tag.table.find_all('tr')

        data_actual = dict()
        headers = 'symbol,name,lastPrice,changeDollar,changePercent,volume,avgVolumeThreeMonth,marketCap,peRatioTTM,_'.split(
            ',')

        for row in row_data:
            stage_one_row_data = dict()
            for n, value in enumerate(row):
                stage_one_row_data[headers[n]] = value.text
            stage_one_row_data.pop('_')
            stage_one_row_data['lastPrice'] = convert_data_strp_number(stage_one_row_data['lastPrice'])
            stage_one_row_data['changeDollar'] = convert_data_strp_number(stage_one_row_data['changeDollar'])
            stage_one_row_data['changePercent'] = convert_data_strp_number(stage_one_row_data['changePercent'])
            stage_one_row_data['peRatioTTM'] = convert_data_strp_number(stage_one_row_data['peRatioTTM'])
            stage_one_row_data['volume'] = convert_alphanumeric_volume(stage_one_row_data['volume'])
            stage_one_row_data['avgVolumeThreeMonth'] = convert_alphanumeric_volume(
                stage_one_row_data['avgVolumeThreeMonth'])

            data_actual[stage_one_row_data['symbol']] = stage_one_row_data

        # Extra row snuck in there
        del data_actual['Symbol']
        return data_actual

    def get_top_gaining_tickers_yf(self) -> dict:
        """
        Returns a dict type data set containing in order the data for the 'Top Gaining Stocks'
        :return: Dict type data set
        """
        soup = self.make_soup(self._yf_top_gainers_url)
        grand_tag = soup.find('div', {'id': "scr-res-table"})

        row_data = grand_tag.table.find_all('tr')

        data_actual = dict()
        headers = 'symbol,name,lastPrice,changeDollar,changePercent,volume,avgVolumeThreeMonth,marketCap,peRatioTTM,_'.split(
            ',')

        for row in row_data[1:]:
            stage_one_row_data = dict()
            for n, value in enumerate(row):
                stage_one_row_data[headers[n]] = value.text
            stage_one_row_data.pop('_')
            stage_one_row_data['lastPrice'] = convert_data_strp_number(stage_one_row_data['lastPrice'])
            stage_one_row_data['changeDollar'] = convert_data_strp_number(stage_one_row_data['changeDollar'])
            stage_one_row_data['changePercent'] = convert_data_strp_number(stage_one_row_data['changePercent'])
            stage_one_row_data['peRatioTTM'] = convert_data_strp_number(stage_one_row_data['peRatioTTM'])
            stage_one_row_data['volume'] = convert_alphanumeric_volume(stage_one_row_data['volume'])
            stage_one_row_data['avgVolumeThreeMonth'] = convert_alphanumeric_volume(
                stage_one_row_data['avgVolumeThreeMonth'])

            data_actual[stage_one_row_data['symbol']] = stage_one_row_data
        return data_actual

    def get_top_losing_tickers_yf(self) -> dict:
        """
        Returns a Dict type data set containing the 'Top Losers' of the day
        :return:
        """
        soup = self.make_soup(self._yf_top_losers_url)
        grand_tag = soup.find('div', {'id': "scr-res-table"})

        row_data = grand_tag.table.find_all('tr')

        data_actual = dict()
        headers = 'symbol,name,lastPrice,changeDollar,changePercent,volume,avgVolumeThreeMonth,marketCap,peRatioTTM,_'.split(
            ',')

        for row in row_data[1:]:
            stage_one_row_data = dict()
            for n, value in enumerate(row):
                stage_one_row_data[headers[n]] = value.text
            stage_one_row_data.pop('_')
            stage_one_row_data['lastPrice'] = convert_data_strp_number(stage_one_row_data['lastPrice'])
            stage_one_row_data['changeDollar'] = convert_data_strp_number(stage_one_row_data['changeDollar'])
            stage_one_row_data['changePercent'] = convert_data_strp_number(stage_one_row_data['changePercent'])
            stage_one_row_data['peRatioTTM'] = convert_data_strp_number(stage_one_row_data['peRatioTTM'])
            stage_one_row_data['volume'] = convert_alphanumeric_volume(stage_one_row_data['volume'])
            stage_one_row_data['avgVolumeThreeMonth'] = convert_alphanumeric_volume(
                stage_one_row_data['avgVolumeThreeMonth'])

            data_actual[stage_one_row_data['symbol']] = stage_one_row_data
        return data_actual

    def get_put_call_ratio_cboe(self) -> dict:
        """Retrieves put/call ratio data from cboe.com"""
        # URL for website that tracks Put/Call ratio
        # Note that a P/C ratio .7 or lower is considered a bull market, and vice versa
        # Note that there will always be more puts, ppl use puts to protect their stocks from sudden dips
        url = 'https://markets.cboe.com/us/options/market_statistics/daily/'
        soup = self.make_soup(url)

        ratios_table = soup.find('div', {"id": "daily-market-stats-data"})

        data_tags = ratios_table.find_all('tr')

        valuable_tags = data_tags[1:9]

        temp_cleanup_list = list()
        stage_two_data = dict()
        for n, i in enumerate(valuable_tags):
            temp_cleanup_list.append(i.text)

        data_sets = list()
        for i in temp_cleanup_list:
            i = i.split('\n')
            data_sets.append(i)

        for i in data_sets:
            stage_two_data[i[1]] = i[2]

        # Rename tags and convert values to workable data set

        data = dict()
        data['totalPutCallRatio'] = safe_float_conversion(stage_two_data['TOTAL PUT/CALL RATIO'])
        data['indexPutCallRatio'] = safe_float_conversion(stage_two_data['INDEX PUT/CALL RATIO'])
        data['vixPutCallRatio'] = safe_float_conversion(
            stage_two_data['cboe volatility index (vix) put/call ratio'.upper()])
        data['majorExchangePutCallRatio'] = safe_float_conversion(
            stage_two_data['exchange traded products put/call ratio'.upper()])

        return data

    def get_next_cpi_report_timestamp(self) -> float:
        """
        Returns the timestamps for all the dates of the upcoming CPI Reports.

        Timestamps are all set a 6am that morning, the report comes out at 8:30 i think.
        """

        url = 'https://www.bls.gov/schedule/news_release/cpi.htm'

        soup = self.make_soup(url)

        table = soup.tbody
        rows = table.find_all('tr')

        # Get date strings from rows
        row_data = list()
        for r in rows:
            data_points = r.find_all('td')
            row_data.append(data_points)
        date_strings = [rd[1].text for rd in row_data]

        months = 'Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec'.split(',')
        # Clean date strings up and get timestamps
        cpi_ts_list = list()
        for ds in date_strings:

            ds = ds.split()
            raw_mon = ds[0]
            raw_day = ds[1]
            raw_year = ds[2]
            if '.' in raw_mon:
                raw_mon = raw_mon[:-1]
            if ',' in raw_day:
                raw_day = raw_day[:-1]

            mon_number = months.index(raw_mon) + 1
            day_number = int(raw_day)
            year_number = int(raw_year)

            ds = f'{day_number}-{mon_number}-{year_number}'

            ts = time.mktime(datetime.datetime.strptime(ds, '%d-%m-%Y').timetuple())

            # Add 6 hours so that it shows the report is that day from 6pm on
            ts = ts + ((60 * 60) * 8)

            # If the report has already happened, don't add it to list
            if time.time() > ts:
                continue
            else:
                cpi_ts_list.append(ts)

        return min(cpi_ts_list)

    def get_next_retail_sales_report_timestamp(self) -> int:
        """
        Returns the approx timestamp (that morning) of the next retail sales report
        """

        url = 'https://tradingeconomics.com/united-states/retail-sales'
        soup = self.make_soup(url)

        table = soup.find('table', {'id': 'calendar'})

        tags = table.find_all('tr')

        # Delete the headers
        del tags[0]

        date_strings = list()
        for n, t in enumerate(tags):
            data = t.find_all('td')
            date_strings.append(data[0].text)

        report_ts_list = list()
        for d in date_strings:

            ts = time.mktime(datetime.datetime.strptime(d, '%Y-%m-%d').timetuple())

            if time.time() < ts:
                report_ts_list.append(ts)

        if len(report_ts_list) > 0:
            return min(report_ts_list)
        else:
            # Return this signaling that it was a failed report grab
            print(f'{self.__str__()}.get_next_retail_sales_report_timestamp()')
            print('# # ERROR NO UPCOMING RETAIL SALES REPORT FOUND # #')
            return 999999999999999

    def get_crypto_data_yf(self) -> dict:
        """
        Returns a Dict type data set containing information on CryptoCurrency
        :return: 'Dict'
        """
        soup = self.make_soup(self._yf_crypto_data_url)

        # Table containing all the data rows.
        grand_dad_table = soup.find('div', {'id': "scr-res-table"})

        # Data rows
        rows = grand_dad_table.tbody.find_all('tr')
        data_actual = dict()
        headers = 'symbol,name,lastPrice,changeDollar,changePercent,marketCap,volumeSinceMidnight,' \
                  'volumeLast24hr,Blahblah,volumeInCirculation,52wk,1daychart'.split(',')

        # Populate the inner data sets, and Convert the workable numbers
        for r in rows:
            stage_one_row_data = dict()
            for n, val in enumerate(r):
                val = val.text
                stage_one_row_data[headers[n]] = val
            stage_one_row_data['lastPrice'] = convert_data_strp_number(stage_one_row_data['lastPrice'])
            stage_one_row_data['changeDollar'] = convert_data_strp_number(stage_one_row_data['changeDollar'])
            stage_one_row_data['changePercent'] = convert_data_strp_number(stage_one_row_data['changePercent'])
            stage_one_row_data['marketCap'] = convert_alphanumeric_volume(stage_one_row_data['marketCap'])
            stage_one_row_data['volumeSinceMidnight'] = convert_alphanumeric_volume(
                stage_one_row_data['volumeSinceMidnight'])
            stage_one_row_data['volumeLast24hr'] = convert_alphanumeric_volume(stage_one_row_data['volumeLast24hr'])
            stage_one_row_data['volumeInCirculation'] = convert_alphanumeric_volume(
                stage_one_row_data['volumeInCirculation'])

            # Remove un-needed Data
            del stage_one_row_data['52wk']
            del stage_one_row_data['1daychart']
            del stage_one_row_data['Blahblah']
            data_actual[stage_one_row_data['symbol']] = stage_one_row_data
        self.crypto_data = data_actual
        return data_actual

    def get_index_data_yf(self) -> dict:
        """
        Returns a dict type data set containing the 'Index' data for the day
        :return: dict
        """
        soup = self.make_soup(self._yf_index_data_url)
        # This is the containing table tag
        granddad_data_table = soup.find('section', {'data-test': "yfin-list-table"})
        # Each '<tr>' tag represents the entire data row for each index future
        data_rows = granddad_data_table.find_all('tr')
        data_actual = dict()
        temp_data = list()
        # headers for the data rows
        headers = ['symbol', 'name', 'lastPrice', 'changeDollar', 'changePercent', 'volume']
        # strip values from data row
        for row in data_rows:
            temp_value_data = list()
            for value in row:
                temp_value_data.append(value.text)
            temp_data.append(temp_value_data)
        # add values to to final 'data_actual' dict
        for row in temp_data[1:]:
            row_data = dict()
            for n, i in enumerate(headers):
                row_data[i] = row[n]
            data_actual[row_data['symbol']] = row_data
        # clean the data
        for key in data_actual.keys():
            data_actual[key]['lastPrice'] = convert_data_strp_number(data_actual[key]['lastPrice'])
            data_actual[key]['changeDollar'] = convert_data_strp_number(data_actual[key]['changeDollar'])
            data_actual[key]['changePercent'] = convert_data_strp_number(data_actual[key]['changePercent'])
            data_actual[key]['volume'] = convert_alphanumeric_volume(data_actual[key]['volume'])

        return data_actual

    def get_vix_data(self) -> dict:
        base = self.get_index_data_yf()
        return base['^VIX']

    def get_analysts_upgrades_downgrades_marketwatch(self):

        def add_data_to_dict(n_check, n_val, key, val, storage_dict):

            if n_val == n_check:
                storage_dict[key] = val.string

        url = "https://www.marketwatch.com/tools/upgrades-downgrades"

        soup = self.make_soup(url)

        table = soup.find('table')
        raw_data_rows = table.find_all('tr')

        row_text_data = list()

        for tr in raw_data_rows[1:]:

            #
            # 1 11/16/2021
            # 2
            #
            # 3 HHR
            # 4
            #
            # 5 HeadHunter Group
            # 6
            #
            # 7 Maintains
            # 8
            #
            # 9 Credit Suisse
            # 10
            #
            # 11 None
            # 12

            data = dict()
            for n, td in enumerate(tr):
                add_data_to_dict(1, n, "date", td.string, data)
                add_data_to_dict(3, n, "ticker", td.string, data)
                add_data_to_dict(5, n, "company", td.string, data)
                add_data_to_dict(7, n, "rating", td.string, data)
                add_data_to_dict(9, n, "analyst", td.string, data)

            row_text_data.append(data)

        print(row_text_data)


class WatchlistAndSymbolsHelper:

    def __init__(self):
        self.session_firefox = requests.Session()
        self.session_firefox.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36"
        })
        # One month
        self.barchart_stats_update_interval = (((60 * 60) * 24) * 3)

    def scrape_yf_watchlists(self, url):
        """Use the linkes from the "Watchlist" section of finance.yahoo.com to build watchlists"""
        soup = BeautifulSoup(self.session_firefox.get(url).content, "lxml")
        table = soup.find("table", {"class": "cwl-symbols W(100%)"})

        raw_rows = table.find_all("tr")
        data = list()
        for row in raw_rows:
            td_tags = row.find_all("td")
            if len(td_tags) < 1:
                continue
            sym = td_tags[0].text
            if isinstance(sym, str) is False:
                continue
            if sym.isupper():
                data.append(sym)

        return data

    def get_watchlist_yf_most_watched(self):
        return self.scrape_yf_watchlists("https://finance.yahoo.com/u/yahoo-finance/watchlists/most-watched")

    def get_watchlist_yf_biggest_52wk_gains(self):
        return self.scrape_yf_watchlists("https://finance.yahoo.com/u/yahoo-finance/watchlists/fiftytwo-wk-gain")

    def get_watchlist_yf_recent_52wk_highs(self):
        return self.scrape_yf_watchlists("https://finance.yahoo.com/u/yahoo-finance/watchlists/fiftytwo-wk-high")

    def get_watchlist_yf_biggest_52wk_losses(self):
        return self.scrape_yf_watchlists("https://finance.yahoo.com/u/yahoo-finance/watchlists/fiftytwo-wk-loss")

    def get_watchlist_yf_most_shorted_stocks(self):
        return self.scrape_yf_watchlists(
            "https://finance.yahoo.com/u/yahoo-finance/watchlists/stocks-with-the-highest-short-interest")

    def get_watchlist_yf_most_newly_added(self):
        return self.scrape_yf_watchlists(
            "https://finance.yahoo.com/u/yahoo-finance/watchlists/most-added"
        )

    def get_watchlist_yf_trending_tickers(self):
        soup_base = BeautifulSoup(requests.get("https://finance.yahoo.com/trending-tickers").content, 'lxml')
        table = soup_base.find("table", {"class": "W(100%)"})

        ticks = list()
        for row in table.find_all("tr"):
            data_points = row.find_all("td")
            if len(data_points) > 0:
                if "-" not in data_points[0].text and "." not in data_points[0].text:
                    ticks.append(data_points[0].text)

        return ticks


if __name__ == '__main__':
    pass
