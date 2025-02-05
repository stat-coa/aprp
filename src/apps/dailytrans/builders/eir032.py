import datetime
import json
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Dict
from urllib.parse import urlparse, parse_qs

import pandas as pd
from bs4 import BeautifulSoup
from bs4.element import ResultSet
from requests import Response, post

from apps.dailytrans.models import DailyTran
from .abstract import AbstractApi
from .utils import date_transfer


class Api(AbstractApi):
    # Settings
    API_NAME = 'eir032'
    ZFILL = True
    ROC_FORMAT = True
    SEP = ''

    # Filters
    START_DATE_FILTER = 'StartDate=%s'
    END_DATE_FILTER = 'EndDate=%s'
    SOURCE_FILTER = 'MarketName=%s'
    CODE_FILTER = 'TypeNo=%s'
    NAME_FILTER = 'TypeName=%s'

    def __init__(self, model, config_code, type_id, logger_type_code=None):
        super(Api, self).__init__(model=model, config_code=config_code, type_id=type_id,
                                  logger='aprp', logger_type_code=logger_type_code)

    def hook(self, dic):

        for key, value in dic.items():
            if isinstance(value, str):
                dic[key] = value.strip()

        product_code = dic.get('品種代碼')
        product = self.PRODUCT_QS.filter(code=product_code).first()
        source_name = dic.get('市場名稱')
        source = self.SOURCE_QS.filter_by_name(source_name).first()
        if product and source:
            tran = DailyTran(
                product=product,
                source=source,
                up_price=dic.get('上價'),
                mid_price=dic.get('中價'),
                low_price=dic.get('下價'),
                avg_price=dic.get('平均價'),
                volume=dic.get('交易量'),
                date=date_transfer(sep=self.SEP, string=dic.get('交易日期'), roc_format=self.ROC_FORMAT)
            )
            return tran
        else:
            if not product and dic.get('魚貨名稱') != "休市":
                self.LOGGER.warning('Cannot Match Product: "%s" In Dictionary %s'
                                    % (product_code, dic), extra=self.LOGGER_EXTRA)
            if not source:
                self.LOGGER.warning('Cannot Match Source: "%s" In Dictionary %s'
                                    % (source_name, dic), extra=self.LOGGER_EXTRA)
            return dic

    def request(self, start_date=None, end_date=None, source=None, code=None, name=None):
        url = self.API_URL
        if start_date:
            if not isinstance(start_date, datetime.date):
                raise NotImplementedError

            start_date_str = date_transfer(sep=self.SEP,
                                           date=start_date,
                                           roc_format=self.ROC_FORMAT,
                                           zfill=self.ZFILL)

            url = '&'.join((url, self.START_DATE_FILTER % start_date_str))

        if end_date:
            if not isinstance(end_date, datetime.date):
                raise NotImplementedError

            end_date_str = date_transfer(sep=self.SEP,
                                         date=end_date,
                                         roc_format=self.ROC_FORMAT,
                                         zfill=self.ZFILL)

            url = '&'.join((url, self.END_DATE_FILTER % end_date_str))

        if start_date and end_date:
            if start_date > end_date:
                raise AttributeError

        if source:
            url = '&'.join((url, self.SOURCE_FILTER % source))

        if code:
            url = '&'.join((url, self.CODE_FILTER % code))

        if name:
            url = '&'.join((url, self.NAME_FILTER % name))

        return self.get(url)

    def load(self, response):
        data = []
        if response.text:
            try:
                data = json.loads(response.text)
            except Exception as e:
                self.LOGGER.exception(f'exception: {e}, response: {response.text}', extra=self.LOGGER_EXTRA)

        data = pd.DataFrame(data,
                            columns=['上價', '中價', '下價', '平均價', '交易量', '交易日期', '品種代碼', '市場名稱',
                                     '魚貨名稱'])
        data = data[data['品種代碼'].isin(self.target_items)]
        try:
            if not data.empty:
                self._access_data_from_api(data)
        except Exception as e:
            self.LOGGER.exception(f'exception: {e}, response: {response.text}', extra=self.LOGGER_EXTRA)

    def _access_data_from_api(self, data: pd.DataFrame):
        data_merge = self._compare_data_from_api_and_db(data)
        condition = ((data_merge['avg_price_x'] != data_merge['avg_price_y']) | (
                data_merge['up_price_x'] != data_merge['up_price_y']) | (
                             data_merge['low_price_x'] != data_merge['low_price_y']) | (
                             data_merge['mid_price_x'] != data_merge['mid_price_y']) | (
                             data_merge['volume_x'] != data_merge['volume_y']))
        if not data_merge[condition].empty:
            for _, value in data_merge[condition].fillna('').iterrows():
                try:
                    existed_tran = DailyTran.objects.get(id=int(value['id'] or 0))
                    if value['avg_price_x']:
                        self._update_data(value, existed_tran)
                    else:
                        existed_tran.delete()
                        self.LOGGER.warning(msg=f"The DailyTran data of the product: {value['product__code']} "
                                                f"on {value['date'].strftime('%Y-%m-%d')} "
                                                f"from source: {value['source__name']} with\n"
                                                f"up_price: {value['up_price_y']}\n"
                                                f"mid_price: {value['mid_price_y']}\n"
                                                f"low_price: {value['low_price_y']}\n"
                                                f"avg_price: {value['avg_price_y']}\n"
                                                f"volume: {value['volume_y']} has been deleted.")
                except Exception as e:
                    self._save_new_data(value)

    def _compare_data_from_api_and_db(self, data: pd.DataFrame):
        columns = {
            '上價': 'up_price',
            '中價': 'mid_price',
            '下價': 'low_price',
            '平均價': 'avg_price',
            '交易量': 'volume',
            '品種代碼': 'product__code',
            '市場名稱': 'source__name',
            '交易日期': 'date',
            '魚貨名稱': 'product__name'
        }
        data.rename(columns=columns, inplace=True)
        data['date'] = data['date'].apply(lambda x: datetime.datetime.strptime(
            f'{int(x) // 10000 + 1911}-{(int(x) // 100) % 100:02d}-{int(x) % 100:02d}', '%Y-%m-%d').date())
        data['source__name'] = data['source__name'].str.replace('台', '臺')
        data['product__code'] = data['product__code'].astype(str)

        data_db = DailyTran.objects.filter(date=data['date'].iloc[0], product__type=1, product__config=self.CONFIG)
        data_db = pd.DataFrame(list(data_db.values('id', 'product__id', 'product__code', 'up_price', 'mid_price',
                                                   'low_price', 'avg_price', 'volume', 'date', 'source__name'))) \
            if data_db else pd.DataFrame(columns=['id', 'product__id', 'product__code', 'up_price', 'mid_price',
                                                  'low_price', 'avg_price', 'volume', 'date', 'source__name'])

        return data.merge(data_db, on=['date', 'product__code', 'source__name'], how='outer')

    def _update_data(self, value, existed_tran):
        existed_tran.up_price = value['up_price_x']
        existed_tran.mid_price = value['mid_price_x']
        existed_tran.low_price = value['low_price_x']
        existed_tran.avg_price = value['avg_price_x']
        existed_tran.volume = value['volume_x']
        existed_tran.save()
        self.LOGGER.info(
            msg=f"The data of the product: {value['product__code']} on"
                f" {value['date'].strftime('%Y-%m-%d')} has been updated.")

    def _save_new_data(self, value):
        products = self.MODEL.objects.filter(code=value['product__code'])
        source = self.SOURCE_QS.filter_by_name(value['source__name']).first()
        new_trans = [DailyTran(
            product=product,
            source=source,
            up_price=value['up_price_x'],
            mid_price=value['mid_price_x'],
            low_price=value['low_price_x'],
            avg_price=value['avg_price_x'],
            volume=value['volume_x'],
            date=value['date']
        ) for product in products]
        DailyTran.objects.bulk_create(new_trans)


class HTMLParser:
    """
    此 class 用於解析 HTML 內容，並取出需要的資料所設計。(使用 BeautifulSoup4)
    """

    TABLE_ID = 'ltable'

    def __init__(self, response: Response, api: 'ScrapperApi'):
        """
        Args:
            response: 請求後的 response 物件
            api: 指向回 `ScrapperApi` 物件參照，方便取得相關資訊
        """

        self.__data: List[OrderedDict] = []

        self.response = response
        self.soup = BeautifulSoup(response.text, 'html.parser')
        self.api = api

    @property
    def data(self):
        return self.__data

    @data.setter
    def data(self, value: Optional[OrderedDict]):
        if value:
            self.__data.append(value)

    @property
    def params(self)-> Dict[str, List[str]]:
        return parse_qs(urlparse(self.response.request.url).query)

    @property
    def source_code(self) -> str:
        return self.params.get('mid')[0]

    @property
    def table(self):
        """
        目標 table 結構大致如下:

        <table id="ltable" class="bk_white">
          <thead>...</thead>
          <tbody>...</tbody>
        </table>
        """

        return self.soup.find('table', {'id': self.TABLE_ID})

    @property
    def tbody(self):
        """
        目標 tbody 結構大致如下:

        <table id="ltable" class="bk_white">
          <thead>...</thead>

          主要抓取目標
          <tbody>
            <tr>...</tr>
            <tr>...</tr>
          </tbody>
        </table>
        """

        return self.table.find('tbody') if self.table else None

    @property
    def all_th(self) -> Optional[ResultSet]:
        """
        目標 thead 結構大致如下，主要用來抓取欄位名稱:

        <thead>
            <tr>
              <th>品種<br>代碼</th>
              <th>魚貨名稱</th>
              <th>上價<br>(元/公斤)</th>
              <th>中價<br>(元/公斤)</th>
              <th>下價<br>(元/公斤)</th>
              <th>交易量<br>(公斤)</th>
              <th>交易量漲跌幅+(-)%</th>
              <th>平均價<br>(元/公斤)</th>
              <th>平均價漲跌幅+(-)%</th>
            </tr>
        </thead>
        """

        return self.table.find_all('th') if self.table else None

    @property
    def all_tr(self) -> Optional[ResultSet]:
        """
        目標 tr 結構大致如下，tr 內容為每一筆資料:

        <table id="ltable" class="bk_white">
          <thead>...</thead>
          <tbody>

            主要抓取目標
            <tr>...</tr>
            <tr>...</tr>
          </tbody>
        </table>
        """

        return self.tbody.find_all('tr') if self.tbody else None
    
    @property
    def all_td_list(self) -> Optional[List[ResultSet]]:
        """
        目標 td 結構大致如下:

        <table id="ltable" class="bk_white">
          <thead>...</thead>
          <tbody>
            <tr>
              主要抓取目標(所有 td)
              <td>1071</td>
              <td>金目鱸</td>
              <td>144.2</td>
              <td>101.7</td>
              <td>77.5</td>
              <td>3,969.3</td>
              <td>	</td>
              <td>105.4</td>
              <td> </td>
            </tr>
            <tr>...</tr>
          </tbody>
        </table>
        """

        return [tr.find_all('td') for tr in self.all_tr] if self.all_tr else None

    @property
    def headers(self) -> List[str]:
        """
        將所有 <th> 標籤的文字(欄位名稱)取出，並去除換行符號
        """

        return [th.text.replace('\n', '') for th in self.all_th] if self.all_th else []

    @staticmethod
    def convert_to_float(value: Optional[str]) -> float:
        return float(value.replace(',', '').strip())

    def _extract_relevant_data(self, row_data: dict) -> Optional[OrderedDict]:
        """
        只抓取需要的欄位資料，並轉換成 dict 格式

        Args:
            row_data: 代表每一筆資料的 dict(已從 HTML 中取出的 raw data)
        """

        try:
            return OrderedDict({
                "交易日期": self.api.date_str,
                "品種代碼": int(row_data.get("品種代碼")),
                "魚貨名稱": row_data.get("魚貨名稱"),
                "市場名稱": self.api.SOURCES.get(self.source_code),
                "上價": self.convert_to_float(row_data.get("上價(元/公斤)")),
                "中價": self.convert_to_float(row_data.get("中價(元/公斤)")),
                "下價": self.convert_to_float(row_data.get("下價(元/公斤)")),
                "交易量": self.convert_to_float(row_data.get("交易量(公斤)")),
                "平均價": self.convert_to_float(row_data.get("平均價(元/公斤)")),
            })
        except (ValueError, TypeError, AttributeError):
            self.api.LOGGER.exception(
                f'ValueError: {row_data}, source: {self.api.SOURCES.get(self.source_code)}',
                extra=self.api.LOGGER_EXTRA
            )

            return None

    def parse_table(self) -> Optional[List[OrderedDict]]:
        """
        將指定的 HTML table 解析後，取出需要的資料
        """

        # 若沒有找到資料則直接返回
        if not self.all_tr:
            self.api.LOGGER.warning(
                f'No Table Found, source: {self.api.SOURCES.get(self.source_code)}', extra=self.api.LOGGER_EXTRA
            )

            return

        for td_list in self.all_td_list:
            # 組成格式為 {header: value} 的 dict
            # e.g. {'品種代碼': '1071', '魚貨名稱': '金目鱸', '上價': '144.2', ...}
            row_data = {
                header: td_list[i].text.strip().replace('\xa0', '')
                for i, header in enumerate(self.headers)
            }
            self.data = self._extract_relevant_data(row_data)

        return self.data


class ScrapperApi(Api):
    """
    `Api` class 的爬蟲版本，直接爬取 '漁產批發市場交易行情站' 網頁上的資料
    因 API 較久以前的資料有異動時不會更新(行情站上的資料最正確)
    """

    MAX_RETRY = 3
    SLEEP_TIME = 3
    URL = 'https://efish.fa.gov.tw/efish/statistics/daysinglemarketmultifish.htm'
    HEADERS = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko)'
                      'Chrome/132.0.0.0 Safari/537.36'
    }

    # 只抓取消費地市場
    SOURCES = OrderedDict({
        'F109': '台北',
        'F241': '三重',
        'F300': '新竹',
        'F330': '桃園',
        'F360': '苗栗',
        'F400': '台中',
        'F500': '彰化',
        'F513': '埔心',
        'F600': '嘉義',
        'F630': '斗南',
        'F722': '佳里',
        'F730': '新營',
        'F820': '岡山',
    })

    def __init__(self, model, config_code, type_id, logger_type_code=None):
        super(ScrapperApi, self).__init__(
            model=model, config_code=config_code, type_id=type_id, logger_type_code=logger_type_code
        )

        self.__query_date: Optional[datetime.date] = None
        self.__df_list: List[pd.DataFrame] = []

    @property
    def query_date(self) -> Optional[datetime.date]:
        return self.__query_date

    @query_date.setter
    def query_date(self, value: datetime.date):
        self.__query_date = value

    @property
    def df_result(self) -> pd.DataFrame:
        return (
            pd.concat(self.__df_list).query(f'品種代碼 in {list(self.target_items)}')
            if self.__df_list
            else pd.DataFrame()
        )

    @df_result.setter
    def df_result(self, value: Optional[List[OrderedDict]]):
        if value:
            self.__df_list.append(pd.DataFrame(value))

    @property
    def request_date_str(self) -> str:
        dt = self.__query_date

        return f'{dt.year - 1911}.{dt.month}.{dt.day}'

    @property
    def date_str(self) -> str:
        dt = self.__query_date

        return f'{self.roc_year}{dt.month:02d}{dt.day:02d}'

    @property
    def roc_year(self) -> str:
        return f'{self.__query_date.year - 1911}'

    @property
    def month(self) -> str:
        return f'{self.__query_date.month}'

    @property
    def day(self) -> str:
        return f'{self.__query_date.day}'

    @property
    def params_list(self) -> List[dict]:
        """ 用於發送 POST 請求的參數列表 """

        return [
            {
                "dateStr": self.request_date_str,
                "calendarType": "tw",
                "year": self.roc_year,
                "month": self.month,
                "day": self.day,
                "mid": key,
                "numbers": "999",
                "orderby": "w",
            }
            for key in self.SOURCES.keys()
        ]

    @property
    def headers_list(self) -> List[dict]:
        return [
            {
                **self.HEADERS,
            }
            for _ in self.SOURCES.keys()
        ]

    @property
    def urls_list(self) -> List[str]:
        return [
            self.URL
            for _ in self.SOURCES.keys()
        ]

    def _make_request(self, url, params, headers) -> Response:
        """
        發送 POST 請求，並回傳 response 物件，若發生錯誤則嘗試重新連線
        """

        retry_count = 0

        while retry_count < self.MAX_RETRY:
            try:
                resp = post(url, params=params, headers=headers)
                self.LOGGER_EXTRA['request_url'] = resp.request.url

                if resp.status_code != 200:
                    retry_count += 1

                    self.LOGGER.warning(f'Connection Refused, Retry {retry_count} Time', extra=self.LOGGER_EXTRA)
                    time.sleep(self.SLEEP_TIME)

                    continue

                return resp
            except Exception as e:
                self.LOGGER.exception(f'exception: {e}', extra=self.LOGGER_EXTRA)

                return Response()

    def _convert_to_dataframe(self, responses: List[Response]) -> pd.DataFrame:
        for resp in responses:
            parser = HTMLParser(response=resp, api=self)

            if resp.status_code == 200:
                self.df_result = parser.parse_table()
            else:
                self.LOGGER.warning(
                    f'Connection Refused, Status Code: {resp.status_code}, '
                    f'source: {self.SOURCES.get(parser.source_code)}', extra=self.LOGGER_EXTRA
                )

        return self.df_result

    def requests(
            self, start_date: Optional[datetime.date] = None, end_date: Optional[datetime.date] = None
    ) -> List[Response]:
        if start_date is None and end_date is None:
            raise ValueError('start_date or end_date must be set')

        self.query_date = start_date or end_date

        # 為增加效率，使用 ThreadPoolExecutor 進行多執行緒請求
        with ThreadPoolExecutor(max_workers=5) as executor:
            return list(executor.map(self._make_request, self.urls_list, self.params_list, self.headers_list))

    def loads(self, responses: List[Response]):
        df = self._convert_to_dataframe(responses)

        try:
            if not df.empty:
                self._access_data_from_api(df)
        except Exception as e:
            self.LOGGER.exception(f'exception: {e}', extra=self.LOGGER_EXTRA)
