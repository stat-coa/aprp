import datetime
import re
from concurrent.futures import ThreadPoolExecutor
from typing import List

import pandas as pd
import urllib3
from pandas import Series
from requests import Response

from apps.dailytrans.models import DailyTran
from .abstract import AbstractApi
from .utils import date_transfer

urllib3.disable_warnings()


class Api(AbstractApi):
    """
    此 API 專門用來請求蔬菜(Crop)與水果(Fruit)的產地價格資料。

    API 參數特殊的情況只有兩種:
    1. 蔬菜品項為 "蒜頭(蒜球)(旬價)"
    2. 水果品項為 "青香蕉下品(內銷)"
    """

    # Settings
    API_NAME = 'apis'
    ZFILL = False
    ROC_FORMAT = False
    SEP = '/'

    # Filters
    START_DATE_FILTER = 'startYear=%s&startMonth=%s&startDay=%s'
    END_DATE_FILTER = 'endYear=%s&endMonth=%s&endDay=%s'
    NAME_FILTER = 'productName=%s'

    def __init__(self, model, config_code, type_id, logger_type_code=None):
        super(Api, self).__init__(model=model, config_code=config_code, type_id=type_id,
                                  logger='aprp', logger_type_code=logger_type_code)

    # TODO: to be removed after the API is fixed
    def hook(self, dic):
        for key, value in dic.items():
            if isinstance(value, str):
                dic[key] = value.strip()

        product_name = dic.get('PRODUCTNAME')
        source_name = dic.get('ORGNAME')
        product = self.PRODUCT_QS.filter(code=product_name).first()
        source = self.SOURCE_QS.filter(name=source_name).first()
        if product and source:
            tran = DailyTran(
                product=product,
                source=source,
                avg_price=float(dic.get('AVGPRICE')),
                date=date_transfer(sep=self.SEP, string=dic.get('PERIOD'), roc_format=self.ROC_FORMAT)
            )
            return tran
        else:
            if product_name and not product:
                self.LOGGER.warning('Cannot Match Product: %s' % (product_name),
                                    extra=self.LOGGER_EXTRA)
            if source_name and source_name != '當日平均價' and not source:
                self.LOGGER.warning('Cannot Match Source: %s' % (source_name),
                                    extra=self.LOGGER_EXTRA)
            return dic

    # TODO: to be removed after the API is fixed
    def hook2(self, dic):
        for key, value in dic.items():
            if isinstance(value, str):
                dic[key] = value.strip()

        product_name = "青香蕉下品(內銷)"
        source_name = dic.get('ORGNAME')
        product = self.PRODUCT_QS.filter(code=product_name).first()
        source = self.SOURCE_QS.filter(name=source_name).first()
        if product and source:
            tran = DailyTran(
                product=product,
                source=source,
                avg_price=float(dic.get('AVGPRICE')),
                date=date_transfer(sep=self.SEP, string=dic.get('PERIOD'), roc_format=self.ROC_FORMAT)
            )
            return tran
        else:
            if product_name and not product:
                self.LOGGER.warning('Cannot Match Product: %s' % (product_name),
                                    extra=self.LOGGER_EXTRA)
            if source_name and source_name != '當日平均價' and not source:
                self.LOGGER.warning('Cannot Match Source: %s' % (source_name),
                                    extra=self.LOGGER_EXTRA)
            return dic

    # TODO: to be removed after the API is fixed
    def hook3(self, dic):
        for key, value in dic.items():
            if isinstance(value, str):
                dic[key] = value.strip()

        product_name = "蒜頭(蒜球)(旬價)"
        source_name = dic.get('ORGNAME')
        YEAR = dic.get('YEAR')
        MONTH = dic.get('MONTH')
        PERIOD = dic.get('PERIOD')
        if PERIOD == None:
            pass
        elif '上旬' in PERIOD:
            PERIOD = '05'
        elif '中旬' in PERIOD:
            PERIOD = '15'
        elif '下旬' in PERIOD:
            PERIOD = '25'

        if PERIOD == None:
            pass
        else:
            date = f'{YEAR}/{MONTH}/{PERIOD}'

        product = self.PRODUCT_QS.filter(code=product_name).first()
        source = self.SOURCE_QS.filter(name=source_name).first()
        if product and source:
            tran = DailyTran(
                product=product,
                source=source,
                avg_price=float(dic.get('AVGPRICE')),
                date=date_transfer(sep=self.SEP, string=date, roc_format=self.ROC_FORMAT)
            )
            return tran
        else:
            if product_name and not product:
                self.LOGGER.warning('Cannot Match Product: %s' % (product_name),
                                    extra=self.LOGGER_EXTRA)
            if source_name and not source:
                self.LOGGER.warning('Cannot Match Source: %s' % (source_name),
                                    extra=self.LOGGER_EXTRA)
            return dic

    @property
    def sources(self):
        return self.SOURCE_QS.values_list('name', flat=True)

    @property
    def products(self):
        return self.PRODUCT_QS.values_list('code', flat=True)

    @staticmethod
    def _access_garlic_data_from_api(data: dict):
        """
        針對 "蒜頭(蒜球)(旬價)" 的 API Response 進行處理，將 "PERIOD" 欄位轉換成日期格式。
        :param data: dict: API Response for "蒜頭(蒜球)"
        """

        data['PERIOD'] = (f"{data['YEAR']}/{data['MONTH']}/"
                          f"{(5 if data['PERIOD'] == '上旬' else 15 if data['PERIOD'] == '中旬' else 25)}")

        data.pop('fun')
        data.pop('YEAR')
        data.pop('MONTH')

        data['PRODUCTNAME'] = '蒜頭(蒜球)(旬價)'

        return data

    def _get_formatted_url(
            self,
            start_date: datetime.date,
            end_date: datetime.date,
            name: str = None
    ) -> str:
        """
        Format the url with filters.

        :param start_date: start date
        :param end_date: end date
        :param name: product name
        """

        url = self.API_URL

        # 設定日期範圍
        if start_date:
            if not isinstance(start_date, datetime.date):
                raise NotImplementedError

            url = '&'.join((url, self.START_DATE_FILTER % (start_date.year, start_date.month, start_date.day)))
        if end_date:
            if not isinstance(end_date, datetime.date):
                raise NotImplementedError

            url = '&'.join((url, self.END_DATE_FILTER % (end_date.year, end_date.month, end_date.day)))
        if start_date and end_date and start_date > end_date:
            raise AttributeError

        return '&'.join((url, self.NAME_FILTER % (name or '')))
    
    def _get_urls(self, formatted_url: str, name: str = None):
        """
        Get urls for different products.

        :param formatted_url: formatted url that contains filters
        :param name: product name
        """
        
        urls = [formatted_url]

        if name == "青香蕉下品(內銷)":
            urls[0] = urls[0].replace("status=4", "status=6").replace("青香蕉下品", "青香蕉")
        elif self.CONFIG.code == 'COG06' and name is None:
            urls.append(formatted_url.replace("status=4", "status=6") + "青香蕉")

        if name == "蒜頭(蒜球)(旬價)":
            urls[0] = urls[0].replace("status=4", "status=7").replace("蒜頭(蒜球)(旬價)", "蒜頭(蒜球)")
        elif self.CONFIG.code == 'COG05' and name is None:
            urls.append(formatted_url.replace("status=4", "status=7") + "蒜頭(蒜球)")

        return urls

    def _handle_response(self, response: Response) -> List[dict]:
        """
        Handle the response from the API.
        """

        data = response.json()
        data_set = data.get('DATASET')

        # handle special cases
        if re.search(r'status=(\d+)', response.url)[1] == '7':
            # handle "status=7" case
            data_set = [self._access_garlic_data_from_api(item) for item in data_set]

        elif re.search(r'status=(\d+)', response.url)[1] == '6':
            # handle "status=6" case
            data_set = list(
                map(
                    lambda x: {**x, 'PRODUCTNAME': x['PRODUCTNAME'][:3] + '下品' + x['PRODUCTNAME'][3:]},
                    data_set
                )
            )
        return data_set

    def _convert_to_data_frame(self, data: list) -> pd.DataFrame:
        """
        Convert the data to a DataFrame.

        :param data: list of data from API
        """

        # TODO: compare the data with the daily report
        # data_api_avg = data_api[data_api['ORGNAME'] == '當日平均價']

        return (
            pd
            .DataFrame(data)
            .assign(AVGPRICE=lambda x: x['AVGPRICE'].astype(float))
            .query('ORGNAME != "當日平均價"')
            .assign(PRODUCTNAME=lambda x: x['PRODUCTNAME'].str.strip())
            .assign(ORGNAME=lambda x: x['ORGNAME'].str.strip())
            .loc[lambda x: x['PRODUCTNAME'].isin(self.products)]
            .loc[lambda x: x['ORGNAME'].isin(self.sources)]
        )

    def request(self, start_date=None, end_date=None, *args, **kwargs):
        """
        Request data from the API.

        :param start_date: datetime.date: start date
        :param end_date: datetime.date: end date
        """

        # 取得品項名稱
        name = kwargs.get('name') or None
        formatted_url = self._get_formatted_url(start_date, end_date, name)
        urls = self._get_urls(formatted_url, name)

        # request data from the API with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=3) as executor:
            results = list(executor.map(self.get, urls))

        return results

    def load(self, responses: List[Response]):
        """
        Load data from the API response.

        :param responses: list of API responses
        """
        data = []

        for response in responses:
            try:
                data_set = self._handle_response(response)

                data.extend(data_set)
            except Exception as e:
                if len(response.text) == 0:
                    self.LOGGER.warning(f'no data returned\nurl={response.request.url}', extra=self.LOGGER_EXTRA)
                else:
                    self.LOGGER.exception(
                        f'resp={response.text}\nurl={response.request.url}\nexc={e}',
                        extra=self.LOGGER_EXTRA
                    )

        # data should look like [D, B, {}, C, {}...] after loads
        if not data:
            return

        data_api = self._convert_to_data_frame(data)

        try:
            self._access_data_from_api(data_api)
        except Exception as e:
            self.LOGGER.exception(f'exception: {e}, data_api: {data_api}', extra=self.LOGGER_EXTRA)

    def _access_data_from_api(self, data: pd.DataFrame):
        """
        Compare data from API with data in DB and update or delete records accordingly.
        """
        # merge data from API with data in DB
        data_merge = self._compare_data_from_api_and_db(data)

        # filter out records that need to be updated or deleted
        # due to merge two data with date, product id and source id, there are two average prices data that are from
        # Api and DB respectively, we can compare these two and decide whether we should update or delete.
        condition = (data_merge['avg_price_x'] != data_merge['avg_price_y'])

        if not data_merge[condition].empty:
            for _, value in data_merge[condition].fillna('').iterrows():
                series: Series = value

                try:
                    # get the existed DailyTran record
                    existed_tran = DailyTran.objects.get(id=int(series['id'] or 0))

                    if series['avg_price_x']:
                        # if the avg_price in API is not None, update the existed record
                        self._update_data(series, existed_tran)
                    else:
                        # if the avg_price in API is None, delete the existed record
                        existed_tran.delete()
                        self.LOGGER.warning(msg=f"The DailyTran data of the product: {series['product__code']} "
                                                f"on {series['date'].strftime('%Y-%m-%d')} "
                                                f"from source: {series['source__name']} with\n"
                                                f"avg_price: {series['avg_price_y']}\n has been deleted.")
                except Exception as e:
                    # if no existed record is found, save the data as a new record
                    self._save_new_data(series)

    def _compare_data_from_api_and_db(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Compare data from API with data in DB and return a merged DataFrame.

        The DataFrame is merged on columns 'date', 'product__code', and 'source__name'.
        The columns 'id', 'avg_price' are added from the DB data.
        The columns 'avg_price_x' and 'avg_price_y' are added from the API and DB data.
        The 'avg_price_x' is the value from API, and 'avg_price_y' is the value from DB.
        :param data: DataFrame: data from API
        :return: DataFrame: merged DataFrame
        """

        columns = {
            'AVGPRICE': 'avg_price',
            'ORGNAME': 'source__name',
            'PERIOD': 'date',
            'PRODUCTNAME': 'product__code',
        }
        merged_by_columns = ['date', 'product__code', 'source__name']
        use_columns = ['id', 'avg_price'] + merged_by_columns
        data.rename(columns=columns, inplace=True)
        data['date'] = data['date'].apply(lambda x: datetime.datetime.strptime(x, '%Y/%m/%d').date())

        daily_tran_qs = DailyTran.objects.filter(
            date=data['date'].iloc[0], product__type=self.TYPE, product__config=self.CONFIG
        )
        data_db = (
            pd.DataFrame(list(daily_tran_qs.values(*use_columns)))
            if daily_tran_qs
            else pd.DataFrame(columns=use_columns)
        )

        return data.merge(data_db, on=merged_by_columns, how='outer')

    def _update_data(self, series: Series, existed_tran: DailyTran):
        """
        Update the data in DB with the data from API.

        :param series: data from API that convert to Pandas `Series`
        :param existed_tran: the existed record in DB
        """

        # update the existed record with the new data
        # the avg_price_x is the value from API, it's newer than the value in DB
        existed_tran.avg_price = series['avg_price_x']

        existed_tran.save()
        self.LOGGER.info(
            msg=f"The data of the product: {series['product__code']} on"
                f" {series['date'].strftime('%Y-%m-%d')} has been updated.")

    def _save_new_data(self, series: Series):
        """
        Save the data in DB.

        :param series: data from API that convert to Pandas `Series`
        """
        # get the products from DB
        products = self.MODEL.objects.filter(code=series['product__code'])

        # get the source from DB
        source = self.SOURCE_QS.get(name=series['source__name'])

        # create a list of new records
        new_trans_list = [DailyTran(
            product=product,
            source=source,
            avg_price=series['avg_price_x'],
            date=series['date']
        ) for product in products]

        # bulk create the new records
        DailyTran.objects.bulk_create(new_trans_list)
