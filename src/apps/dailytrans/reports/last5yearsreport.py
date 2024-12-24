import itertools
import logging
import operator
from _pydecimal import Context, ROUND_HALF_UP
from datetime import date
from functools import reduce
from typing import List, Union

import numpy as np
import pandas as pd
import psycopg2
from django.conf import settings
from sqlalchemy import create_engine

from apps.configs.models import AbstractProduct

db_logger = logging.getLogger('aprp')

#連結資料庫相關設定
database = settings.DATABASES['default']['NAME']
user = settings.DATABASES['default']['USER']
password = settings.DATABASES['default']['PASSWORD']
host = settings.DATABASES['default']['HOST']
port = settings.DATABASES['default']['PORT']

con = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
engine = create_engine('postgresql://' + user + ':' + password + '@' + host + ':' + str(port) + '/' + database, echo=False)

class Last5YearsReportFactory(object):
    def __init__(self, product_id: List[int], source: Union[List[int], list], is_hogs=False, is_rams=False):
        self.product_id = product_id
        self.source = source
        self.today = date.today()
        self.today_year = self.today.year
        self.today_month = self.today.month
        self.last_5_years_ago = self.today_year - 5
        self.last_year = self.today_year - 1
        self.is_hogs = is_hogs
        self.is_rams = is_rams

    def get_table(self) -> pd.DataFrame:
        """
        使用 pandas 直接連線資料庫，將特定品項的近五年日交易(DailyTran)資料撈出來，並且轉成 DataFrame 後回傳
        """

        product_qs = AbstractProduct.objects.filter(id__in=self.product_id)
        products = product_qs.exclude(track_item=False)
        
        if product_qs.filter(track_item=False):
            sub_products = reduce(
                operator.or_,
                (product.children().filter(track_item=True) for product in product_qs.filter(track_item=False))
            )
            products = products | sub_products

        if product_qs.first().track_item is False and product_qs.first().config.id == 13:
            products = product_qs

        self.all_product_id_list = [i.id for i in products]
        all_date_list = [f'{self.last_5_years_ago}-01-01',self.today.strftime("%Y-%m-%d")]
        table = pd.read_sql_query("select product_id, source_id, avg_price, avg_weight, volume, date from dailytrans_dailytran INNER JOIN unnest(%(all_product_id_list)s) as pid ON pid=dailytrans_dailytran.product_id where ((date between %(all_date_list00)s and %(all_date_list01)s))", params={'all_product_id_list':self.all_product_id_list,'all_date_list00':all_date_list[0],'all_date_list01':all_date_list[1]},con=engine)

        table['date'] = pd.to_datetime(table['date'], format='%Y-%m-%d')

        return table

    def result(self, table: pd.DataFrame):
        source_list = self.source
        product_data_dict = {}
        avg_price_dict = {}
        avg_volume_dict = {}
        avg_weight_dict = {}
        avg_volume_weight_dict = {}
        has_volume = False
        has_weight = False
        last_5_years_one_month_table = pd.DataFrame()

        # 迭代近五年年分
        for y in range(self.last_5_years_ago, self.today_year + 1):
            # 第一個元素固定為整年份平均價格，後續會依序加入每個月份的平均價格
            avg_price_month_list: List[float] = []
            avg_volume_month_list: List[float] = []
            avg_weight_month_list: List[float] = []
            avg_volume_weight_month_list: List[float] = []

            """
            total_price -> 整個年份的總價格:
                1. 有 weight 與 volume: price * weight * volume
                2. 只有 volume: price * volume
                3. 若 weight 與 volume 都沒有: group by date 的平均價格後相加
                
            total_volume -> 整個年份的總交易量
            """
            total_price, total_volume, total_weight, total_volume_weight = 0, 0, 0, 0

            """
            days_with_price -> 加權平均重量，會用於計算年平均價格:
                1. 有 weight 與 volume: weight * volume
                2. 只有 volume: volume
                3. 若 weight 與 volume 都沒有: 單純計算有價格的天數當作 volume
                
            days_with_volume -> 計算整年有交易量的天數，會用於計算年平均交易量:
                1. 有 volume: group by date 的 volume 的 count
                2. 若 weight 與 volume 都沒有: 0
            """
            days_with_price, days_with_volume, days_with_weight, days_with_volume_weight = 0, 0, 0, 0
            end_month = 13

            if y == self.today_year:
                end_month = self.today_month + 1

            # 迭代年分的每個月
            for m in range(1, end_month):
                # 將單一月份的 DataFtaFrame 取出，若有來源或是品項為毛豬，則會再進一步過濾資料
                if source_list:
                    one_month_data = table[(pd.to_datetime(table['date']).dt.year == y) & (pd.to_datetime(table['date']).dt.month == m) ].query("source_id == @source_list")
                else:
                    # 毛豬(規格豬)計算需排除澎湖市場
                    if self.is_hogs:
                        one_month_data = table[(pd.to_datetime(table['date']).dt.year == y) & (pd.to_datetime(table['date']).dt.month == m) ].query("source_id != 40050")
                    else:
                        # 一般情況下，直接取出單一月份的 DataFrame
                        one_month_data = table[(pd.to_datetime(table['date']).dt.year == y) & (pd.to_datetime(table['date']).dt.month == m) ]

                if one_month_data['avg_price'].any():
                    has_volume = one_month_data['volume'].notna().sum() / one_month_data['avg_price'].count() > 0.8
                    has_weight = one_month_data['avg_weight'].notna().sum() / one_month_data['avg_price'].count() > 0.8
                else:
                    has_volume = False
                    has_weight = False

                if has_volume and has_weight:
                    one_month_total_price = (one_month_data['avg_price'] * one_month_data['avg_weight'] * one_month_data['volume']).sum()
                    one_month_total_weight = (one_month_data['avg_weight'] * one_month_data['volume']).sum()
                    one_month_total_volume = (one_month_data['volume']).sum()
                    total_price += one_month_total_price
                    total_weight += one_month_total_weight
                    avgprice = one_month_total_price / one_month_total_weight
                    avgweight = one_month_total_weight / one_month_total_volume

                    # 羊的交易量
                    if self.is_rams:
                        total_volume += one_month_total_volume
                        avgvolume = one_month_data.groupby('date').sum()['volume'].mean()

                    # 毛豬交易量為頭數
                    elif self.is_hogs:
                        total_volume += one_month_total_volume / 1000
                        total_volume_weight += one_month_total_weight
                        avgvolume = one_month_data.groupby('date').sum()['volume'].mean() / 1000
                        avgweight = avgweight
                        avgvolumeweight = (avgweight*avgvolume*1000) / 1000

                    # 環南市場-雞的交易量
                    else:
                        # avgvolume = (one_month_data['avg_weight']*one_month_data['volume']).sum()/(one_month_data['volume']).sum()
                        total_volume += one_month_total_volume
                        avgvolume = one_month_data.groupby('date').sum()['volume'].mean()
                        avgweight = one_month_data.groupby('date').sum()['avg_weight'].mean()

                    days_with_price += (one_month_data['avg_weight'] * one_month_data['volume']).sum()
                    days_with_weight += one_month_data['volume'].sum()
                    days_with_volume += one_month_data.groupby('date').sum()['volume'].count()
                    days_with_volume_weight += one_month_data.groupby('date').sum()['volume'].count()

                elif has_volume:
                    total_price += (one_month_data['avg_price'] * one_month_data['volume']).sum()
                    total_volume += one_month_data.groupby('date').sum()['volume'].sum() / 1000
                    avgprice = (one_month_data['avg_price'] * one_month_data['volume']).sum() / one_month_data['volume'].sum()
                    avgvolume = one_month_data.groupby('date').sum()['volume'].mean() / 1000
                    days_with_price += (one_month_data['volume'].sum())
                    days_with_volume += one_month_data.groupby('date').sum()['volume'].count()

                else:
                    total_price += one_month_data.groupby('date').mean()['avg_price'].sum()
                    avgprice = one_month_data.groupby('date').mean()['avg_price'].mean()
                    avgvolume = np.nan
                    avgweight = np.nan
                    days_with_price += one_month_data.groupby('date').mean()['avg_price'].count()

                avg_price_month_list.append(float(Context(prec=28, rounding=ROUND_HALF_UP).create_decimal(avgprice)))
                avg_volume_month_list.append(float(Context(prec=28, rounding=ROUND_HALF_UP).create_decimal(avgvolume)))

                if self.is_hogs and has_weight:
                    avg_weight_month_list.append(float(Context(prec=28, rounding=ROUND_HALF_UP).create_decimal(avgweight)))
                    avg_volume_weight_month_list.append(float(Context(prec=28, rounding=ROUND_HALF_UP).create_decimal(avgvolumeweight)))
                elif has_weight:
                    avg_weight_month_list.append(float(Context(prec=28, rounding=ROUND_HALF_UP).create_decimal(avgweight)))

            # insert yearly avg price, volume, weight and volume * weight to dict
            avgprice_year = total_price / days_with_price
            avg_price_month_list.insert(0, float(Context(prec=28, rounding=ROUND_HALF_UP).create_decimal(avgprice_year)))

            if [x for x in avg_volume_month_list if x == x]:
                avgvolume_year = total_volume / days_with_volume
                avg_volume_month_list.insert(0, float(Context(prec=28, rounding=ROUND_HALF_UP).create_decimal(avgvolume_year)))

            avg_price_dict[f"{y - 1911}年"] = avg_price_month_list
            avg_volume_dict[f"{y - 1911}年"] = avg_volume_month_list

            if self.is_hogs and has_weight:
                avgweight_year = total_weight / days_with_weight
                avg_weight_month_list.insert(0, float(Context(prec=28, rounding=ROUND_HALF_UP).create_decimal(avgweight_year)))
                avg_weight_dict[f"{y - 1911}年"] = avg_weight_month_list
                avgvolumeweight_yaer = total_volume_weight / days_with_volume_weight / 1000
                avg_volume_weight_month_list.insert(0, float(Context(prec=28, rounding=ROUND_HALF_UP).create_decimal(avgvolumeweight_yaer)))
                avg_volume_weight_dict[f"{y - 1911}年"] = avg_volume_weight_month_list
            elif has_weight:
                avgweight_year = total_weight / days_with_weight
                avg_weight_month_list.insert(0, float(Context(prec=28, rounding=ROUND_HALF_UP).create_decimal(avgweight_year)))
                avg_weight_dict[f"{y - 1911}年"] = avg_weight_month_list

        product_data_dict[self.all_product_id_list[0]] = {
            'avgprice': avg_price_dict,
            'avgvolume': avg_volume_dict,
            'avgweight': avg_weight_dict,
            'avgvolumeweight': avg_volume_weight_dict
        }

        # 近五年平均值
        last_5_years_avg_data = {}
        last_5_years_avg_data['avgprice'] = {}
        last_5_years_avg_data['avgvolume'] = {}
        last_5_years_avg_data['avgweight'] = {}
        last_5_years_avg_data['avgvolumeweight'] = {}
        has_volume = False
        has_weight = False
        last_5_years_avgprice_list = [np.nan]
        last_5_years_avgvolume_list = [np.nan]
        last_5_years_avgweight_list = [np.nan]
        last_5_years_avgvolumeweight_list = [np.nan]
        avgprice_data = pd.DataFrame()
        avgvolume_data = pd.DataFrame()
        avgweight_data = pd.DataFrame()
        avgvolumeweight_data = pd.DataFrame()

        for m in range(1, 13):
            avgvolume_temp_list = []

            if source_list:
                last_5_years_one_month_table = table[(pd.to_datetime(table['date']).dt.year >= self.last_5_years_ago) & (pd.to_datetime(table['date']).dt.year <= self.last_year) & (pd.to_datetime(table['date']).dt.month == m)].query("source_id == @source_list")
            else:
                if self.is_hogs: #毛豬(規格豬)計算需排除澎湖市場
                    last_5_years_one_month_table = table[(pd.to_datetime(table['date']).dt.year >= self.last_5_years_ago) & (pd.to_datetime(table['date']).dt.year <= self.last_year) & (pd.to_datetime(table['date']).dt.month == m)].query("source_id != 40050")
                else:
                    last_5_years_one_month_table = table[(pd.to_datetime(table['date']).dt.year >= self.last_5_years_ago) & (pd.to_datetime(table['date']).dt.year <= self.last_year) & (pd.to_datetime(table['date']).dt.month == m)]

            if last_5_years_one_month_table['avg_price'].any():
                has_volume = last_5_years_one_month_table['volume'].notna().sum() / last_5_years_one_month_table['avg_price'].count() > 0.8
                has_weight = last_5_years_one_month_table['avg_weight'].notna().sum() / last_5_years_one_month_table['avg_price'].count() > 0.8
            else:
                has_volume = False
                has_weight = False

            last_5_years_one_month_table=last_5_years_one_month_table.copy()    #此步驟為避免後續 dataframe 計算過程中出現警告訊息

            if has_volume and has_weight:
                last_5_years_one_month_table['pvw'] = last_5_years_one_month_table['avg_price'] * last_5_years_one_month_table['volume'] * last_5_years_one_month_table['avg_weight']
                last_5_years_one_month_table['vw'] = last_5_years_one_month_table['volume'] * last_5_years_one_month_table['avg_weight']
                one_month_avgprice = last_5_years_one_month_table.groupby('date')['pvw'].sum()/last_5_years_one_month_table.groupby('date')['vw'].sum()
                one_month_avgweight = last_5_years_one_month_table.groupby('date')['vw'].sum()/last_5_years_one_month_table.groupby('date')['volume'].sum()
                one_month_sumvolume = last_5_years_one_month_table.groupby('date')['volume'].sum()
                avgprice_one_month = (one_month_avgprice*one_month_sumvolume*one_month_avgweight).sum()/(one_month_sumvolume*one_month_avgweight).sum()
                avgweight_one_month = (one_month_sumvolume*one_month_avgweight).sum()/(one_month_sumvolume).sum()
                last_5_years_avg_data['avgprice'][m] = float(Context(prec=28, rounding=ROUND_HALF_UP).create_decimal(avgprice_one_month))

                if self.is_rams: #羊的交易量,
                    last_5_years_avg_data['avgvolume'][m] = last_5_years_one_month_table.groupby('date').sum()['volume'].mean()
                    last_5_years_avg_data['avgweight'][m] = float(Context(prec=28, rounding=ROUND_HALF_UP).create_decimal(avgweight_one_month))
                elif self.is_hogs: #毛豬交易量為頭數
                    last_5_years_avg_data['avgvolume'][m] = last_5_years_one_month_table.groupby('date').sum()['volume'].mean() / 1000
                    last_5_years_avg_data['avgweight'][m] = float(Context(prec=28, rounding=ROUND_HALF_UP).create_decimal(avgweight_one_month))
                    one_month_avgvolumeweight = (last_5_years_one_month_table.groupby('date')['vw'].sum()).mean() / 1000
                    last_5_years_avg_data['avgvolumeweight'][m] = float(Context(prec=28, rounding=ROUND_HALF_UP).create_decimal(one_month_avgvolumeweight))
                    last_5_years_avgvolumeweight_list.append(last_5_years_avg_data['avgvolumeweight'][m])
                else:
                    last_5_years_avg_data['avgvolume'][m] = last_5_years_one_month_table.groupby('date').sum()['volume'].mean()
                    last_5_years_avg_data['avgweight'][m] = last_5_years_one_month_table.groupby('date').sum()['avg_weight'].mean()

                last_5_years_avgprice_list.append(last_5_years_avg_data['avgprice'][m])
                last_5_years_avgvolume_list.append(last_5_years_avg_data['avgvolume'][m])
                last_5_years_avgweight_list.append(last_5_years_avg_data['avgweight'][m])


            elif has_volume:
                #平均價
                last_5_years_one_month_table['pv']=last_5_years_one_month_table['avg_price'] * last_5_years_one_month_table['volume']
                one_month_avgprice = last_5_years_one_month_table.groupby('date')['pv'].sum()/last_5_years_one_month_table.groupby('date')['volume'].sum()
                one_month_sumvolume = last_5_years_one_month_table.groupby('date').sum()['volume'].values
                avgprice_one_month = (one_month_avgprice*one_month_sumvolume).sum()/one_month_sumvolume.sum()
                last_5_years_avg_data['avgprice'][m] = float(Context(prec=28, rounding=ROUND_HALF_UP).create_decimal(avgprice_one_month))

                last_5_years_avgprice_list.append(last_5_years_avg_data['avgprice'][m])

                #平均量
                last_5_years_avgvolume_month = last_5_years_one_month_table.groupby('date').sum()['volume'].values
                for j in last_5_years_avgvolume_month:
                    avgvolume_temp_list.append(float(Context(prec=28, rounding=ROUND_HALF_UP).create_decimal(j)))

                last_5_years_avg_data['avgvolume'][m] = sum(avgvolume_temp_list) / len(avgvolume_temp_list) / 1000
                last_5_years_avgvolume_list.append(last_5_years_avg_data['avgvolume'][m])

            else:
                if last_5_years_one_month_table.groupby('date').mean()['avg_price'].values.any():
                    one_month_avgprice = last_5_years_one_month_table.groupby('date').mean()['avg_price'].values.mean()
                    last_5_years_avg_data['avgprice'][m] = one_month_avgprice
                    last_5_years_avgprice_list.append(last_5_years_avg_data['avgprice'][m])
                    has_price = True
                else:
                    last_5_years_avgprice_list.append(np.nan)
                    has_price = False

                # 為避免list對應月份數量錯誤,缺少數值的月份補空值
                last_5_years_avgvolume_list.append(np.nan)
                last_5_years_avgweight_list.append(np.nan)
                last_5_years_avgvolumeweight_list.append(np.nan)

        columns_name = ['年平均', '1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月']
        avgprice_data = pd.DataFrame.from_dict(product_data_dict[self.all_product_id_list[0]]['avgprice'], orient='index')
        avgprice_data.columns = columns_name
        avgprice_data.loc['近五年平均'] = last_5_years_avgprice_list
        avgprice_data = avgprice_data.round(2)

        # define a variable for avg volume dict
        temp_avg_volume = product_data_dict[self.all_product_id_list[0]]['avgvolume']

        # merge all avg volume data into one list and remove nan value
        avg_volumes = [i for i in list(itertools.chain(*list(temp_avg_volume.values()))) if str(i) != 'nan']

        if temp_avg_volume and sum(avg_volumes) > 0:
            avgvolume_data = pd.DataFrame.from_dict(product_data_dict[self.all_product_id_list[0]]['avgvolume'], orient='index')
            avgvolume_data.columns = columns_name
            avgvolume_data.loc['近五年平均'] = last_5_years_avgvolume_list
            avgvolume_data = avgvolume_data.round(3)

        if self.is_hogs and product_data_dict[self.all_product_id_list[0]]['avgweight']:
            avgweight_data = pd.DataFrame.from_dict(product_data_dict[self.all_product_id_list[0]]['avgweight'], orient='index')
            avgweight_data.columns = columns_name
            avgweight_data.loc['近五年平均'] = last_5_years_avgweight_list
            avgweight_data = avgweight_data.round(3)

            avgvolumeweight_data = pd.DataFrame.from_dict(product_data_dict[self.all_product_id_list[0]]['avgvolumeweight'], orient='index')
            avgvolumeweight_data.columns = columns_name
            avgvolumeweight_data.loc['近五年平均'] = last_5_years_avgvolumeweight_list
            avgvolumeweight_data = avgvolumeweight_data.round(3)

        elif product_data_dict[self.all_product_id_list[0]]['avgweight']:
            avgweight_data = pd.DataFrame.from_dict(product_data_dict[self.all_product_id_list[0]]['avgweight'], orient='index')
            avgweight_data.columns = columns_name
            avgweight_data.loc['近五年平均'] = last_5_years_avgweight_list
            avgweight_data = avgweight_data.round(3)

        return avgprice_data, avgvolume_data, avgweight_data, avgvolumeweight_data

    def __call__(self):
        df = self.get_table()

        if not df.empty:
            return self.result(df)
        else:
            db_logger.error(f'DB query error : product_id_list = {self.all_product_id_list}; source_list = {self.source}', extra={'type_code': 'LOT-last5yearsreport'})
