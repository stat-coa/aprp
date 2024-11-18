import time
import datetime

from django.db.models.expressions import RawSQL

import pandas as pd

from django.utils.translation import ugettext as _
from django.db.models import Func, IntegerField, Q

from apps.dailytrans.models import DailyTran, is_leap
from apps.configs.api.serializers import TypeSerializer
from apps.watchlists.models import WatchlistItem
from apps.configs.models import AbstractProduct


def get_query_set(_type, items, sources=None):
    """
    獲取符合條件的每日交易資料查詢集

    此函數用於根據給定的產品類型和項目列表(可以是監控項目或產品)來過濾每日交易資料。
    支援額外的來源過濾，如果未指定來源，則會自動從項目中獲取相關的來源。

    Args:
        _type (Type): 產品類型對象
            - 用於過濾特定類型的交易資料
            - 必須是 configs.Type 的實例

        items (QuerySet): 項目查詢集
            - 支援兩種類型的項目:
                1. WatchlistItem: 監控清單項目
                2. AbstractProduct: 產品對象
            - 用於確定要查詢哪些產品的交易資料

        sources (Iterable[Source], optional): 資料來源集合
            - 可選參數，用於進一步過濾特定來源的資料
            - 如果未提供，將自動從 items 中獲取相關的來源
            - 可以是 Source 對象的任何可迭代集合

    Returns:
        QuerySet[DailyTran]: 過濾後的每日交易資料查詢集
            - 返回符合所有過濾條件的 DailyTran 查詢集
            - 如果 items 為空，返回空的查詢集

    Raises:
        AttributeError: 當 items 中包含不支援的對象類型時拋出

    使用範例:
        ```python
        # 使用監控清單項目查詢
        watchlist_items = WatchlistItem.objects.filter(parent=watchlist)
        query_set = get_query_set(type_obj, watchlist_items)

        # 使用產品對象查詢，並指定特定來源
        products = AbstractProduct.objects.filter(type=type_obj)
        specific_sources = Source.objects.filter(type=type_obj)
        query_set = get_query_set(type_obj, products, sources=specific_sources)
        ```

    實現邏輯:
    1. 檢查 items 是否為空
    2. 驗證 items 中的對象類型
    3. 收集所有相關的產品 ID
    4. 建立基本的查詢條件
    5. 處理來源過濾:
       - 如果提供了 sources 參數，直接使用
       - 否則從 items 中收集相關的來源
    6. 返回最終的查詢集
    """
    if not items:
        return DailyTran.objects.none()

    # 驗證項目類型
    if not (isinstance(items.first(), (WatchlistItem, AbstractProduct))):
        raise AttributeError(f"Found not support type {items.first()}")

    # 收集農產品 ID
    product_ids = {item.product_id for item in items if isinstance(item, WatchlistItem)}
    product_ids.update({item.id for item in items if isinstance(item, AbstractProduct)})

    # 根據type和農產品 ID 建立基本查詢
    query = DailyTran.objects.filter(product__type=_type, product_id__in=product_ids)

    # 處理來源過濾
    if not sources:
        # 從 WatchlistItem 收集來源
        sources = {source for item in items if isinstance(item, WatchlistItem)
                   for source in item.sources.all()}
        # 從 AbstractProduct 收集來源
        sources.update({source for item in items if isinstance(item, AbstractProduct)
                        for source in item.sources()})

    if sources:
        query = query.filter(source__in=sources)

    return query


def get_group_by_date_query_set(query_set, start_date=None, end_date=None, specific_year=True):
    """
    按日期對查詢結果進行分組和聚合計算

    這個函數處理交易數據的時間序列分析，包括:
    1. 日期範圍過濾
    2. 數據完整性檢查
    3. 價格、交易量和重量的加權平均計算

    Args:
        query_set (QuerySet): 原始查詢集
        start_date (datetime.date, optional): 開始日期
        end_date (datetime.date, optional): 結束日期
        specific_year (bool): 是否按特定年份過濾
            - True: 只查詢指定年份的數據
            - False: 查詢跨年度的數據

    Returns:
        tuple: (DataFrame, bool, bool)
            - DataFrame: 包含聚合後的數據
                columns: ['date', 'avg_price', 'num_of_source', 'sum_volume', 'avg_avg_weight']
            - has_volume: 是否包含交易量數據
            - has_weight: 是否包含交易重量數據

    計算邏輯:
    1. 檢查數據完整性（volume 和 weight）
    2. 根據日期範圍過濾數據
    3. 計算加權平均價格和其他統計值
    4. 對缺失值進行處理
    """
    # 檢查交易量和重量數據的完整性
    has_volume = query_set.filter(volume__isnull=False).count() > (0.8 * query_set.count())
    has_weight = query_set.filter(avg_weight__isnull=False).count() > (0.8 * query_set.count())

    # 日期範圍過濾
    if isinstance(start_date, datetime.date) and isinstance(end_date, datetime.date):
        if specific_year:
            query_set = query_set.filter(date__range=[start_date, end_date])
        else:
            query_set = query_set.between_month_day_filter(start_date, end_date)

    # 空數據處理
    if not query_set:
        return pd.DataFrame(
            columns=['date', 'avg_price', 'num_of_source', 'sum_volume', 'avg_avg_weight']), False, False

    if has_volume and has_weight:
        query_set = query_set.filter(Q(volume__gt=0) & Q(avg_weight__gt=0))

    # 將查詢結果轉換為 DataFrame
    df = pd.DataFrame(list(query_set.values()))
    df = df[['product_id', 'date', 'avg_price', 'avg_weight', 'volume', 'source_id']]

    # 數據處理和計算
    # 將缺失值填充為 1，不用因為缺失值設定判斷式再進行計算
    df['vol_for_calculation'] = df['volume'].fillna(1)
    df['wt_for_calculation'] = df['avg_weight'].fillna(1)
    # 計算單日單一交易地點的總價格和總交易重量
    df['avg_price'] = df['avg_price'] * df['wt_for_calculation'] * df['vol_for_calculation']
    df['wt_for_calculation'] = df['wt_for_calculation'] * df['vol_for_calculation']

    # 根據來源市場 ID 分組計算
    if all(pd.isna(df['source_id'])):
        df['source_id'].fillna(1, inplace=True)

    # 按日期和來源市場 ID 分組
    group = df.groupby(['date', 'source_id'])
    df_fin = group.sum()
    df_fin['num_of_source'] = 1

    # 按日期分組
    group = df_fin.groupby('date')
    df_fin = group.sum()

    # 計算最終的平均價格和重量
    df_fin['avg_price'] = df_fin['avg_price'] / df_fin['wt_for_calculation']
    df_fin['avg_weight'] = df_fin['wt_for_calculation'] / df_fin['vol_for_calculation']
    df_fin.reset_index(inplace=True)
    df_fin = df_fin.sort_values('date')

    # 重命名列
    df_fin.rename(columns={'volume': 'sum_volume', 'avg_weight': 'avg_avg_weight'}, inplace=True)

    # 處理缺失的量和重量數據
    if not has_volume:
        df_fin['sum_volume'] = df_fin['num_of_source']
    if not has_weight:
        df_fin['avg_avg_weight'] = 1

    return df_fin[['date', 'avg_price', 'num_of_source', 'sum_volume', 'avg_avg_weight']], has_volume, has_weight


def get_daily_price_volume(_type, items, sources=None, start_date=None, end_date=None):
    """
    獲取每日價格和交易量數據，並生成適合前端展示的格式

    此函數整合每日交易數據，生成包含價格、交易量和重量的時間序列數據，並提供兩種數據格式：
    1. Highcharts 圖表格式
    2. 原始數據表格格式

    Args:
        _type (Type): 產品類型對象
        items (QuerySet): 產品項目集合
            - 可以是 WatchlistItem 或 AbstractProduct 對象的集合
        sources (QuerySet, optional): 資料來源集合
            - 如果為 None，則從 items 中自動獲取相關來源
        start_date (date, optional): 開始日期
        end_date (date, optional): 結束日期

    Returns:
        dict: 回傳包含以下鍵值的字典：
            - 'type': 產品類型資訊 (序列化後的數據)
            - 'highchart': 適用於 Highcharts 的數據格式
                {
                    'avg_price': [[timestamp, price], ...],
                    'sum_volume': [[timestamp, volume], ...] (可選),
                    'avg_weight': [[timestamp, weight], ...] (可選)
                }
            - 'raw': 原始數據表格格式
                {
                    'columns': [
                        {'value': '日期', 'format': 'date'},
                        {'value': '平均價格', 'format': 'avg_price'},
                        ...
                    ],
                    'rows': [[date, price, volume, weight], ...]
                }
            - 'no_data': 是否有數據的標記 (boolean)

    實作細節:
    1. 使用 get_query_set 獲取基礎查詢集
    2. 通過 get_group_by_date_query_set 進行數據聚合
    3. 生成時間序列並處理缺失值
    4. 轉換數據格式以符合前端需求
    """
    # 獲取並處理查詢數據
    query_set = get_query_set(_type, items, sources)
    q, has_volume, has_weight = get_group_by_date_query_set(query_set, start_date, end_date)

    # 檢查是否有數據
    if q.size == 0:
        return {'no_data': True}

    # 定義基本欄位
    columns = [
        {'value': _('Date'), 'format': 'date'},
        {'value': _('Average Price'), 'format': 'avg_price'}
    ]

    # 根據數據可用性添加額外欄位
    if has_volume:
        columns.append({'value': _('Sum Volume'), 'format': 'sum_volume'})
    if has_weight:
        columns.append({'value': _('Average Weight'), 'format': 'avg_avg_weight'})

    # 準備原始數據格式
    raw_data = {'columns': columns, 'rows': [[dic['date'], dic['avg_price']] for _, dic in q.iterrows()]}

    # 處理日期範圍和缺失值
    start_date = start_date or q['date'].iloc[0]
    end_date = end_date or q['date'].iloc[-1]
    diff = (end_date - start_date).days + 1
    date_list = pd.date_range(start_date, periods=diff, freq='D')

    # 重建索引以處理缺失值
    missing_point_data = q.set_index('date').reindex(date_list, fill_value=None)

    # 準備 Highcharts 數據格式
    highchart_data = {
        'avg_price': [[to_unix(date), price] for date, price in missing_point_data['avg_price'].items()]
    }

    # 根據數據可用性添加交易量和重量數據
    if has_volume:
        raw_data['rows'] = [[dic['date'], dic['avg_price'], dic['sum_volume']] for _, dic in q.iterrows()]
        highchart_data['sum_volume'] = [
            [to_unix(date), sum_volume] for date, sum_volume in missing_point_data['sum_volume'].items()
        ]
    if has_weight:
        raw_data['rows'] = [
            [dic['date'], dic['avg_price'], dic['sum_volume'], dic['avg_avg_weight']]
            for _, dic in q.iterrows()
        ]
        highchart_data['avg_weight'] = [
            [to_unix(date), weight] for date, weight in missing_point_data['avg_avg_weight'].items()
        ]

    # 準備回傳數據
    return {
        'type': TypeSerializer(_type).data,
        'highchart': highchart_data,
        'raw': raw_data,
        'no_data': len(highchart_data['avg_price']) == 0,
    }


def get_daily_price_by_year(_type, items, sources=None):
    """
    獲取按年份分組的每日價格數據，用於年度比較分析

    此函數將每日價格數據按年份組織，使數據可以按年進行比較。
    為了方便比較，所有年份的數據都會被調整到同一個年份(2016)的日期範圍內。

    Args:
        _type (Type): 產品類型對象
        items (Iterable): 產品項目集合
            - 可以是 WatchlistItem 或 AbstractProduct 對象的集合
        sources (Iterable[Source], optional): 資料來源集合
            - 如果為 None，則從 items 中自動獲取相關來源

    Returns:
        dict: 包含以下結構的字典：
            {
                'years': [(year, selected), ...] - 年份列表及其選擇狀態
                'type': {序列化的類型資訊}
                'price': {
                    'highchart': {
                        '2020': [[timestamp, price], ...],
                        '2019': [[timestamp, price], ...],
                        ...
                    },
                    'raw': {
                        'columns': [{column定義}, ...],
                        'rows': [[date, price2020, price2019, ...], ...]
                    }
                },
                'volume': {同上格式} (如果有交易量數據),
                'weight': {同上格式} (如果有重量數據),
                'no_data': boolean
            }

    Implementation Details:
    1. 內部輔助函數:
        - get_result(key): 處理單個數據類型(價格/交易量/重量)的結果生成
            - 生成 highchart 和原始數據兩種格式
            - 處理閏年特殊情況

    工作流程:
    1. 獲取基本查詢集
    2. 進行數據分組和聚合
    3. 調整數據到 2016 年的日期範圍
    4. 生成多種數據格式
    5. 處理特殊情況（如閏年）
    """

    def get_result(key):
        """
        為指定的數據類型生成結果字典

        Args:
            key (str): 數據類型標識符 ('avg_price', 'sum_volume', 或 'avg_avg_weight')

        Returns:
            dict: {
                'highchart': 按年份分組的數據點,
                'raw': 表格格式的原始數據
            }
        """
        # 初始化結果字典
        result = {str(year): [] for year in pd.to_datetime(q['date']).dt.year.unique()}
        if q.size == 0:
            return {
                'highchart': result,
                'raw': None
            }

        # 生成標準年份的日期範圍 (使用2016作為基準年)
        date_list = pd.date_range(datetime.date(2016, 1, 1), datetime.date(2016, 12, 31), freq='D')

        # 處理每一個數據點
        for i, dic in q.iterrows():
            # 將數據點的日期調整到2016年
            date = datetime.date(year=2016, month=dic['date'].month, day=dic['date'].day)
            result[str(dic['date'].year)].append(
                (to_unix(date), None if pd.isna(dic[key]) else dic[key]) if dic[key] else None
            )

            # 處理閏年特殊情況
            if not ((dic['date'].year % 4 == 0 and dic['date'].year % 100 != 0) or dic['date'].year % 400 == 0) and \
                    dic['date'].month == 2 and dic['date'].day == 28:
                result[str(dic['date'].year)].append((to_unix(datetime.date(2016, 2, 29)), None))

        # 創建和處理原始數據表格
        df = pd.DataFrame.from_dict(result, orient='columns')
        df = df.applymap(lambda x: x[1] if isinstance(x, tuple) else x)
        df.insert(0, 'date', date_list)

        # 移除全部為空的行
        row_empty = df.iloc[:, 1:].isna().all(axis=1)
        raw_data_rows_remove_empty = df[~row_empty]
        raw_data_rows_remove_empty.fillna('', inplace=True)
        raw_data_rows_remove_empty = raw_data_rows_remove_empty.values.tolist()

        # 準備表格列定義
        raw_data_columns = [{'value': _('Date'), 'format': 'date'}]
        raw_data_columns.extend([{'value': str(year), 'format': key} for year in years])

        return {
            'highchart': result,
            'raw': {
                'columns': raw_data_columns,
                'rows': raw_data_rows_remove_empty
            }
        }

    # 主函數邏輯開始
    query_set = get_query_set(_type, items, sources)
    q, has_volume, has_weight = get_group_by_date_query_set(query_set)

    # 處理空數據情況
    if q.size == 0:
        return {'no_data': True}

    # 獲取所有相關年份
    years = sorted({date.year for date in q['date']})

    # 判斷年份是否應該被選中
    def selected_year(y):
        this_year = datetime.date.today().year
        return this_year > y >= datetime.date.today().year - 5

    # 重建完整的日期範圍並填充缺失值
    q = q.set_index('date').reindex(
        pd.date_range(datetime.date(q['date'].min().year, 1, 1),
                      datetime.date(q['date'].max().year, 12, 31)),
        fill_value=None
    )
    q = q.reset_index().rename(columns={'index': 'date'})

    # 準備回傳數據
    response_data = {
        'years': [(year, selected_year(year)) for year in years],
        'type': TypeSerializer(_type).data,
        'price': get_result('avg_price'),
    }

    # 根據數據可用性添加交易量和重量數據
    if has_volume:
        response_data['volume'] = get_result('sum_volume')

    if has_weight:
        response_data['weight'] = get_result('avg_avg_weight')

    response_data['no_data'] = len(response_data['price']['highchart'].keys()) == 0

    return response_data


def annotate_avg_price(df, key):
    """
    計算加權平均價格

    此函數使用交易量和重量作為權重，計算分組後的加權平均價格。
    採用以下公式：
    加權平均價格 = Σ(價格 × 交易量 × 平均重量) / Σ(交易量 × 平均重量)

    Args:
        df (pd.DataFrame): 包含價格、交易量和重量數據的 DataFrame
            必需欄位:
            - avg_price: 平均價格
            - sum_volume: 交易量
            - avg_avg_weight: 平均重量
        key (str): 分組的鍵值
            例如：'month', 'year' 等

    Returns:
        pd.Series: 返回每個分組的加權平均價格

    計算步驟:
    1. 計算每筆交易的總價值（價格 × 交易量 × 重量）
    2. 計算每筆交易的權重（交易量 × 重量）
    3. 對每個分組進行加總
    4. 計算最終的加權平均價格
    """
    # 計算加權總交易價值
    df['weighted_avg_price'] = df['avg_price'] * df['sum_volume'] * df['avg_avg_weight']
    # 計算總交易重量
    df['weighted_sum_volume_weight'] = df['sum_volume'] * df['avg_avg_weight']

    # 按分組計算總和
    df['weighted_avg_price'] = df.groupby(key)['weighted_avg_price'].transform('sum')
    df['weighted_sum_volume_weight'] = df.groupby(key)['weighted_sum_volume_weight'].transform('sum')

    # 計算最終的加權平均價格
    df['monthly_avg_price'] = df['weighted_avg_price'] / df['weighted_sum_volume_weight']

    # 返回每個分組的第一個值（因為同一分組的值都相同）
    return df.groupby(key)['monthly_avg_price'].first()


def annotate_avg_weight(df, key):
    """
    計算加權平均重量

    此函數使用交易量作為權重，計算分組後的加權平均重量。
    採用以下公式：
    加權平均重量 = Σ(重量 × 交易量) / Σ(交易量)

    Args:
        df (pd.DataFrame): 包含重量和交易量數據的 DataFrame
            必需欄位:
            - sum_volume: 交易量
            - avg_avg_weight: 平均重量
        key (str): 分組的鍵值
            例如：'month', 'year' 等

    Returns:
        pd.Series: 返回每個分組的加權平均重量

    計算步驟:
    1. 計算每筆交易的總重量（重量 × 交易量）
    2. 按分組加總交易量和總重量
    3. 計算最終的加權平均重量
    """
    # 計算總交易重量
    df['weighted_avg_weight'] = df['sum_volume'] * df['avg_avg_weight']
    # 保存交易量
    df['weighted_sum_volume'] = df['sum_volume']

    # 按分組計算總和
    df['weighted_avg_weight'] = df.groupby(key)['weighted_avg_weight'].transform('sum')
    df['weighted_sum_volume'] = df.groupby(key)['weighted_sum_volume'].transform('sum')

    # 計算最終的加權平均重量
    df['avg_weight'] = df['weighted_avg_weight'] / df['weighted_sum_volume']

    # 返回每個分組的第一個值
    return df.groupby(key)['avg_weight'].first()


def get_monthly_price_distribution(_type, items, sources=None, selected_years=None):
    """
    計算並返回月度價格分布統計資料

    此函數分析價格的月度分布情況，計算各種統計指標（最小值、四分位數、中位數等），
    用於展示價格的波動範圍和分布特徵。

    Args:
        _type (Type): 產品類型對象
        items (Iterable): 產品項目集合
            - 可以是 WatchlistItem 或 AbstractProduct 對象的集合
        sources (Iterable[Source], optional): 資料來源集合
            - 若為 None，則從 items 中自動獲取相關來源
        selected_years (list[int], optional): 選擇的年份清單
            - 若提供，則只分析指定年份的數據
            - 若為 None，則分析所有可用年份的數據

    Returns:
        dict: 包含以下結構的字典：
            {
                'type': 產品類型資訊,
                'years': 可用年份列表,
                'price': {
                    'highchart': {
                        'perc_0': [[month, value], ...],    # 最小值
                        'perc_25': [[month, value], ...],   # 第一四分位數
                        'perc_50': [[month, value], ...],   # 中位數
                        'perc_75': [[month, value], ...],   # 第三四分位數
                        'perc_100': [[month, value], ...],  # 最大值
                        'mean': [[month, value], ...],      # 平均值
                        'years': [年份列表]
                    },
                    'raw': {
                        'columns': [列定義],
                        'rows': [數據行]
                    }
                },
                'volume': 相同格式 (如果有交易量數據),
                'weight': 相同格式 (如果有重量數據),
                'no_data': boolean
            }

    內部實現細節：
    1. get_result(key): 內部函數，處理單個指標（價格/交易量/重量）的統計分析
       - 計算各種百分位數
       - 計算平均值
       - 生成圖表和原始數據格式
    """

    def get_result(key):
        """
        為指定的數據類型生成分布統計結果

        Args:
            key (str): 數據類型 ('avg_price', 'sum_volume', 或 'avg_avg_weight')

        Returns:
            dict: {
                'highchart': 圖表格式的分布數據,
                'raw': 表格格式的原始數據
            }

        工作流程:
        1. 計算各百分位數
        2. 計算加權平均值（對於價格和重量）
        3. 生成圖表和原始數據格式
        """
        highchart_data = {}
        raw_data = {}

        if q.empty:
            return {
                'highchart': highchart_data,
                'raw': raw_data,
            }

        # 計算各百分位數
        s_quantile = q.groupby('month')[key].quantile([.0, .25, .5, .75, 1])

        # 根據數據類型選擇平均值計算方法
        if key == 'avg_price':
            s_mean = annotate_avg_price(q, 'month')
        elif key == 'avg_avg_weight':
            s_mean = annotate_avg_weight(q, 'month')
        else:
            s_mean = q.groupby('month')[key].mean()

        # 生成圖表數據格式
        highchart_data = {
            'perc_0': [[key[0], value] for key, value in s_quantile.iteritems() if key[1] == 0.0],
            'perc_25': [[key[0], value] for key, value in s_quantile.iteritems() if key[1] == 0.25],
            'perc_50': [[key[0], value] for key, value in s_quantile.iteritems() if key[1] == 0.5],
            'perc_75': [[key[0], value] for key, value in s_quantile.iteritems() if key[1] == 0.75],
            'perc_100': [[key[0], value] for key, value in s_quantile.iteritems() if key[1] == 1.0],
            'mean': [[key, value] for key, value in s_mean.iteritems()],
            'years': years
        }

        # 生成表格數據格式
        raw_data = {
            'columns': [
                {'value': _('Month'), 'format': 'integer'},
                {'value': _('Min'), 'format': key},
                {'value': _('25%'), 'format': key},
                {'value': _('50%'), 'format': key},
                {'value': _('75%'), 'format': key},
                {'value': _('Max'), 'format': key},
                {'value': _('Mean'), 'format': key},
            ],
            'rows': [
                [i,
                 s_quantile.loc[i][0.0],
                 s_quantile.loc[i][0.25],
                 s_quantile.loc[i][0.5],
                 s_quantile.loc[i][0.75],
                 s_quantile.loc[i][1],
                 s_mean.loc[i]] for i in s_quantile.index.levels[0]
            ]
        }

        return {
            'highchart': highchart_data,
            'raw': raw_data,
        }

    # 主函數邏輯
    query_set = get_query_set(_type, items, sources)
    years = sorted({date[0].year for date in query_set.values_list('date')})

    # 如果指定了年份，進行過濾
    if selected_years:
        query_set = query_set.filter(date__year__in=selected_years)

    # 獲取基本數據
    q, has_volume, has_weight = get_group_by_date_query_set(query_set)
    q['month'] = pd.to_datetime(q['date']).dt.month

    # 準備回傳數據
    response_data = {
        'type': TypeSerializer(_type).data,
        'years': years,
        'price': get_result('avg_price')
    }

    # 添加交易量和重量數據（如果有）
    if has_volume:
        response_data['volume'] = get_result('sum_volume')

    if has_weight:
        response_data['weight'] = get_result('avg_avg_weight')

    response_data['no_data'] = len(response_data['price']['highchart'].keys()) == 0

    return response_data


def get_integration(_type, items, start_date, end_date, sources=None, to_init=True):
    """
    整合分析特定時期的價格、交易量和重量數據

    此函數提供了複雜的時間序列數據整合，支持不同時期的數據比較分析，
    包括當期與去年同期比較，以及近五年的數據趨勢分析。

    Args:
        _type (Type): 產品類型對象
        items (Iterable): 產品項目集合
            - 可以是 WatchlistItem 或 AbstractProduct 對象
        start_date (datetime): 開始日期
        end_date (datetime): 結束日期
        sources (Iterable[Source], optional): 資料來源集合
            - 若為 None，則從 items 中自動獲取
        to_init (bool): 控制回傳數據的格式
            - True: 回傳初始化數據（本期、去年同期和五年數據）
            - False: 回傳年度比較數據

    Returns:
        dict: {
            'type': 產品類型資訊,
            'integration': [
                {
                    'name': 時期名稱,
                    'avg_price': 平均價格,
                    'sum_volume': 總交易量,
                    'avg_avg_weight': 平均重量,
                    'points': [...] 數據點列表,
                    'base': 是否為基準期間,
                    'order': 排序順序
                },
                ...
            ],
            'has_volume': 是否包含交易量,
            'has_weight': 是否包含重量,
            'no_data': 是否有數據
        }

    內部輔助函數:
    1. spark_point_maker: 生成數據點序列
    2. pandas_annotate_init: 計算初始化數據的統計值
    3. pandas_annotate_year: 計算年度比較的統計值
    4. generate_integration: 生成整合後的數據結構
    """

    def spark_point_maker(qs, add_unix=True):
        """
        生成資料點序列

        Args:
            qs (pd.DataFrame): 包含日期和值的 DataFrame
            add_unix (bool): 是否添加 unix 時間戳

        Returns:
            list: 數據點字典列表
        """
        points = qs.copy()
        if add_unix:
            points['unix'] = points['date'].apply(to_unix)
        return points.to_dict('record')

    def pandas_annotate_init(df):
        """
        計算初始化數據的統計值

        包括平均價格、交易量和重量的加權平均計算

        Args:
            df (pd.DataFrame): 原始數據框架

        Returns:
            dict: 包含各項統計值的字典
        """
        series = df.mean()
        result = series.T.to_dict()

        # 計算加權平均價格
        df['key'] = 'value'
        price = annotate_avg_price(df, 'key')
        result['avg_price'] = price['value']

        return result

    def pandas_annotate_year(qs, start_date=None, end_date=None):
        """
        計算年度比較的統計值

        處理跨年度數據的比較，支持多年度數據的整合分析

        Args:
            qs (pd.DataFrame): 原始數據框架
            start_date (datetime): 開始日期
            end_date (datetime): 結束日期

        Returns:
            list: 各年度統計結果列表
        """
        df = qs.copy()
        df['avg_price'] = df['avg_price'] * df['sum_volume'] * df['avg_avg_weight']
        df['sum_volume_weight'] = df['sum_volume'] * df['avg_avg_weight']

        # 處理跨年度的情況
        if start_date and end_date and start_date.year != end_date.year:
            df_list = []
            for i in range(1, start_date.year - df['date'].iloc[0].year + 1):
                # 計算每個年度區間的統計值
                split_df = df[
                    (df['date'] >= datetime.date(start_date.year - i, start_date.month, start_date.day)) \
                    & (df['date'] <= datetime.date(end_date.year - i, end_date.month, end_date.day)) \
                    ].mean()
                split_df['year'] = start_date.year - i
                split_df['end_year'] = end_date.year - i
                df_list.append(split_df)
            df = pd.concat(df_list, axis=1).T
        else:
            # 單年度的處理
            df['year'] = pd.to_datetime(df['date']).dt.year
            df = df.groupby(['year'], as_index=False).mean()

        # 計算最終的加權平均價格
        df['avg_price'] = df['avg_price'] / df['sum_volume_weight']
        result = df.T.to_dict().values()

        # 更新每年的加權平均價格
        price = annotate_avg_price(df, 'year')
        for dic in result:
            year = dic['year']
            if year in price:
                dic['avg_price'] = price[year]

        return result

    def generate_integration(query, start, end, specific_year, name, base, order):
        """
        生成整合數據結構

        Args:
            query (QuerySet): 查詢集
            start (datetime): 開始日期
            end (datetime): 結束日期
            specific_year (bool): 是否特定年份
            name (str): 時期名稱
            base (bool): 是否為基準期間
            order (int): 排序順序

        處理邏輯:
        1. 獲取指定時期的數據
        2. 計算統計值
        3. 生成數據點
        4. 組織數據結構
        """
        q, with_volume, with_weight = get_group_by_date_query_set(query,
                                                                  start_date=start,
                                                                  end_date=end,
                                                                  specific_year=specific_year)
        years = pd.to_datetime(q['date']).dt.year.unique()
        if q.empty or ('5' in name and len(years) < 5):
            return
        data = pandas_annotate_init(q)
        data['name'] = name
        data['points'] = spark_point_maker(q)
        data['base'] = base
        data['order'] = order
        integration.append(data)

    # 主函數邏輯開始
    query_set = get_query_set(_type, items, sources)
    diff = end_date - start_date + datetime.timedelta(1)
    last_start_date = start_date - diff
    last_end_date = end_date - diff
    integration = []

    if to_init:
        # 生成本期數據
        generate_integration(query_set, start_date, end_date, True, _('This Term'), True, 1)
        # 生成去年同期數據
        generate_integration(query_set, last_start_date, last_end_date, True, _('Last Term'), False, 2)

        # 生成五年數據
        start_date_fy = datetime.datetime(start_date.year - 5, start_date.month + 1, 1) \
            if (is_leap(start_date.year) and start_date.month == 2 and start_date.day == 29) \
            else datetime.datetime(start_date.year - 5, start_date.month, start_date.day)
        end_date_fy = datetime.datetime(end_date.year - 1, end_date.month, end_date.day - 1) \
            if (is_leap(end_date.year) and end_date.month == 2 and end_date.day == 29) \
            else datetime.datetime(end_date.year - 1, end_date.month, end_date.day)
        query_set_fy = query_set.filter(date__gte=start_date_fy, date__lte=end_date_fy)
        generate_integration(query_set_fy, start_date, end_date, False, _('5 Years'), False, 3)
    else:
        # 生成年度比較數據
        this_year = end_date.year
        query_set = query_set.filter(date__year__lt=this_year)
        q, has_volume, has_weight = get_group_by_date_query_set(query_set,
                                                                start_date=start_date,
                                                                end_date=end_date,
                                                                specific_year=False)
        if q.size > 0:
            data_all = pandas_annotate_year(q, start_date, end_date)
            if start_date.year == end_date.year:
                # 處理單年度數據
                for dic in data_all:
                    year = dic['year']
                    q_filter_by_year = q[pd.to_datetime(q['date']).dt.year == year].sort_values('date')
                    dic['name'] = '%0.0f' % year
                    dic['points'] = spark_point_maker(q_filter_by_year)
                    dic['base'] = False
                    dic['order'] = 4 + this_year - year
            else:
                # 處理跨年度數據
                for dic in data_all:
                    start_year = int(dic['year'])
                    end_year = int(dic['end_year'])
                    q_filter_by_year = q[(
                            (q['date'] >= datetime.date(start_year, start_date.month, start_date.day)) &
                            (q['date'] <= datetime.date(end_year, end_date.month, end_date.day))
                    )]
                    dic['name'] = '{}~{}'.format(start_year, end_year)
                    dic['points'] = spark_point_maker(q_filter_by_year)
                    dic['base'] = False
                    dic['order'] = 4 + this_year - start_year

            integration = list(data_all)
            integration.reverse()

    # 準備回傳數據
    response_data = {
        'type': TypeSerializer(_type).data,
        'integration': integration,
        'has_volume': False,
        'has_weight': False,
        'no_data': not integration,
    }

    if integration:
        response_data['has_volume'] = (integration[0]['sum_volume'] != integration[0]['num_of_source'])
        response_data['has_weight'] = (integration[0]['avg_avg_weight'] != 1)

    return response_data


class Day(Func):
    function = 'EXTRACT'
    template = '%(function)s(DAY from %(expressions)s)'
    output_field = IntegerField()


class Month(Func):
    function = 'EXTRACT'
    template = '%(function)s(MONTH from %(expressions)s)'
    output_field = IntegerField()


class Year(Func):
    function = 'EXTRACT'
    template = '%(function)s(YEAR from %(expressions)s)'
    output_field = IntegerField()


class RawAnnotation(RawSQL):
    """
    RawSQL also aggregates the SQL to the `group by` clause which defeats the purpose of adding it to an Annotation.
    See: https://medium.com/squad-engineering/hack-django-orm-to-calculate-median-and-percentiles-or-make-annotations-great-again-23d24c62a7d0
    """

    def get_group_by_cols(self):
        return []


# transfer datetime to unix to_unix format
def to_unix(date):
    return int(time.mktime(date.timetuple()) * 1000)


def to_date(number):
    return datetime.datetime.fromtimestamp(float(number) / 1000.0)
