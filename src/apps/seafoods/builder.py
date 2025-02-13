import datetime
import time
from typing import Union

from apps.dailytrans.builders.efish import Api as OriginApi
from apps.dailytrans.builders.eir032 import Api as WholeSaleApi
from apps.dailytrans.builders.eir032 import ScrapperApi
from apps.dailytrans.builders.utils import (
    product_generator,
    director,
    date_generator,
    DirectData,
)
from .models import Seafood

MODELS = [Seafood]
CONFIG_CODE = 'COG13'
WHOLESALE_DELTA_DAYS = 30
ORIGIN_DELTA_DAYS = 365
LOGGER_TYPE_CODE = 'LOT-seafoods'


@director
def direct(*args, **kwargs):
    direct_generic_wholesale(*args, **kwargs)
    direct_origin(*args, **kwargs)


@director
def direct_generic_wholesale(
        start_date: Union[datetime.datetime, str], end_date: Union[datetime.datetime, str], *args, **kwargs
):
    """
    此 function 為使用 API 或爬蟲的方式取得批發價格，目前的策略為若日期區間大於 30 天，則使用爬蟲方式取得資料，否則使用 API 取得資料
    (因 API 短期區間內的價格比較不會有不同步的問題)。
    """

    data = DirectData(CONFIG_CODE, 1, LOGGER_TYPE_CODE)

    for model in MODELS:
        wholesale_api = WholeSaleApi(model=model, **data._asdict())
        scrapping_api = ScrapperApi(model=model, **data._asdict())
        date_diff = end_date - start_date

        for delta in range(date_diff.days + 1):
            if date_diff.days >= 30:
                responses = scrapping_api.requests(start_date=start_date + datetime.timedelta(days=delta))

                # if all requests failed, try to use API to fetch the data
                if all(resp.status_code != 200 for resp in responses):
                    scrapping_api.LOGGER.warning(
                        'All requests failed, try to use API to fetch the data', extra=scrapping_api.LOGGER_EXTRA
                    )

                    response = wholesale_api.request(
                        start_date=start_date + datetime.timedelta(days=delta),
                        end_date=start_date + datetime.timedelta(days=delta)
                    )
                    wholesale_api.load(response)
                else:
                    scrapping_api.loads(responses)

                    # prevent from being blocked by the server
                    time.sleep(5)
            else:
                response = wholesale_api.request(
                    start_date=start_date + datetime.timedelta(days=delta),
                    end_date=start_date + datetime.timedelta(days=delta)
                )
                wholesale_api.load(response)

    return data


@director
def direct_wholesale(start_date=None, end_date=None, *args, **kwargs):
    data = DirectData(CONFIG_CODE, 1, LOGGER_TYPE_CODE)

    for model in MODELS:
        wholesale_api = WholeSaleApi(model=model, **data._asdict())
        date_diff = end_date - start_date

        for delta in range(date_diff.days + 1):
            response = wholesale_api.request(start_date=start_date + datetime.timedelta(days=delta),
                                             end_date=start_date + datetime.timedelta(days=delta))
            wholesale_api.load(response)

    return data


@director
def direct_wholesale_by_scrapping(start_date=None, end_date=None, *args, **kwargs):
    """
    此 function 為使用爬蟲方式取得批發價格，主要用於手動更新年份較舊的資料。
    """

    data = DirectData(CONFIG_CODE, 1, LOGGER_TYPE_CODE)

    for model in MODELS:
        wholesale_api = ScrapperApi(model=model, **data._asdict())
        date_diff = end_date - start_date

        for delta in range(date_diff.days + 1):
            responses = wholesale_api.requests(start_date=start_date + datetime.timedelta(days=delta))
            wholesale_api.loads(responses)

            # prevent from being blocked by the server
            time.sleep(5)

    return data


@director
def direct_origin(start_date=None, end_date=None, *args, **kwargs):
    data = DirectData(CONFIG_CODE, 2, LOGGER_TYPE_CODE)

    for model in MODELS:
        origin_api = OriginApi(model=model, **data._asdict())

        for obj in product_generator(model, type=2, **kwargs):
            for delta_start_date, delta_end_date in date_generator(start_date, end_date, ORIGIN_DELTA_DAYS):
                response = origin_api.request(start_date=delta_start_date, end_date=delta_end_date, code=obj.code)
                origin_api.load(response)
                # sleep for that poor api service...
                time.sleep(10)

    return data
