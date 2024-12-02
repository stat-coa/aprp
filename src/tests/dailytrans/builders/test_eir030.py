import pandas as pd
import pytest

from datetime import date
from apps.configs.models import Config, Source, AbstractProduct
from apps.crops.models import Crop
from apps.dailytrans.models import DailyTran
from apps.crops.builder import WholeSaleApi05


@pytest.mark.django_db
class TestCropsEir030:
    @staticmethod
    def _get_result(crops_wholesale05_api, mock_wholesale_api05) -> pd.DataFrame:
        data = pd.DataFrame(
            crops_wholesale05_api,
            columns=[
                '上價',
                '中價',
                '下價',
                '平均價',
                '交易量',
                '交易日期',
                '作物代號',
                '市場名稱',
                '種類代碼',
            ],
        )
        data = data[data['作物代號'].isin(mock_wholesale_api05.target_items)]
        return mock_wholesale_api05._compare_data_from_api_and_db(data)

    def test_instance(self, mock_wholesale_api05):
        # Arrange
        config = Config.objects.get(code='COG05')
        sources = Source.objects.filter(configs__exact=config).filter(type__id=1)
        products = AbstractProduct.objects.filter(type__id=1, track_item=True)

        # Assert
        assert mock_wholesale_api05.MODEL == Crop
        assert mock_wholesale_api05.CONFIG == config
        assert mock_wholesale_api05.SOURCE_QS.count() == sources.count()
        assert mock_wholesale_api05.PRODUCT_QS.count() == products.count()

    def test_compare_data_from_api_and_db_with_daily_trans_existed(
            self, mock_wholesale_api05: WholeSaleApi05, crops_wholesale05_api
    ):
        # Act
        df_result = self._get_result(crops_wholesale05_api, mock_wholesale_api05)

        # Assert
        assert type(df_result.date[0]) == date
        assert df_result.date[0].isoformat() == '2024-11-25'
        assert df_result.query('id.notnull()').shape[0] == DailyTran.objects.filter(date=df_result.date[0]).count()

    def test_compare_data_from_api_and_db_with_daily_trans_not_exist(
            self, mock_wholesale_api05: WholeSaleApi05, crops_wholesale05_api
    ):
        # Arrange
        DailyTran.objects.filter(date='2024-11-25').delete()

        # Act
        df_result = self._get_result(crops_wholesale05_api, mock_wholesale_api05)

        # Assert
        assert df_result.query('id.notnull()').shape[0] == 0
        assert df_result.query('date.notnull()').shape[0] == len(crops_wholesale05_api)
