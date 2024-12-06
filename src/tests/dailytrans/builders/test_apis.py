import datetime as dt
from datetime import datetime
from unittest.mock import patch, MagicMock
from urllib.parse import urlparse, parse_qs

import pandas as pd
import pytest
from pandas import Series

from apps.configs.models import Config, Source, AbstractProduct, Type
from apps.crops.models import Crop
from apps.dailytrans.builders.apis import Api as OriginApi
from apps.dailytrans.builders.utils import DirectData
from apps.dailytrans.models import DailyTran
from apps.fruits.models import Fruit


@pytest.fixture
def mock_daily_trans_data_frame():
    return pd.DataFrame(
        [{'avg_price_x': 50.0,
          'source__name': '竹崎地區農會',
          'date': dt.date(2024, 10, 31),
          'product__code': '椪柑',
          'avg_price_y': 50.0,
          'id': 15755202}]
    )


@pytest.mark.django_db
class TestOriginApis:
    def test_instance(self, mock_crops_origin_api: OriginApi, mock_fruits_origin_api: OriginApi):
        # Case1: crops
        # Arrange
        data = DirectData('COG05', 2, 'LOT-crops')
        _type = Type.objects.get(id=data.type_id)
        config = Config.objects.get(code=data.config_code)
        sources = Source.objects.filter(configs__exact=config).filter(type__id=_type.id)
        products = AbstractProduct.objects.filter(type__id=_type.id, track_item=True, config=config)

        # Assert
        assert mock_crops_origin_api.MODEL == Crop
        assert mock_crops_origin_api.TYPE == _type
        assert mock_crops_origin_api.CONFIG == config
        assert mock_crops_origin_api.SOURCE_QS.count() == sources.count()
        assert mock_crops_origin_api.PRODUCT_QS.count() == products.count()

        # Case2: fruits
        # Arrange
        data = DirectData('COG06', 2, 'LOT-fruits')
        _type = Type.objects.get(id=data.type_id)
        config = Config.objects.get(code=data.config_code)
        sources = Source.objects.filter(configs__exact=config).filter(type__id=_type.id)
        products = AbstractProduct.objects.filter(type__id=_type.id, track_item=True, config=config)

        # Assert
        assert mock_fruits_origin_api.MODEL == Fruit
        assert mock_fruits_origin_api.TYPE == _type
        assert mock_fruits_origin_api.CONFIG == config
        assert mock_fruits_origin_api.SOURCE_QS.count() == sources.count()
        assert mock_fruits_origin_api.PRODUCT_QS.count() == products.count()

    def test_sources_property(self, mock_crops_origin_api: OriginApi):
        # Arrange
        orig_sources = mock_crops_origin_api.SOURCE_QS

        # Act
        sources = mock_crops_origin_api.sources

        # Assert
        assert sources.count() == orig_sources.count()

    def test_products_property(self, mock_crops_origin_api: OriginApi):
        # Arrange
        orig_products = mock_crops_origin_api.PRODUCT_QS

        # Act
        products = mock_crops_origin_api.products

        # Assert
        assert products.count() == orig_products.count()

    def test_get_formatted_url(self, mock_crops_origin_api: OriginApi):
        # Arrange
        parsed_orig_url = urlparse(mock_crops_origin_api.API_URL)
        start_date = datetime.strptime('2024-11-25', '%Y-%m-%d').date()
        end_date = datetime.strptime('2024-11-25', '%Y-%m-%d').date()

        # Act
        url = mock_crops_origin_api._get_formatted_url(start_date, end_date)
        parsed_url = urlparse(url)
        params = parse_qs(parsed_url.query)

        # Assert
        assert parsed_url.hostname == parsed_orig_url.hostname
        assert parsed_url.path == parsed_orig_url.path
        assert params['status'][0] == '4'
        assert params['startYear'][0] == str(start_date.year)
        assert params['endYear'][0] == str(end_date.year)
        assert params['startMonth'][0] == str(start_date.month)
        assert params['endMonth'][0] == str(end_date.month)
        assert params['startDay'][0] == str(start_date.day)
        assert params['endDay'][0] == str(end_date.day)
        assert params.get('productName') is None

    def test_get_urls(self, mock_crops_origin_api: OriginApi, mock_fruits_origin_api: OriginApi):
        # Arrange
        start_date = datetime.strptime('2024-11-25', '%Y-%m-%d').date()
        end_date = datetime.strptime('2024-11-25', '%Y-%m-%d').date()
        formated_url = mock_crops_origin_api._get_formatted_url(start_date, end_date)

        # Case1: crops
        # Act
        urls = mock_crops_origin_api._get_urls(formated_url)
        params1 = parse_qs(urlparse(urls[0]).query)
        params2 = parse_qs(urlparse(urls[1]).query)

        # Assert
        assert len(urls) == 2
        assert params1['status'][0] == '4'
        assert params1.get('productName') is None
        assert params2['status'][0] == '7'
        assert params2['productName'][0] == '蒜頭(蒜球)'

        # Case2: fruits
        # Arrange
        formated_url = mock_fruits_origin_api._get_formatted_url(start_date, end_date)

        # Act
        urls = mock_fruits_origin_api._get_urls(formated_url)
        params2 = parse_qs(urlparse(urls[1]).query)

        # Assert
        assert len(urls) == 2
        assert params2['status'][0] == '6'
        assert params2['productName'][0].find('青香蕉') != -1

    def test_access_garlic_data_from_api(self, crops_origin_api, mock_crops_origin_api: OriginApi):
        # Arrange
        garlic_data = crops_origin_api['garlic']['DATASET']

        # Act
        result = [mock_crops_origin_api._access_garlic_data_from_api(i) for i in garlic_data]

        # Assert
        assert len(result) == len(garlic_data)
        assert len(result[0].keys()) == 4
        assert {d['PERIOD'] for d in result} == {'2024/11/5', '2024/11/15'}
        assert {d['PRODUCTNAME'] for d in result} == {'蒜頭(蒜球)(旬價)'}

    @patch('requests.Response')
    def test_handle_response(
            self,
            mock_response,
            crops_origin_api,
            mock_crops_origin_api: OriginApi,
            mock_fruits_origin_api: OriginApi
    ):
        # Arrange
        start_date = datetime.strptime('2024-11-25', '%Y-%m-%d').date()
        end_date = datetime.strptime('2024-11-25', '%Y-%m-%d').date()
        formated_url = mock_crops_origin_api._get_formatted_url(start_date, end_date)
        data_list = crops_origin_api['normal']['DATASET']
        mock_response.json.return_value = crops_origin_api['normal']
        mock_response.url = formated_url

        # Case1: normal
        # Act
        result = mock_crops_origin_api._handle_response(mock_response)

        # Assert
        assert len(result) == len(data_list)
        assert result[0] == data_list[0]

        # Case2: garlic
        # Arrange
        garlic = AbstractProduct.objects.get(id=49012)
        urls = mock_crops_origin_api._get_urls(formated_url)
        data_list = crops_origin_api['garlic']['DATASET']
        mock_response.json.return_value = crops_origin_api['garlic']
        mock_response.url = urls[1]

        # Act
        result = mock_crops_origin_api._handle_response(mock_response)

        # Assert
        assert len(result) == len(data_list)
        assert "PERIOD" in result[0].keys()
        assert result[0]['PRODUCTNAME'] == garlic.code

        # Case3: banana
        # Arrange
        banana = Fruit.objects.get(id=59019)
        urls = mock_fruits_origin_api._get_urls(formated_url)
        data_list = crops_origin_api['banana']['DATASET']
        mock_response.json.return_value = crops_origin_api['banana']
        mock_response.url = urls[1]

        # Act
        result = mock_fruits_origin_api._handle_response(mock_response)

        # Assert
        assert len(result) == len(data_list)
        assert "PERIOD" in result[0].keys()
        assert result[0]['PERIOD'] == '2024/10/31'
        assert result[0]['PRODUCTNAME'] == banana.code

    @staticmethod
    def get_data_set(resp, api: OriginApi, data, daily_trans: str = None):
        start_date = datetime.strptime('2024-11-25', '%Y-%m-%d').date()
        end_date = datetime.strptime('2024-11-25', '%Y-%m-%d').date()
        formated_url = api._get_formatted_url(start_date, end_date)
        urls = api._get_urls(formated_url)

        data_list = []
        resp.json.return_value = data[daily_trans] if daily_trans else data['normal']
        resp.url = urls[0]
        data_list.extend(api._handle_response(resp))
        resp.json.return_value = data['garlic' if api.CONFIG.code == 'COG05' else 'banana']
        resp.url = urls[1]
        data_list.extend(api._handle_response(resp))

        return data_list

    @patch('requests.Response')
    def test_convert_to_data_frame(
            self,
            mock_response,
            crops_origin_api,
            mock_crops_origin_api: OriginApi,
            mock_fruits_origin_api: OriginApi
    ):
        # Arrange
        # Case1: crops
        data_list = self.get_data_set(mock_response, mock_crops_origin_api, crops_origin_api)

        # Act
        result = mock_crops_origin_api._convert_to_data_frame(data_list)

        # Assert
        assert result.empty is False
        assert set(result.columns.tolist()) == {'AVGPRICE', 'ORGNAME', 'PERIOD', 'PRODUCTNAME'}
        assert set(result.isna().all().tolist()) == {False}

        # Case2: fruits
        data_list = self.get_data_set(mock_response, mock_fruits_origin_api, crops_origin_api)

        # Act
        result = mock_fruits_origin_api._convert_to_data_frame(data_list)

        # Assert
        assert result.empty is False
        assert set(result.columns.tolist()) == {'AVGPRICE', 'ORGNAME', 'PERIOD', 'PRODUCTNAME'}
        assert set(result.isna().all().tolist()) == {False}

    @patch('requests.Response')
    def test_compare_data_from_api_and_db_with_db_data_not_exist(
            self,
            mock_response,
            crops_origin_api,
            mock_crops_origin_api: OriginApi
    ):
        # Arrange
        data_list = self.get_data_set(mock_response, mock_crops_origin_api, crops_origin_api)
        df = mock_crops_origin_api._convert_to_data_frame(data_list)

        # Act
        result = mock_crops_origin_api._compare_data_from_api_and_db(df)

        # Assert
        assert {'avg_price_x', 'source__name', 'date', 'product__code'}.issubset(set(result.columns.tolist()))
        assert type(result.date[0]) == dt.date
        assert result.avg_price_x.isna().all() == False
        assert result.id.isna().all() == True
        assert result.avg_price_y.isna().all() == True

    @patch('requests.Response')
    def test_compare_data_from_api_and_db_with_db_data_existed(
            self,
            mock_response,
            crops_origin_api,
            mock_crops_origin_api: OriginApi,
            mock_fruits_origin_api: OriginApi,
            load_daily_trans_origin_crops_fixtures
    ):
        # Arrange
        data_list = self.get_data_set(mock_response, mock_fruits_origin_api, crops_origin_api, '20241031')
        df = mock_fruits_origin_api._convert_to_data_frame(data_list)

        # Act
        result = mock_fruits_origin_api._compare_data_from_api_and_db(df)

        # Assert
        assert result.empty == False
        assert result.id.any() == True
        assert result.avg_price_y.any() == True

    def test_update_data(
            self,
            mock_crops_origin_api: OriginApi,
            mock_fruits_origin_api: OriginApi,
            load_daily_trans_origin_crops_fixtures,
            mock_daily_trans_data_frame
    ):
        # Arrange
        series: Series = mock_daily_trans_data_frame.iloc[0]
        series.avg_price_x = 999.5
        daily_trans = DailyTran.objects.get(id=series.id)

        # Act
        mock_fruits_origin_api._update_data(series, daily_trans)

        # Assert
        assert daily_trans.update_time is not None
        assert daily_trans.avg_price == series.avg_price_x

    def test_save_new_data(
            self,
            mock_crops_origin_api: OriginApi,
            mock_fruits_origin_api: OriginApi,
            mock_daily_trans_data_frame
    ):
        # Arrange
        series: Series = mock_daily_trans_data_frame.iloc[0]

        # Act
        mock_fruits_origin_api._save_new_data(series)
        daily_trans = DailyTran.objects.filter(date=series.date)
        daily_trans_obj: DailyTran = daily_trans[0]

        # Assert
        assert len(daily_trans) > 0
        assert daily_trans_obj.avg_price == series.avg_price_x
        assert daily_trans_obj.product.code == series.product__code
        assert daily_trans_obj.source.name == series.source__name
        assert daily_trans_obj.date == series.date

    @patch('apps.dailytrans.builders.apis.Api._update_data')
    @patch('apps.dailytrans.builders.apis.Api._save_new_data')
    @patch(
        'apps.dailytrans.builders.apis.DailyTran',
        new_callable=MagicMock,
        spec=DailyTran
    )
    def test_access_data_from_api_with_no_data_change(
            self,
            mock_daily_trans,
            mock_save_new_data,
            mock_update_data,
            mock_crops_origin_api: OriginApi,
            mock_fruits_origin_api: OriginApi,
            mock_daily_trans_data_frame
    ):
        # Arrange
        df = mock_daily_trans_data_frame
        data = DirectData('COG06', 2, 'LOT-fruits')
        api = OriginApi(model=Fruit, **data._asdict())
        api._compare_data_from_api_and_db = MagicMock()
        api._compare_data_from_api_and_db.return_value = df

        # Act
        api._access_data_from_api(df)

        # Assert
        api._compare_data_from_api_and_db.assert_called_once_with(df)
        mock_daily_trans.objects.get.assert_not_called()
        mock_save_new_data.assert_not_called()
        mock_update_data.assert_not_called()

    @patch('apps.dailytrans.builders.apis.Api._update_data')
    @patch('apps.dailytrans.builders.apis.Api._save_new_data')
    @patch(
        'apps.dailytrans.builders.apis.DailyTran',
        new_callable=MagicMock,
        spec=DailyTran
    )
    def test_access_data_from_api_with_data_changed(
            self,
            mock_daily_trans,
            mock_save_new_data,
            mock_update_data,
            mock_crops_origin_api: OriginApi,
            mock_fruits_origin_api: OriginApi,
            load_daily_trans_origin_crops_fixtures,
            mock_daily_trans_data_frame
    ):
        # Arrange
        df = mock_daily_trans_data_frame.assign(avg_price_x=999.5)
        data = DirectData('COG06', 2, 'LOT-fruits')
        api = OriginApi(model=Fruit, **data._asdict())
        api._compare_data_from_api_and_db = MagicMock()
        api._compare_data_from_api_and_db.return_value = df
        daily_trans = DailyTran.objects.get(id=df.iloc[0].id)
        mock_daily_trans.objects.get.return_value = daily_trans

        # Act
        api._access_data_from_api(df)

        # Assert
        api._compare_data_from_api_and_db.assert_called_once_with(df)
        mock_daily_trans.objects.get.assert_called_once_with(id=df.iloc[0].id)
        mock_update_data.assert_called_once()
        mock_save_new_data.assert_not_called()

    @patch('apps.dailytrans.builders.apis.Api._update_data')
    @patch('apps.dailytrans.builders.apis.Api._save_new_data')
    @patch(
        'apps.dailytrans.builders.apis.DailyTran',
        new_callable=MagicMock,
        spec=DailyTran
    )
    def test_access_data_from_api_with_new_data(
            self,
            mock_daily_trans,
            mock_save_new_data,
            mock_update_data,
            mock_crops_origin_api: OriginApi,
            mock_fruits_origin_api: OriginApi,
            mock_daily_trans_data_frame
    ):
        # Arrange
        df = mock_daily_trans_data_frame.assign(avg_price_x=999.5)
        data = DirectData('COG06', 2, 'LOT-fruits')
        api = OriginApi(model=Fruit, **data._asdict())
        api._compare_data_from_api_and_db = MagicMock()
        api._compare_data_from_api_and_db.return_value = df
        mock_daily_trans.objects.get.side_effect = DailyTran.DoesNotExist

        # Act
        api._access_data_from_api(df)

        # Assert
        api._compare_data_from_api_and_db.assert_called_once_with(df)
        mock_daily_trans.objects.get.assert_called_once()
        mock_update_data.assert_not_called()
        mock_save_new_data.assert_called_once()

    @patch('apps.dailytrans.builders.apis.Api._update_data')
    @patch('apps.dailytrans.builders.apis.Api._save_new_data')
    @patch(
        'apps.dailytrans.builders.apis.DailyTran',
        new_callable=MagicMock,
        spec=DailyTran
    )
    def test_access_data_from_api_with_data_to_delete(
            self,
            mock_daily_trans,
            mock_save_new_data,
            mock_update_data,
            mock_crops_origin_api: OriginApi,
            mock_fruits_origin_api: OriginApi,
            load_daily_trans_origin_crops_fixtures,
            mock_daily_trans_data_frame
    ):
        # Arrange
        df = mock_daily_trans_data_frame.assign(avg_price_x='')
        data = DirectData('COG06', 2, 'LOT-fruits')
        api = OriginApi(model=Fruit, **data._asdict())
        api._compare_data_from_api_and_db = MagicMock()
        api._compare_data_from_api_and_db.return_value = df
        daily_trans = DailyTran.objects.get(id=df.iloc[0].id)
        daily_trans.delete = MagicMock()
        mock_daily_trans.objects.get.return_value = daily_trans

        # Act
        api._access_data_from_api(df)

        # Assert
        api._compare_data_from_api_and_db.assert_called_once_with(df)
        mock_daily_trans.objects.get.assert_called_once_with(id=df.iloc[0].id)
        mock_update_data.assert_not_called()
        daily_trans.delete.assert_called_once()
        mock_save_new_data.assert_not_called()

    @patch('requests.Response')
    def test_access_data_from_api_with_real_data(
            self,
            mock_response,
            crops_origin_api,
            mock_crops_origin_api: OriginApi,
    ):
        # Arrange
        data_list = self.get_data_set(mock_response, mock_crops_origin_api, crops_origin_api, 'all_crops')
        df = mock_crops_origin_api._convert_to_data_frame(data_list)

        # Act
        mock_crops_origin_api._access_data_from_api(df)
        qs = DailyTran.objects.all()

        # Assert
        assert qs.count() == len(df)
