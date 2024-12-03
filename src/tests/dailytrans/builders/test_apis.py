from datetime import datetime
from unittest.mock import patch
from urllib.parse import urlparse, parse_qs

import pytest

from apps.configs.models import Config, Source, AbstractProduct, Type
from apps.crops.models import Crop
from apps.dailytrans.builders.apis import Api as OriginApi
from apps.dailytrans.builders.utils import DirectData
from apps.fruits.models import Fruit


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
        urls = mock_crops_origin_api._get_urls(formated_url)
        data_list = crops_origin_api['garlic']['DATASET']
        mock_response.json.return_value = crops_origin_api['garlic']
        mock_response.url = urls[1]

        # Act
        result = mock_crops_origin_api._handle_response(mock_response)

        # Assert
        assert len(result) == len(data_list)
        assert "PERIOD" in result[0].keys()

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
        assert result[0]['PERIOD'] == '2024/11/15'
        assert result[0]['PRODUCTNAME'] == banana.code
