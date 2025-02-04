import datetime
from unittest.mock import patch, MagicMock

import pytest
from bs4 import BeautifulSoup
from requests import Response, Request

from apps.dailytrans.builders.eir032 import HTMLParser, ScrapperApi
from apps.dailytrans.builders.utils import DirectData
from apps.seafoods.models import Seafood


@pytest.fixture
def mock_html_page():
    with open('./data/mock_html.txt', encoding='utf8') as f:
        return f.read()


@pytest.mark.django_db
class TestHTMLParser:

    @patch('apps.configs.models.Type.objects', new_callable=MagicMock)
    @patch('apps.configs.models.Source.objects', new_callable=MagicMock)
    @patch('apps.configs.models.Config.objects', new_callable=MagicMock)
    @patch('apps.seafoods.models.Seafood.objects', new_callable=MagicMock)
    def get_api_obj(
            self,
            mock_seafood_model,
            mock_config_model,
            mock_source_model,
            mock_type_model
    ):
        data = DirectData('COG13', 1, 'LOT-seafoods')

        return ScrapperApi(model=Seafood, **data._asdict())

    @patch('requests.Response', spec=Response)
    def test_params_property(
            self, mock_response: MagicMock, mock_html_page: str):
        # Arrange
        api = self.get_api_obj()
        dt = datetime.datetime.strptime('2025-02-03', '%Y-%m-%d')
        api.query_date = dt
        params_dict = api.params_list[0].items()
        mock_params = '&'.join([f'{k}={v}' for k, v in params_dict])
        req = Request(url=f'{api.URL}?{mock_params}')
        mock_response.text = mock_html_page
        mock_response.request = req
        parser = HTMLParser(mock_response, api)

        # Act
        params = parser.params

        # Assert
        assert params['dateStr'][0] == api.request_date_str
        assert params['mid'][0] == list(api.SOURCES.keys())[0]

    @patch('requests.Response', spec=Response)
    def test_table_property(self, mock_response: MagicMock, mock_html_page: str):
        # Arrange
        api = self.get_api_obj()
        mock_response.text = mock_html_page
        parser = HTMLParser(mock_response, api)

        # Case 1: The table is found
        expected_result = parser.soup.find('table', {'id': parser.TABLE_ID})

        # Act
        table = parser.table

        # Assert
        assert table is not None
        assert table == expected_result
        assert table.text.find('魚貨名稱') != -1

        # Case 2: The table is not found
        parser.soup = BeautifulSoup('<html></html>', 'html.parser')

        # Act
        table = parser.table

        # Assert
        assert table is None

    @patch('requests.Response', spec=Response)
    def test_tbody_property(self, mock_response: MagicMock, mock_html_page: str):
        # Arrange
        api = self.get_api_obj()
        mock_response.text = mock_html_page
        parser = HTMLParser(mock_response, api)

        # Case 1: The tbody is found
        expected_result = parser.table.find('tbody')

        # Act
        tbody = parser.tbody

        # Assert
        assert tbody is not None
        assert tbody == expected_result
        assert tbody.text.find('金目鱸') != -1

        # Case 2: The table is not found
        parser.soup = BeautifulSoup('<html></html>', 'html.parser')

        # Act
        tbody = parser.table

        # Assert
        assert tbody is None

    @patch('requests.Response', spec=Response)
    def test_all_th_property(self, mock_response: MagicMock, mock_html_page: str):
        # Arrange
        api = self.get_api_obj()
        mock_response.text = mock_html_page
        parser = HTMLParser(mock_response, api)

        # Case 1: The tbody is found
        expected_result = parser.table.find_all('th')

        # Act
        th = parser.all_th

        # Assert
        assert len(th) != 0
        assert len(th) == len(expected_result)
        assert th[0].text.strip() == '品種代碼'
        assert th[1].text.strip() == '魚貨名稱'

        # Case 2: The table is not found
        parser.soup = BeautifulSoup('<html></html>', 'html.parser')

        # Act
        th = parser.all_th

        # Assert
        assert th is None


class TestScrapperApi:
    ...
