import datetime
from typing import Tuple
from unittest.mock import patch, MagicMock

import pytest
from bs4 import BeautifulSoup
from bs4.element import ResultSet
from requests import Response, Request

from apps.dailytrans.builders.eir032 import HTMLParser, ScrapperApi
from apps.dailytrans.builders.utils import DirectData
from apps.seafoods.models import Seafood


@pytest.fixture
def mock_html_page():
    with open('./data/mock_html.txt', encoding='utf8') as f:
        return f.read()


@pytest.fixture
def mock_no_data_html_page():
    with open('./data/mock_html_with_no_data.txt', encoding='utf8') as f:
        return f.read()


@pytest.fixture
def mock_raw_data():
    return {
        '品種代碼': '1071',
        '魚貨名稱': '金目鱸',
        '上價(元/公斤)': '144.2',
        '中價(元/公斤)': '101.7',
        '下價(元/公斤)': '0',
        '交易量(公斤)': '3,969.3',
        '交易量漲跌幅+(-)%': '',
        '平均價(元/公斤)': '105.4',
        '平均價漲跌幅+(-)%': ''
    }


@pytest.fixture
@patch('apps.configs.models.Type.objects', new_callable=MagicMock)
@patch('apps.configs.models.Source.objects', new_callable=MagicMock)
@patch('apps.configs.models.Config.objects', new_callable=MagicMock)
@patch('apps.seafoods.models.Seafood.objects', new_callable=MagicMock)
def mock_api_obj(
        mock_seafood_model,
        mock_config_model,
        mock_source_model,
        mock_type_model
):
    data = DirectData('COG13', 1, 'LOT-seafoods')

    return ScrapperApi(model=Seafood, **data._asdict())

@pytest.fixture
@patch('requests.Response', spec=Response)
def mock_instances(mock_response: MagicMock, mock_api_obj) -> Tuple[MagicMock, ScrapperApi]:
    dt = datetime.datetime.strptime('2025-02-03', '%Y-%m-%d')
    api = mock_api_obj
    api.query_date = dt
    params_dict = api.params_list[0].items()
    mock_params = '&'.join([f'{k}={v}' for k, v in params_dict])
    req = Request(url=f'{api.URL}?{mock_params}')
    mock_response.request = req
    mock_response.api = api

    return mock_response, api


@pytest.mark.django_db
class TestHTMLParser:
    def test_params_property(self, mock_instances, mock_html_page: str):
        # Arrange
        mock_response, api = mock_instances
        mock_response.text = mock_html_page
        parser = HTMLParser(mock_response, api)

        # Act
        params = parser.params

        # Assert
        assert params['dateStr'][0] == api.request_date_str
        assert params['mid'][0] == list(api.SOURCES.keys())[0]

    @patch('requests.Response', spec=Response)
    def test_table_property(self, mock_response: MagicMock, mock_api_obj, mock_html_page: str):
        # Arrange
        api = mock_api_obj
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
    def test_tbody_property(self, mock_response: MagicMock, mock_api_obj, mock_html_page: str):
        # Arrange
        api = mock_api_obj
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
    def test_all_th_property(self, mock_response: MagicMock, mock_api_obj, mock_html_page: str):
        # Arrange
        api = mock_api_obj
        mock_response.text = mock_html_page
        parser = HTMLParser(mock_response, api)

        # Case 1: The th is found
        expected_result = parser.table.find_all('th')

        # Act
        th = parser.all_th

        # Assert
        assert len(th) != 0
        assert len(th) == len(expected_result)
        assert th[0].text.strip() == '品種代碼'
        assert th[1].text.strip() == '魚貨名稱'

        # Case 2: The th is not found
        parser.soup = BeautifulSoup('<html></html>', 'html.parser')

        # Act
        th = parser.all_th

        # Assert
        assert th is None

    @patch('requests.Response', spec=Response)
    def test_all_tr_property(
            self, mock_response: MagicMock, mock_api_obj, mock_html_page: str, mock_no_data_html_page: str
    ):
        # Arrange
        api = mock_api_obj
        mock_response.text = mock_html_page
        parser = HTMLParser(mock_response, api)

        # Case 1: The tr is found
        expected_result = parser.tbody.find_all('tr')

        # Act
        tr = parser.all_tr

        # Assert
        assert len(tr) != 0
        assert len(tr) == len(expected_result)
        assert tr[0].text.find('金目鱸') != -1
        assert tr[1].text.find('虱目魚') != -1

        # Case 2: no data
        parser.soup = BeautifulSoup(mock_no_data_html_page, 'html.parser')

        # Act
        tr = parser.all_tr

        # Assert
        assert type(tr) == ResultSet
        assert len(tr) == 0

        # Case 3: The tr is not found
        parser.soup = BeautifulSoup('<html></html>', 'html.parser')

        # Act
        tr = parser.all_tr

        # Assert
        assert tr is None

    @patch('requests.Response', spec=Response)
    def test_all_td_list_property(
            self, mock_response: MagicMock, mock_api_obj, mock_html_page: str, mock_no_data_html_page: str
    ):
        # Arrange
        api = mock_api_obj
        mock_response.text = mock_html_page
        parser = HTMLParser(mock_response, api)

        # Case 1: The td is found
        all_tr = parser.all_tr
        expected_result = [tr.find_all('td') for tr in all_tr]

        # Act
        td = parser.all_td_list

        # Assert
        assert len(td) != 0
        assert len(td) == len(expected_result)
        assert td[0][0].text.strip() == '1071'
        assert td[0][1].text.strip() == '金目鱸'

        # Case 2: no data
        parser.soup = BeautifulSoup(mock_no_data_html_page, 'html.parser')

        # Act
        td = parser.all_td_list

        # Assert
        assert td is None

        # Case 3: The tr is not found
        parser.soup = BeautifulSoup('<html></html>', 'html.parser')

        # Act
        td = parser.all_td_list

        # Assert
        assert td is None

    @patch('requests.Response', spec=Response)
    def test_headers_property(
            self, mock_response: MagicMock, mock_api_obj, mock_html_page: str, mock_no_data_html_page: str
    ):
        # Arrange
        api = mock_api_obj
        mock_response.text = mock_html_page
        parser = HTMLParser(mock_response, api)

        # Case 1: The headers is existed
        expected_result = [th.text.strip().replace('\n', '') for th in parser.all_th]

        # Act
        headers = parser.headers

        # Assert
        assert len(headers) != 0
        assert headers == expected_result

        # Case 2: The headers is not existed
        parser.soup = BeautifulSoup('<html></html>', 'html.parser')

        # Act
        headers = parser.headers

        # Assert
        assert len(headers) == 0

    def test_extract_relevant_data(
            self, mock_instances, mock_html_page: str, mock_no_data_html_page: str, mock_raw_data: dict
    ):
        # Arrange
        mock_response, api = mock_instances
        mock_response.text = mock_html_page
        parser = HTMLParser(mock_response, api)

        # Case 1: happy path
        expected_result = {
            '交易日期': '1140203',
            '品種代碼': 1071,
            '魚貨名稱': '金目鱸',
            '市場名稱': '台北',
            '上價': 144.2,
            '中價': 101.7,
            '下價': 0.0,
            '交易量': 3969.3,
            '平均價': 105.4
        }

        # Act
        data = parser._extract_relevant_data(mock_raw_data)

        # Assert
        assert data == expected_result

        # Case 2: bad path with empty data
        mock_data = mock_raw_data.copy()
        mock_data['下價(元/公斤)'] = ''

        # Act
        data = parser._extract_relevant_data(mock_data)

        # Assert
        assert data is None

        # Case 3: bad path with key not found
        mock_data = mock_raw_data.copy()
        del mock_data['下價(元/公斤)']

        # Act
        data = parser._extract_relevant_data(mock_data)

        # Assert
        assert data is None

    def test_parse_table(self, mock_instances, mock_html_page: str, mock_no_data_html_page: str, mock_raw_data: dict):
        # Arrange
        mock_response, api = mock_instances
        mock_response.text = mock_html_page
        parser = HTMLParser(mock_response, api)

        # Case 1: happy path
        expected_result = parser.all_td_list

        # Act
        data = parser.parse_table()

        # Assert
        assert len(data) == len(expected_result)

        # Case 2: no data
        parser.soup = BeautifulSoup(mock_no_data_html_page, 'html.parser')

        # Act
        data = parser.parse_table()

        # Assert
        assert data is None


@pytest.mark.django_db
class TestScrapperApi:
    ...
