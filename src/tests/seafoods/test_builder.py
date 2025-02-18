import datetime
import logging
import time
from typing import Tuple
from unittest.mock import patch, MagicMock, NonCallableMagicMock

import pytest
from requests import Response

from apps.dailytrans.builders.eir032 import Api, ScrapperApi
from apps.dailytrans.builders.utils import DirectData
from apps.seafoods.builder import direct_generic_wholesale, CONFIG_CODE, LOGGER_TYPE_CODE
from apps.seafoods.models import Seafood


@pytest.fixture
@patch('apps.dailytrans.builders.abstract.logging', spec=logging)
@patch('apps.seafoods.builder.ScrapperApi', spec=ScrapperApi)
@patch('apps.seafoods.builder.WholeSaleApi', spec=Api)
def mock_apis(api, scrapper_api, mock_log) -> Tuple[NonCallableMagicMock, NonCallableMagicMock]:
    data = DirectData(CONFIG_CODE, 1, LOGGER_TYPE_CODE)
    mock_api: NonCallableMagicMock = api(model=Seafood, **data._asdict())
    mock_scrapper_api: NonCallableMagicMock = scrapper_api(model=Seafood, **data._asdict())
    mock_scrapper_api.LOGGER = mock_log.getLogger('aprp')
    mock_scrapper_api.LOGGER_EXTRA = {'type_code': LOGGER_TYPE_CODE, 'request_url': None}

    return mock_api, mock_scrapper_api


@pytest.mark.django_db
@patch('apps.seafoods.builder.time', spec=time)
@patch('apps.seafoods.builder.ScrapperApi', spec=ScrapperApi)
@patch('apps.seafoods.builder.WholeSaleApi', spec=Api)
def test_direct_generic_wholesale(api: MagicMock, scrapper_api: MagicMock, mock_time: MagicMock, mock_apis):
    # Arrange
    mock_api, mock_scrapper_api = mock_apis
    resp = Response()
    resp.status_code = 200
    dt = datetime.datetime.now().date()
    api.return_value = mock_api
    scrapper_api.return_value = mock_scrapper_api
    mock_api.request.return_value = resp
    mock_scrapper_api.requests.return_value = [resp]


    # Case 1: happy path
    # Act
    direct_generic_wholesale(delta=-3)

    # Assert
    assert mock_scrapper_api.requests.call_count == 4
    mock_scrapper_api.requests.assert_called_with(start_date=dt)
    mock_api.request.assert_not_called()

    # Case 2: all failed requests
    mock_api.reset_mock()
    mock_scrapper_api.reset_mock()
    mock_time.reset_mock()
    resp.status_code = 404

    # Act
    direct_generic_wholesale(delta=-3)

    # Assert
    assert mock_scrapper_api.LOGGER.warning.call_count == 4
    mock_scrapper_api.LOGGER.warning.assert_called_with(
        'All requests failed, try to use API to fetch the data', extra=mock_scrapper_api.LOGGER_EXTRA
    )
    assert mock_api.request.call_count == 4
    assert mock_api.load.call_count == 4
    mock_scrapper_api.loads.assert_not_called()
