import json

import environ
import pytest
from django.core.management import call_command

from apps.crops.models import Crop
from apps.fruits.models import Fruit
from apps.dailytrans.builders.eir030 import Api as WholeSaleApi05
from apps.dailytrans.builders.apis import Api as OriginApi
from apps.dailytrans.builders.utils import DirectData

BASE_DIR = environ.Path(__file__) - 3


@pytest.fixture
def load_base_fixtures():
    call_command('loaddata', 'configs-type-test.yaml', verbosity=0)
    call_command('loaddata', 'configs-unit-test.yaml', verbosity=0)
    call_command('loaddata', 'configs-config-test.yaml', verbosity=0)
    call_command('loaddata', 'configs-source-test.yaml', verbosity=0)


@pytest.fixture
def load_crops_wholesale_fixtures(load_base_fixtures):
    call_command('loaddata', 'configs-abstractproduct-crops-test.yaml', verbosity=0)
    call_command('loaddata', 'dailytrans-cog05-test.yaml', verbosity=0)


@pytest.fixture
def load_crops_origin_fixtures(load_base_fixtures):
    call_command('loaddata', 'configs-abstractproduct-crops-origin-test.yaml', verbosity=0)


@pytest.fixture
def load_fruits_origin_fixtures():
    call_command('loaddata', 'configs-abstractproduct-fruits-origin-test.yaml', verbosity=0)

@pytest.fixture
def load_daily_trans_origin_crops_fixtures():
    call_command('loaddata', 'dailytrans-crops-origin-test.yaml', verbosity=0)


@pytest.fixture
def crops_wholesale05_api():
    with open(BASE_DIR('fixtures/crops-api-wholesale05.json'), encoding='utf8') as f:
        return json.load(f)


@pytest.fixture
def crops_origin_api():
    with open(BASE_DIR('fixtures/crops-api-origin.json'), encoding='utf8') as f:
        return json.load(f)


@pytest.fixture
def mock_wholesale_api05(load_crops_wholesale_fixtures):
    data = DirectData('COG05', 1, 'LOT-crops')

    return WholeSaleApi05(model=Crop, **data._asdict())


@pytest.fixture
def mock_crops_origin_api(load_crops_origin_fixtures):
    data = DirectData('COG05', 2, 'LOT-crops')

    return OriginApi(model=Crop, **data._asdict())


@pytest.fixture
def mock_fruits_origin_api(load_fruits_origin_fixtures):
    data = DirectData('COG06', 2, 'LOT-fruits')

    return OriginApi(model=Fruit, **data._asdict())
