import json

import environ
import pytest
from django.core.management import call_command

from apps.crops.models import Crop
from apps.dailytrans.builders.eir030 import Api as WholeSaleApi05
from apps.dailytrans.builders.utils import DirectData

BASE_DIR = environ.Path(__file__) - 3


@pytest.fixture
def load_fixtures():
    call_command('loaddata', 'configs-type-test.yaml', verbosity=0)
    call_command('loaddata', 'configs-unit-test.yaml', verbosity=0)
    call_command('loaddata', 'configs-config-test.yaml', verbosity=0)
    call_command('loaddata', 'configs-source-test.yaml', verbosity=0)
    call_command('loaddata', 'configs-abstractproduct-crops-test.yaml', verbosity=0)
    call_command('loaddata', 'dailytrans-cog05-test.yaml', verbosity=0)


@pytest.fixture
def crops_wholesale05_api():
    with open(BASE_DIR('fixtures/crops-api-wholesale05.json'), encoding='utf8') as f:
        return json.load(f)


@pytest.fixture
def mock_wholesale_api05(load_fixtures, crops_wholesale05_api):
    data = DirectData('COG05', 1, 'LOT-crops')

    return WholeSaleApi05(model=Crop, **data._asdict())
