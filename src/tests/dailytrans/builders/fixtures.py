import json

import environ
import pytest

BASE_DIR = environ.Path(__file__) - 3


@pytest.fixture
def crops_wholesale05():
    with open(BASE_DIR('fixtures/crops-api-wholesale05.json'), encoding='utf8') as f:
        return json.load(f)
