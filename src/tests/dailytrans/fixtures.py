import pytest
from django.core.management import call_command

from tests.dailytrans.factories import (
    DailyTranFactory,
)


@pytest.fixture
def daily_tran(product_of_pig, sources_for_pig):
    return DailyTranFactory(
        product=product_of_pig,
        source=sources_for_pig[0],
    )


@pytest.fixture
def load_base_fixtures():
    call_command('loaddata', 'configs-type-test.yaml', verbosity=0)
    call_command('loaddata', 'configs-unit-test.yaml', verbosity=0)
    call_command('loaddata', 'configs-config-test.yaml', verbosity=0)
    call_command('loaddata', 'configs-source-test.yaml', verbosity=0)
