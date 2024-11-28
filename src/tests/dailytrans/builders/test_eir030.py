import pytest
from django.core.management import call_command


@pytest.fixture
def load_fixtures():
    call_command('loaddata', 'configs-type-test.yaml', verbosity=0)
    call_command('loaddata', 'configs-unit-test.yaml', verbosity=0)
    call_command('loaddata', 'configs-config-test.yaml', verbosity=0)
    call_command('loaddata', 'configs-source-test.yaml', verbosity=0)
    call_command('loaddata', 'configs-abstractproduct-config5-test.yaml', verbosity=0)
    call_command('loaddata', 'dailytrans-cog05-test.yaml', verbosity=0)


@pytest.mark.django_db
class TestEir030:
    def test_api_wholesale05(self, crops_wholesale05, load_fixtures):
        assert crops_wholesale05 is not None
