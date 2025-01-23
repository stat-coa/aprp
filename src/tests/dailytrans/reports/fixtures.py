import pytest
from django.core.management import call_command


@pytest.fixture
def load_abstract_product_fixtures():
    call_command('loaddata', 'configs-abstractproduct-monitors.yaml', verbosity=0)


@pytest.fixture
def load_month_fixtures():
    call_command('loaddata', 'configs-month-test.yaml', verbosity=0)


@pytest.fixture
def load_watchlist_fixture():
    call_command('loaddata', 'watchlists-watchlist-test.yaml', verbosity=0)


@pytest.fixture
def load_monitor_profile_fixtures(
    load_base_fixtures, load_month_fixtures, load_abstract_product_fixtures, load_watchlist_fixture
):
    call_command('loaddata', 'mp-2024h2-test.yaml', verbosity=0)


@pytest.fixture
def load_daily_tran_fixtures_of_eggplant(load_monitor_profile_fixtures):
    call_command('loaddata', 'dailytrans-eggplant-test.yaml', verbosity=0)


@pytest.fixture
def load_daily_tran_fixtures_of_eggplant_1m(load_monitor_profile_fixtures):
    call_command('loaddata', 'dailytrans-eggplant-1m-test.yaml', verbosity=0)


@pytest.fixture
def load_daily_tran_fixtures_of_hog(load_monitor_profile_fixtures):
    call_command('loaddata', 'dailytrans-hog-test.yaml', verbosity=0)


@pytest.fixture
def load_daily_tran_fixtures_of_banana(load_monitor_profile_fixtures):
    call_command('loaddata', 'dailytrans-banana-test.yaml', verbosity=0)


@pytest.fixture
def load_daily_tran_fixtures_of_ram(load_monitor_profile_fixtures):
    call_command('loaddata', 'dailytrans-ram-test.yaml', verbosity=0)


@pytest.fixture
def load_daily_tran_fixtures_of_cattle(load_monitor_profile_fixtures):
    call_command('loaddata', 'dailytrans-cattle-test.yaml', verbosity=0)
