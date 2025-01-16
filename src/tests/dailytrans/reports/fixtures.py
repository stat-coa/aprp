import pytest
from django.contrib.auth.models import User
from django.core.management import call_command


@pytest.fixture
def load_abstract_product_fixtures():
    call_command('loaddata', 'configs-abstractproduct-monitors.yaml', verbosity=0)


@pytest.fixture
def load_month_fixtures():
    call_command('loaddata', 'configs-month-test.yaml', verbosity=0)


@pytest.fixture
def create_user():
    User.objects.create_user('admin', 'admin', 'admin@mail.com').save()

@pytest.fixture
def load_monitor_profile_fixtures(load_base_fixtures, load_month_fixtures,load_abstract_product_fixtures, create_user):
    call_command('loaddata', 'mp-2024h2-test.yaml', verbosity=0)
