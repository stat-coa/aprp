from __future__ import absolute_import, unicode_literals
import os
import time

from celery import Celery
from celery.schedules import crontab
import datetime

from celery.task import task

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dashboard.configs.base')

app = Celery('dashboard')

# Using a string here means the worker don't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app
# app.autodiscover_tasks()

# Add the following line to enable the workaround
app.conf.beat_max_loop_interval = 0

app.conf.beat_schedule = {
    # ======================================== Job ========================================
    'monitor_profile_active_update': {
        'task': 'MockDefaultWatchlistMonitorProfileUpdate',
        'schedule': crontab(minute='*/15'),
    },
    'delete_not_updated_trans': {
        'task': 'MockDeleteNotUpdatedTrans',
        'schedule': crontab(minute=0, hour='*'),
    },
    'update_daily_report': {
        'task': 'MockUpdateDailyReport',
        'schedule': crontab(minute='0,30', hour='9-12', day_of_week='1-5'),
        'args': (-1,)  # Update yesterday's report
    },
    # ======================================== ShortTerm Builder ========================================
    'daily-chicken-builder-3d': {
        'task': 'MockDailyChickenBuilder',
        'schedule': crontab(minute=0, hour='*', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-cattle-builder-3d': {
        'task': 'MockDailyCattleBuilder',
        'schedule': crontab(minute=3, hour='*', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-ram-builder-3d': {
        'task': 'MockDailyRamBuilder',
        'schedule': crontab(minute=6, hour='*', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-goose-builder-3d': {
        'task': 'MockDailyGooseBuilder',
        'schedule': crontab(minute=9, hour='*', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-duck-builder-3d': {
        'task': 'MockDailyDuckBuilder',
        'schedule': crontab(minute=12, hour='*', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-hog-builder-3d': {
        'task': 'MockDailyHogBuilder',
        'schedule': crontab(minute=15, hour='*', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-rice-builder-3d': {
        'task': 'MockDailyRiceBuilder',
        'schedule': crontab(minute=18, hour='*', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-flower-builder-3d': {
        'task': 'MockDailyFlowerBuilder',
        'schedule': crontab(minute=20, hour='8-18', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-crop-builder-3d': {
        'task': 'MockDailyCropBuilder',
        'schedule': crontab(minute=30, hour='8-18', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-fruit-builder-3d': {
        'task': 'MockDailyFruitBuilder',
        'schedule': crontab(minute=40, hour='8-18', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-seafood-wholesale-builder-3d': {
        'task': 'MockDailyWholesaleSeafoodBuilder',
        'schedule': crontab(minute=50, hour='8-18', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-seafood-origin-builder-3d': {
        'task': 'MockDailyOriginSeafoodBuilder',
        'schedule': crontab(minute=15, hour='11,13', day_of_week='1-5'),
        'args': (-4,)  # direct 5 day
    },

    'beat-per-minute': {
        'task': 'MockBeat',
        'schedule': 60.0,
        'args': (datetime.datetime.now(),),
    },
}


@task(name='MockDefaultWatchlistMonitorProfileUpdate')
def mock_default_watchlist_monitor_profile_update():
    time.sleep(5)


@task(name='MockDeleteNotUpdatedTrans')
def mock_delete_not_updated_trans():
    time.sleep(2.5)


@task(name='MockUpdateDailyReport')
def mock_update_daily_report(arg):
    print(f'args: {arg}')
    time.sleep(40)


@task(name='MockDailyChickenBuilder')
def mock_daily_chicken_builder(arg):
    print(f'args: {arg}')
    time.sleep(8)


@task(name='MockDailyCattleBuilder')
def mock_daily_cattle_builder(arg):
    print(f'args: {arg}')
    time.sleep(3)


@task(name='MockDailyRamBuilder')
def mock_daily_ram_builder(arg):
    print(f'args: {arg}')
    time.sleep(5.5)


@task(name='MockDailyGooseBuilder')
def mock_daily_goose_builder(arg):
    print(f'args: {arg}')
    time.sleep(8.5)


@task(name='MockDailyDuckBuilder')
def mock_daily_duck_builder(arg):
    print(f'args: {arg}')
    time.sleep(12.5)


@task(name='MockDailyHogBuilder')
def mock_daily_hog_builder(arg):
    print(f'args: {arg}')
    time.sleep(3.5)


@task(name='MockDailyRiceBuilder')
def mock_daily_rice_builder(arg):
    print(f'args: {arg}')
    time.sleep(6.5)


@task(name='MockDailyFlowerBuilder')
def mock_daily_flower_builder(arg):
    print(f'args: {arg}')
    time.sleep(30)


@task(name='MockDailyCropBuilder')
def mock_daily_crop_builder(arg):
    print(f'args: {arg}')
    time.sleep(1800)


@task(name='MockDailyFruitBuilder')
def mock_daily_fruit_builder(arg):
    print(f'args: {arg}')
    time.sleep(1000)


@task(name='MockDailyWholesaleSeafoodBuilder')
def mock_daily_wholesale_seafood_builder(arg):
    print(f'args: {arg}')
    time.sleep(900)


@task(name='MockDailyOriginSeafoodBuilder')
def mock_daily_original_seafood_builder(arg):
    print(f'args: {arg}')
    time.sleep(600)


@task(name='MockBeat')
def mock_beat(arg):
    print(f'args: {arg}')
    time.sleep(1)
