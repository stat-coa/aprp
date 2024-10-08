from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from celery.schedules import crontab
import datetime

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dashboard.configs.base')

app = Celery('dashboard')

# Using a string here means the worker don't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app
app.autodiscover_tasks()

# Add the following line to enable the workaround
app.conf.beat_max_loop_interval = 0

app.conf.beat_schedule = {
    # ======================================== Job ========================================
    'monitor_profile_active_update': {
        'task': 'DefaultWatchlistMonitorProfileUpdate',
        'schedule': crontab(minute='*/15'),
    },
    'delete_not_updated_trans': {
        'task': 'DeleteNotUpdatedTrans',
        'schedule': crontab(minute=0, hour='*'),
    },
    'update_daily_report': {
        'task': 'UpdateDailyReport',
        'schedule': crontab(minute='0,30', hour='9-12', day_of_week='1-5'),
        'args': (-1,)  # Update yesterday's report
    },
    # ======================================== ShortTerm Builder ========================================
    'daily-chicken-builder-3d': {
        'task': 'DailyChickenBuilder',
        'schedule': crontab(minute=0, hour='*', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-cattle-builder-3d': {
        'task': 'DailyCattleBuilder',
        'schedule': crontab(minute=3, hour='*', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-ram-builder-3d': {
        'task': 'DailyRamBuilder',
        'schedule': crontab(minute=6, hour='*', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-goose-builder-3d': {
        'task': 'DailyGooseBuilder',
        'schedule': crontab(minute=9, hour='*', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-duck-builder-3d': {
        'task': 'DailyDuckBuilder',
        'schedule': crontab(minute=12, hour='*', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-hog-builder-3d': {
        'task': 'DailyHogBuilder',
        'schedule': crontab(minute=15, hour='*', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-rice-builder-3d': {
        'task': 'DailyRiceBuilder',
        'schedule': crontab(minute=18, hour='*', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-flower-builder-3d': {
        'task': 'DailyFlowerBuilder',
        'schedule': crontab(minute=20, hour='8-18', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-crop-builder-3d': {
        'task': 'DailyCropBuilder',
        'schedule': crontab(minute=30, hour='8-18', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-fruit-builder-3d': {
        'task': 'DailyFruitBuilder',
        'schedule': crontab(minute=40, hour='8-18', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-seafood-wholesale-builder-3d': {
        'task': 'DailyWholesaleSeafoodBuilder',
        'schedule': crontab(minute=50, hour='8-18', day_of_week='1-5'),
        'args': (-3,)  # direct 3 day
    },
    'daily-seafood-origin-builder-3d': {
        'task': 'DailyOriginSeafoodBuilder',
        'schedule': crontab(minute=15, hour='11,13', day_of_week='1-5'),
        'args': (-4,)  # direct 5 day
    },
    # ======================================== 1 month Builder ========================================
    'daily-feed-builder-31d': {
        'task': 'DailyFeedBuilder',
        'schedule': crontab(minute=10, hour='9,17'),
        'args': (-1,)  # direct 1 days range    因飼料網頁編排一次就會抓同一個月份的資料
    },
    'daily-naifchickens-builder-31d': {
        'task': 'DailyNaifchickensBuilder',
        'schedule': crontab(minute=13, hour='8', day_of_week='1'),
        'args': (-30,)  # direct 31 days range  環南市場白肉雞/土雞網頁編排一次就會抓同一個月份的資料,但白肉雞總貨和土雞總貨網頁一次只會抓一天
    },
    'daily-chicken-builder-31d': {
        'task': 'DailyChickenBuilder',
        'schedule': crontab(minute=0, hour='22', day_of_week='1'),
        'args': (-30,)  # direct 31 days range
    },
    'daily-cattle-builder-31d': {
        'task': 'DailyCattleBuilder',
        'schedule': crontab(minute=0, hour='8', day_of_week='2'),
        'args': (-30,)  # direct 31 days range
    },
    'daily-ram-builder-31d': {
        'task': 'DailyRamBuilder',
        'schedule': crontab(minute=0, hour='22', day_of_week='2'),
        'args': (-30,)  # direct 31 days range
    },
    'daily-goose-builder-31d': {
        'task': 'DailyGooseBuilder',
        'schedule': crontab(minute=0, hour='8', day_of_week='3'),
        'args': (-30,)  # direct 31 days range
    },
    'daily-duck-builder-31d': {
        'task': 'DailyDuckBuilder',
        'schedule': crontab(minute=0, hour='22', day_of_week='3'),
        'args': (-30,)  # direct 31 days range
    },
    'daily-hog-builder-31d': {
        'task': 'DailyHogBuilder',
        'schedule': crontab(minute=0, hour='8', day_of_week='4'),
        'args': (-30,)  # direct 31 days range
    },
    'daily-rice-builder-31d': {
        'task': 'DailyRiceBuilder',
        'schedule': crontab(minute=0, hour='22', day_of_week='4'),
        'args': (-30,)  # direct 31 days range
    },
    'daily-flower-builder-31d': {
        'task': 'DailyFlowerBuilder',
        'schedule': crontab(minute=0, hour='0', day_of_week='saturday'),
        'args': (-30,)  # direct 31 days range
    },
    'daily-crop-builder-31d': {
        'task': 'DailyCropBuilder',
        'schedule': crontab(minute=0, hour='4', day_of_week='saturday'),
        'args': (-30,)  # direct 31 days range
    },
    'daily-fruit-builder-31d': {
        'task': 'DailyFruitBuilder',
        'schedule': crontab(minute=0, hour='2', day_of_week='sunday'),
        'args': (-30,)  # direct 31 days range
    },
    'daily-seafood-wholesale-builder-31d': {
        'task': 'DailyWholesaleSeafoodBuilder',
        'schedule': crontab(minute=0, hour='6', day_of_week='sunday'),
        'args': (-30,)  # direct 31 days range
    },
}


@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))
