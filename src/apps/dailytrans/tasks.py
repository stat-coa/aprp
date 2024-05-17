import os
import logging
from datetime import datetime, timedelta
from celery.task import task
from django.conf import settings

from apps.dailytrans.models import DailyTran, DailyReport
from apps.dailytrans.reports.dailyreport import DailyReportFactory
from google_api.backends import DefaultGoogleDriveClient


@task(name="DeleteNotUpdatedTrans")
def delete_not_updated_trans(not_updated_times=15):
    db_logger = logging.getLogger('aprp')
    logger_extra = {
        'type_code': 'LOT-dailytrans',
    }
    try:
        dailytrans = DailyTran.objects.filter(not_updated__gte=not_updated_times)
        count = dailytrans.count()
        dailytrans.all().delete()
        if count > 0:
            db_logger.info('Delete %s not updated dailytrans' % count, extra=logger_extra)

    except Exception as e:
        db_logger.exception(e, extra=logger_extra)


@task(name='UpdateDailyReport')
def update_daily_report(delta_days=-1):
    date = datetime.now() + timedelta(days=delta_days)

    # generate file
    factory = DailyReportFactory(specify_day=date)
    file_name, file_path = factory()
