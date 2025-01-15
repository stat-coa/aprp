import datetime
from typing import Optional, List

from dateutil import rrule
from django.db.models import (
    Model,
    CASCADE,
    DateTimeField,
    DateField,
    ForeignKey,
    FloatField,
    QuerySet,
    IntegerField,
    CharField,
)
from django.utils import timezone
from django.utils.translation import ugettext_lazy as _

from apps.configs.models import AbstractProduct, Source


class DailyTranQuerySet(QuerySet):
    def update(self, *args, **kwargs):
        kwargs['update_time'] = timezone.now()
        super(DailyTranQuerySet, self).update(*args, **kwargs)

    def between_month_day_filter(
            self,
            start_date: Optional[datetime.date]=None,
            end_date: Optional[datetime.date]=None
    ):
        """
        Filters a queryset based on a range of dates defined by the start and end dates provided.
        If either date is not specified, the original queryset is returned without filtering.

        Args:
            start_date: The start date for the filtering range.
            end_date : The end date for the filtering range.

        Returns:
            self: The filtered queryset based on the specified date range.
        """

        if not start_date or not end_date:
            return self

        date_ranges = []
        start_year = start_date.year
        end_year = end_date.year

        for i in range(start_year - 2011 + 1):
            start_date = datetime.date(start_year - i, start_date.month + 1, 1) \
                    if (is_leap(start_year - i) and start_date.month == 2 and start_date.day == 29) \
                    else datetime.date(start_year - i, start_date.month, start_date.day)

            end_date = datetime.date(end_year - i, end_date.month, end_date.day - 1) \
                    if (is_leap(end_year - i) and end_date.month == 2 and end_date.day == 29) \
                    else datetime.date(end_year - i, end_date.month, end_date.day)

            date_range = list(rrule.rrule(rrule.DAILY, dtstart=start_date, until=end_date))
            date_ranges.extend(date_range)

        return self.filter(date__in=date_ranges)

    def filter_by_date_lte(
            self,
            days: List[datetime.datetime],
            products: List[AbstractProduct],
            sources: Optional[List[Source]] = None
    ) -> List['DailyTran']:
        qs = self.filter(product__in=products, source__in=sources) if sources else self.filter(product__in=products)

        return [qs.filter(date__lte=d.date()).order_by('-date').first() for d in days]


class DailyTran(Model):
    product = ForeignKey('configs.AbstractProduct', on_delete=CASCADE, verbose_name=_('Product'))
    source = ForeignKey('configs.Source', null=True, blank=True, on_delete=CASCADE, verbose_name=_('Source'))
    up_price = FloatField(null=True, blank=True, verbose_name=_('Up Price'))
    mid_price = FloatField(null=True, blank=True, verbose_name=_('Mid Price'))
    low_price = FloatField(null=True, blank=True, verbose_name=_('Low Pirce'))
    avg_price = FloatField(verbose_name=_('Average Price'))
    avg_weight = FloatField(null=True, blank=True, verbose_name=_('Average Weight'))
    volume = FloatField(null=True, blank=True, verbose_name=_('Volume'))
    date = DateField(auto_now=False, default=timezone.now().today, verbose_name=_('Date'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))
    not_updated = IntegerField(default=0, verbose_name=_('Not Updated Count'))
    create_time = DateTimeField(auto_now_add=True, null=True, blank=True, verbose_name=_('Create Time'))

    objects = DailyTranQuerySet.as_manager()

    class Meta:
        verbose_name = _('Daily Transition')
        verbose_name_plural = _('Daily Transitions')

    def __str__(self):
        return 'product: %s, source: %s, avg_price: %s, volume: %s, avg_weight: %s, date: %s, updated: %s' % (
            self.product.name, self.source, self.avg_price, self.volume, self.avg_weight, self.date, self.update_time
        )

    def __unicode__(self):
        return 'product: %s, source: %s, avg_price: %s, volume: %s, avg_weight: %s, date: %s, updated: %s' % (
            self.product.name, self.source, self.avg_price, self.volume, self.avg_weight, self.date, self.update_time
        )

    @property
    def month_day(self):
        return int(self.date.strftime('%m%d'))


class DailyReport(Model):
    date = DateField(auto_now=False, default=timezone.now().today, verbose_name=_('Date'))
    file_id = CharField(max_length=120, unique=True, verbose_name=_('File ID'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))
    create_time = DateTimeField(auto_now_add=True, null=True, blank=True, verbose_name=_('Created'))

    class Meta:
        verbose_name = _('Daily Report')
        verbose_name_plural = _('Daily Reports')

    def __str__(self):
        return '{}, {}'.format(self.date, self.file_id)


class FestivalReport(Model):
    festival_id = ForeignKey('configs.Festival', on_delete=CASCADE, verbose_name=_('Festival ID'))
    file_id = CharField(max_length=120, unique=True, verbose_name=_('File ID'))
    file_volume_id = CharField(max_length=120, null=True, blank=True, verbose_name=_('File Volume ID'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))
    create_time = DateTimeField(auto_now_add=True, null=True, blank=True, verbose_name=_('Created'))

    class Meta:
        verbose_name = _('Festival Report')
        verbose_name_plural = _('Festival Reports')

    def __str__(self):
        return '{}, {}, {}'.format(self.festival_id, self.file_id, self.file_volume_id)


def is_leap(year):
    return (year % 4 == 0 and year % 100 != 0) or year % 400 == 0