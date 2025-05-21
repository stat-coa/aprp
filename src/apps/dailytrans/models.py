import calendar
import datetime

from dateutil import rrule
from typing import Optional, List
from apps.configs.models import AbstractProduct, Source
from django.db.models import (
    CASCADE,
    CharField,
    DateField,
    DateTimeField,
    ForeignKey,
    FloatField,
    IntegerField,
    Model,
    QuerySet
)
from django.utils import timezone
from django.utils.translation import ugettext_lazy as _


class DailyTranQuerySet(QuerySet):
    def update(self, *args, **kwargs):
        kwargs['update_time'] = timezone.now()
        super(DailyTranQuerySet, self).update(**kwargs)
        # 父類別的 update() 只會處理 **kwargs，並不會處理 *args

    def between_month_day_filter(
            self,
            start_date: Optional[datetime.date]=None,
            end_date: Optional[datetime.date]=None
    ):
        """
        Optional[X] 等同於 Union[X,None] 也就是 X型別或是 None

        Filters a queryset based on a range of dates defined by the start and end dates provided.
        If either date is not specified, the original queryset is returned without filtering.
        依據提供的起始日期與結束日期範圍過濾 QuerySet
        如果任一日期未指定，則返回原始的 QuerySet，不做任何過濾

        Args:
            start_date: The start date for the filtering range.
            end_date : The end date for the filtering range.
            start_date: 篩選範圍的起始日期
            end_date  : 篩選範圍的結束日期

        Returns:
            self: The filtered queryset based on the specified date range.
            self: 根據指定日期範圍篩選後的 QuerySet
        """
        # 如果 start_date 或 end_date 未指定，則返回原始的 QuerySet
        if not start_date or not end_date:
            return self

        date_ranges = []
        start_year = start_date.year
        end_year = end_date.year

        # 每跑一次 i 就是 start_year - i 年, end_year - i 年
        # 如果 start_date 是2月29日 且當前年份 start_year - i 年是閏年，就調整為當年的3月1日
        # 跳過閏年是為了每年都用同樣的天數來比較
        for i in range(start_year - 2011 + 1):
            start_date = (
                datetime.date(start_year - i, start_date.month + 1, 1)
                    if (is_leap(start_year - i) and start_date.month == 2 and start_date.day == 29)
                    else datetime.date(start_year - i, start_date.month, start_date.day)
            )

            end_date = (
                datetime.date(end_year - i, end_date.month, end_date.day - 1)
                    if (is_leap(end_year - i) and end_date.month == 2 and end_date.day == 29)
                    else datetime.date(end_year - i, end_date.month, end_date.day)
            )
            # rrule() 會在每天各產生一個日期 ex. 2025-01-01
            date_range = list(rrule.rrule(rrule.DAILY, dtstart=start_date, until=end_date))
            date_ranges.extend(date_range)
        # date__in 為 SQL 的 IN 條件，只選出 date 欄位的值
        return self.filter(date__in=date_ranges)

    def filter_by_date_lte(self, days: List[datetime.datetime],
                        products: List[AbstractProduct],
                        sources: Optional[List[Source]] = None) -> List['DailyTran']:
        qs = (
            self.filter(product__in=products, source__in=sources)
            if sources else self.filter(product__in=products)
        )
        # date__lte 為 查詢 date 欄位 <= 指定日期
        return [qs.filter(date__lte=d.date()).order_by('-date').first() for d in days]


class DailyTran(Model):
    """
    product: 午仔魚
    source: 桃園(漁產品-批發)
    up_price: 317.2
    mid_price: 275.1
    low_price: 217.6
    avg_price: 272.5
    avg_weight: None
    volume: 180.0
    date: 2017-06-10
    update_time: 2020-07-26 08:55:55.667957+00:00
    not_updated: 0
    creat_time: None
    """
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
        return (f'product: {self.product.name}, source: {self.source}, avg_price: {self.avg_price}'
                f', volume: {self.volume}, avg_weight: {self.avg_weight}, date: {self.date},'
                f' updated: {self.update_time}')

    @property
    def month_day(self):
        return int(self.date.strftime('%m%d'))


class DailyReport(Model):
    """
    date: 2024-12-10
    file_id: 1bH64LKtP6UTMwQY0aWAnXz9VZRdf0rnM
    update_time: 2024-12-11 04:34:26.350437+00:00
    create_time: 2024-12-11 02:03:58.949463+00:00
    """
    date = DateField(auto_now=False, default=timezone.now().today, verbose_name=_('Date'))
    file_id = CharField(max_length=120, unique=True, verbose_name=_('File ID'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))
    create_time = DateTimeField(auto_now_add=True, null=True, blank=True, verbose_name=_('Created'))

    class Meta:
        verbose_name = _('Daily Report')
        verbose_name_plural = _('Daily Reports')

    def __str__(self):
        return f'{self.date}, {self.file_id}'


class FestivalReport(Model):
    """
    festival_id: 112_中秋節
    file_id: 1XCcEPo_ERq-8TutP0MyU-mTgDfnaZUzh
    file_volume_id: 1pzgZBOS0MOHg95j1T5P4RvO4KgLKl5hM
    update_time: 2024-01-11 07:17:57.597381+00:00
    create_time: 2024-01-11 07:17:57.597381+00:00
    """
    festival_id = ForeignKey('configs.Festival', on_delete=CASCADE, verbose_name=_('Festival ID'))
    file_id = CharField(max_length=120, unique=True, verbose_name=_('File ID'))
    file_volume_id = CharField(max_length=120, null=True, blank=True, verbose_name=_('File Volume ID'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))
    create_time = DateTimeField(auto_now_add=True, null=True, blank=True, verbose_name=_('Created'))

    class Meta:
        verbose_name = _('Festival Report')
        verbose_name_plural = _('Festival Reports')

    def __str__(self):
        return f'{self.festival_id}, {self.file_id}, {self.file_volume_id}'

def is_leap(year):
    return calendar.isleap(year)
