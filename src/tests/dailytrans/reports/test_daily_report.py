from datetime import datetime

import pytest

from apps.dailytrans.reports.dailyreport import SimplifyDailyReportFactory


@pytest.mark.django_db
class TestSimplifyDailyReportFactory:
    @staticmethod
    def get_daily_report_factory(date = datetime.strptime('2024-11-12', '%Y-%m-%d')):
        return SimplifyDailyReportFactory(date)

    def test_this_week_start_property(self):
        # Arrange
        f = self.get_daily_report_factory()

        # Assert
        assert f.this_week_start == datetime.strptime('2024-11-06', '%Y-%m-%d')

    def test_this_week_end_property(self):
        # Arrange
        f = self.get_daily_report_factory()

        # Assert
        assert f.this_week_end == f.specify_day

    def test_last_week_start_property(self):
        # Arrange
        f = self.get_daily_report_factory()

        # Assert
        assert f.last_week_start == datetime.strptime('2024-10-30', '%Y-%m-%d')

    def test_last_week_end_property(self):
        # Arrange
        f = self.get_daily_report_factory()

        # Assert
        assert f.last_week_end == datetime.strptime('2024-11-05', '%Y-%m-%d')

    def test_watchlist_property(self, load_watchlist_fixture):
        # Arrange
        f = self.get_daily_report_factory()

        # Act
        w = f.watchlist

        # Assert
        assert w is not None
        assert w.name == '113下半年'

    def test_monitor_profile_qs_property(self, load_monitor_profile_fixtures):
        # Arrange
        f = self.get_daily_report_factory()

        # Act
        qs = f.monitor_profile_qs

        # Assert
        assert qs.count() == 51

    def test_show_monitor_property(self, load_monitor_profile_fixtures):
        # Arrange
        # Case1: always display
        f = self.get_daily_report_factory()
        m = f.monitor_profile_qs.filter(product__name__icontains='香蕉').first()
        f.monitor = m

        # Assert
        assert f.show_monitor is True

        # Case2: the monitor month is in the watchlist month
        m = f.monitor_profile_qs.filter(product__name__icontains='落花生').first()
        f.monitor = m

        # Assert
        assert f.show_monitor is True

        # Case3: the monitor month is not in the watchlist month
        m = f.monitor_profile_qs.filter(product__name__icontains='洋蔥').first()
        f.monitor = m

        # Assert
        assert f.show_monitor is False

    def test_monitor_has_desc_property(self, load_monitor_profile_fixtures):
        # Arrange
        f = self.get_daily_report_factory(datetime.strptime('2024-12-12', '%Y-%m-%d'))

        # Case1: has desc
        m = f.monitor_profile_qs.filter(product__name__icontains='紅豆').first()
        f.monitor = m

        # Assert
        assert f.monitor_has_desc is True

        # Case2: no desc
        m = f.monitor_profile_qs.filter(product__name__icontains='洋蔥').first()
        f.monitor = m

        # Assert
        assert f.monitor_has_desc is False

    def test_this_week_date_property(self):
        # Arrange
        f = self.get_daily_report_factory()

        # Act
        days = f.this_week_date

        # Assert
        assert days[0] == datetime.strptime('2024-11-06', '%Y-%m-%d')
        assert days[-1] == f.specify_day

    def test_last_week_date_property(self):
        # Arrange
        f = self.get_daily_report_factory()

        # Act
        days = f.last_week_date

        # Assert
        assert days[0] == datetime.strptime('2024-11-06', '%Y-%m-%d')
        assert days[-1] == datetime.strptime('2024-10-30', '%Y-%m-%d')
