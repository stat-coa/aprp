from datetime import datetime
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from apps.dailytrans.models import DailyTran
from apps.dailytrans.reports.dailyreport import SimplifyDailyReportFactory, DailyTranHandler


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


@pytest.mark.django_db
class TestDailyTranHandler:
    @staticmethod
    def get_daily_trans_df() -> pd.DataFrame:
        return (
            pd
            .DataFrame(list(DailyTran.objects.all().values()))
            .sort_values('date')
        )

    @patch('pandas.read_sql_query')
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_has_volume_property(self, conn, mock_read_sql_query, load_daily_tran_fixtures_of_eggplant):
        # Arrange
        # Case1: has volume
        raw_df = self.get_daily_trans_df()
        df = raw_df.copy()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})

        # Assert
        assert h.has_volume == True

        # Case2: no volume - empty dataframe
        df = pd.DataFrame()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})

        # Assert
        assert h.has_volume == False

        # Case3: no volume - no value of volume column
        mock_read_sql_query.return_value = raw_df.copy()
        h = DailyTranHandler('', {})
        h.df.volume = None

        # Assert
        assert h.has_volume == False

    @patch('pandas.read_sql_query')
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_has_weight_property(self, conn, mock_read_sql_query, load_daily_tran_fixtures_of_eggplant):
        # Arrange
        # Case1: has weight
        raw_df = self.get_daily_trans_df()
        df = raw_df.copy()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})
        h.df.avg_weight = 1

        # Assert
        assert h.has_weight == True

        # Case2: no weight - empty dataframe
        df = pd.DataFrame()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})

        # Assert
        assert h.has_weight == False

        # Case3: no weight - no value of weight column
        mock_read_sql_query.return_value = raw_df.copy()
        h = DailyTranHandler('', {})

        # Assert
        assert h.has_weight == False

    @patch('pandas.read_sql_query')
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_fulfilled_df_property_by_eggplant(self, conn, mock_read_sql_query, load_daily_tran_fixtures_of_eggplant):
        # Arrange
        df = self.get_daily_trans_df()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})

        # Act
        df_fulfilled = h.fulfilled_df

        # Assert
        assert df_fulfilled.shape[0] == df.shape[0]
        assert df_fulfilled.query('vol_for_calculation == 1').shape[0] == 0
        assert df_fulfilled.wt_for_calculation.sum() == df.assign(avg_weight=lambda x: x.volume).avg_weight.sum()
        assert df_fulfilled.avg_price.sum() == df.assign(avg_price=lambda x: x.avg_price * x.volume).avg_price.sum()
        assert df_fulfilled.query('source_id == 1').shape[0] == 0

    @patch('pandas.read_sql_query')
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_fulfilled_df_property_by_hog(self, conn, mock_read_sql_query, load_daily_tran_fixtures_of_hog):
        # Arrange
        df = self.get_daily_trans_df()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})

        # Act
        df_fulfilled = h.fulfilled_df

        # Assert
        assert df_fulfilled.shape[0] == df.shape[0]
        assert df_fulfilled.query('wt_for_calculation == 1').shape[0] == 0
        assert df_fulfilled.wt_for_calculation.sum() == df.assign(
            avg_weight=lambda x: x.volume * x.avg_weight
        ).avg_weight.sum()
        assert df_fulfilled.avg_price.sum() == df.assign(
            avg_price=lambda x: x.avg_price * x.volume * x.avg_weight
        ).avg_price.sum()
        assert df_fulfilled.query('source_id == 1').shape[0] == 0

    @patch('pandas.read_sql_query')
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_fulfilled_df_property_by_banana(self, conn, mock_read_sql_query, load_daily_tran_fixtures_of_banana):
        # Arrange
        df = self.get_daily_trans_df()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})
        h.df.source_id = None

        # Act
        df_fulfilled = h.fulfilled_df

        # Assert
        assert df_fulfilled.shape[0] == df.shape[0]
        assert df_fulfilled.query('vol_for_calculation == 1').shape[0] == df.shape[0]
        assert df_fulfilled.query('wt_for_calculation == 1').shape[0] == df.shape[0]
        assert df_fulfilled.query('source_id == 1').shape[0] == df.shape[0]

    @patch('pandas.read_sql_query')
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_df_with_group_by_date_by_eggplant(self, conn, mock_read_sql_query, load_daily_tran_fixtures_of_eggplant):
        # Arrange
        df = self.get_daily_trans_df()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})
        df_fulfilled = h.fulfilled_df

        # Act
        df_grouped = h.df_with_group_by_date

        # Assert
        assert df_grouped.query('avg_avg_weight == 1').shape[0] == df_fulfilled.groupby('date').sum().shape[0]
        assert df_grouped.sum_volume.sum() == df_fulfilled.volume.sum()
        assert (df_grouped.num_of_source.sum() == df_fulfilled
                .groupby(['date', 'source_id']).sum().assign(num_of_source=1).num_of_source.sum())
