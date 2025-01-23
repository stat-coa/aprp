from datetime import datetime
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from apps.dailytrans.models import DailyTran
from apps.dailytrans.reports.dailyreport import SimplifyDailyReportFactory, DailyTranHandler, ExtraItem


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

    def test_get_simple_avg_price(self, load_daily_tran_fixtures_of_eggplant):
        # Arrange
        f = self.get_daily_report_factory()
        df = TestDailyTranHandler.get_daily_trans_df()
        qs = list(DailyTran.objects.all())

        # Case1: has daily trans
        price = round(f.get_simple_avg_price(qs), 2)

        # Assert
        assert price == round(df.avg_price.mean(), 2)

        # Case2: no daily trans
        qs = []

        # Assert
        assert f.get_simple_avg_price(qs) == 0.0

    @patch('pandas.read_sql_query', new_callable=MagicMock)
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_set_this_week_data(
            self,
            conn,
            mock_read_sql_query: MagicMock,
            load_daily_tran_fixtures_of_eggplant
    ):
        # Arrange
        f = self.get_daily_report_factory()
        f.monitor = f.monitor_profile_qs.filter(product__name__icontains='茄子').first()
        mock_read_sql_query.return_value = TestDailyTranHandler.get_daily_trans_df()

        # Act
        f.set_this_week_data()
        expected_price_keys = {
            f"""{f.col_mapping[f'{dt.date()}']}{f.monitor.row}"""
            for dt in f.this_week_date
        }
        expected_volume_keys = {
            f"""{f.col_mapping[f'{dt.date()}_volume']}{f.monitor.row}"""
            for dt in f.this_week_date
        }

        # Assert
        assert expected_price_keys.issubset(set(f.result[f.monitor.product.name].keys()))
        assert expected_volume_keys.issubset(set(f.result[f.monitor.product.name].keys()))

    @patch('pandas.read_sql_query', new_callable=MagicMock)
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_set_avg_price_values(
            self,
            conn,
            mock_read_sql_query: MagicMock,
            load_daily_tran_fixtures_of_eggplant
    ):
        # Arrange
        f = self.get_daily_report_factory()
        f.monitor = f.monitor_profile_qs.filter(product__name__icontains='茄子').first()
        f.result[f.monitor.product.name] = {}
        mock_read_sql_query.return_value = TestDailyTranHandler.get_daily_trans_df()
        f.two_weeks_handler = DailyTranHandler('', {})
        last_week_avg_price = f.two_weeks_handler.get_avg_price(
            f.last_week_start.date(), f.last_week_end.date()
        )
        this_week_avg_price = f.two_weeks_handler.get_avg_price(
            f.this_week_start.date(), f.this_week_end.date()
        )
        weekly_price_change_rate = (this_week_avg_price - last_week_avg_price) / last_week_avg_price * 100

        # Act
        f.set_avg_price_values()


        # Assert
        assert round(f.result[f.monitor.product.name][f'L{f.monitor.row}'], 2) == round(weekly_price_change_rate, 2)
        assert round(f.result[f.monitor.product.name][f'H{f.monitor.row}'], 2) == round(this_week_avg_price, 2)
        assert round(f.result[f.monitor.product.name][f'W{f.monitor.row}'], 2) == round(last_week_avg_price, 2)

    @patch('pandas.read_sql_query', new_callable=MagicMock)
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_set_volume_values(
            self,
            conn,
            mock_read_sql_query: MagicMock,
            load_daily_tran_fixtures_of_eggplant
    ):
        # Arrange
        f = self.get_daily_report_factory()
        f.monitor = f.monitor_profile_qs.filter(product__name__icontains='茄子').first()
        f.result[f.monitor.product.name] = {}
        mock_read_sql_query.return_value = TestDailyTranHandler.get_daily_trans_df()
        f.two_weeks_handler = DailyTranHandler('', {})
        last_week_avg_vol = f.two_weeks_handler.get_avg_volume(
            f.last_week_start.date(), f.last_week_end.date()
        )
        this_week_avg_vol = f.two_weeks_handler.get_avg_volume(
            f.this_week_start.date(), f.this_week_end.date()
        )
        weekly_vol_change_rate = (this_week_avg_vol - last_week_avg_vol) / last_week_avg_vol * 100

        # Act
        f.set_volume_values()

        # Assert
        assert round(f.result[f.monitor.product.name][f'T{f.monitor.row}'], 2) == round(this_week_avg_vol, 2)
        assert round(f.result[f.monitor.product.name][f'U{f.monitor.row}'], 2) == round(weekly_vol_change_rate, 2)


    def test_set_monitor_price(self, load_daily_tran_fixtures_of_eggplant):
        # Arrange
        f = self.get_daily_report_factory()
        f.monitor = f.monitor_profile_qs.filter(product__name__icontains='茄子').first()
        f.result[f.monitor.product.name] = {}

        # Act
        f.set_monitor_price()

        # Assert
        assert f.result[f.monitor.product.name][f'F{f.monitor.row}'] == f.monitor.price

    @patch('apps.dailytrans.reports.dailyreport.QueryString', new_callable=MagicMock)
    @patch('pandas.read_sql_query', new_callable=MagicMock)
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_set_same_month_of_last_year_value(
            self,
            conn,
            mock_read_sql_query: MagicMock,
            mock_query_string,
            load_daily_tran_fixtures_of_eggplant_1m
    ):
        # Arrange
        f = self.get_daily_report_factory()
        f.monitor = f.monitor_profile_qs.filter(product__name__icontains='茄子').first()
        f.result[f.monitor.product.name] = {}
        mock_read_sql_query.return_value = TestDailyTranHandler.get_daily_trans_df()
        h = DailyTranHandler('', {})
        df = h.df_with_group_by_date
        expected_avg_price = (df.avg_price * df.sum_volume).sum() / df.sum_volume.sum()

        # Act
        f.set_same_month_of_last_year_value()

        # Assert
        assert round(f.result[f.monitor.product.name][f'G{f.monitor.row}'], 2) == round(expected_avg_price, 2)

    def test_update_ram_data(self, load_daily_tran_fixtures_of_ram):
        # Arrange
        f = self.get_daily_report_factory()
        f.monitor = f.monitor_profile_qs.filter(product__name__icontains='羊', product__track_item=True).first()
        f.result[f.monitor.product.name] = {}

        # Act
        f.update_ram_data()
        expected_date_keys = {
            f"""{f.col_mapping[f'{dt.date()}']}{f.monitor.row}"""
            for dt in f.this_week_date
        }
        expected_values = {
            'M117': 364.0,
            'N117': 358.0,
            'O117': '(11/07)',
            'P117': '(11/07)',
            'Q117': '(11/07)',
            'R117': 367.0,
            'S117': '(11/11)'
        }

        # Assert
        assert expected_date_keys.issubset(set(f.result[f.monitor.product.name].keys()))
        assert f.result[f.monitor.product.name] == expected_values

    def test_update_cattle_data(self, load_daily_tran_fixtures_of_cattle):
        # Arrange
        f = self.get_daily_report_factory()
        f.monitor = f.monitor_profile_qs.filter(product__name__icontains='牛').first()
        f.result[f.monitor.product.name] = {}

        # Act
        f.update_cattle_data()
        expected_date_keys = {
            f"""{f.col_mapping[f'{dt.date()}']}{f.monitor.row}"""
            for dt in f.this_week_date
        }

        # Assert
        assert expected_date_keys.issubset(set(f.result[f.monitor.product.name].keys()))
        assert len(set(f.result[f.monitor.product.name].values())) == 2

    def test_set_visible_rows(self, load_daily_tran_fixtures_of_eggplant):
        # Arrange
        f = self.get_daily_report_factory()
        f.monitor = f.monitor_profile_qs.filter(product__name__icontains='茄子').first()
        expected_result = [f.monitor.row]
        f.result[f.monitor.product.name] = {}
        extra_monitors = ExtraItem.get_extra_monitors()

        # Act
        f.set_visible_rows()
        f.monitor = extra_monitors[0]
        f.set_visible_rows()
        expected_result.append(f.monitor.row)


        # Assert
        assert f.excel_handler.visible_rows == expected_result

    def test_remove_crop_desc(self, load_daily_tran_fixtures_of_eggplant):
        # Arrange
        f = self.get_daily_report_factory()
        f.monitor = f.monitor_profile_qs.filter(product__name__icontains='紅豆').first()

        # Act
        f.set_product_desc()

        # Assert
        assert f.monitor.product.name not in f.excel_handler.dict_crop_desc


@pytest.mark.django_db
class TestDailyTranHandler:
    @staticmethod
    def get_daily_trans_df() -> pd.DataFrame:
        return (
            pd
            .DataFrame(list(DailyTran.objects.all().values()))
            .sort_values('date')
        )

    @staticmethod
    def get_calculated_avg_price(df: pd.DataFrame) -> float:

        return round(
            df
            .groupby(['date'])
            .sum()
            .assign(avg_price=lambda x: x.avg_price / x.wt_for_calculation)
            .avg_price
            .sum(),
            2
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
    def test_df_with_group_by_date_with_eggplant(self, conn, mock_read_sql_query, load_daily_tran_fixtures_of_eggplant):
        # Arrange
        df = self.get_daily_trans_df()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})
        df_fulfilled = h.fulfilled_df
        expected_avg_price = self.get_calculated_avg_price(df_fulfilled)
        expected_num_of_source = (
            df_fulfilled
            .groupby(['date', 'source_id'])
            .sum()
            .assign(num_of_source=1)
            .num_of_source.sum()
        )

        # Act
        df_grouped = h.df_with_group_by_date

        # Assert
        assert all(df_grouped['avg_avg_weight'] == 1)
        assert df_grouped.sum_volume.sum() == df_fulfilled.volume.sum()
        assert round(df_grouped.avg_price.sum(), 2) == expected_avg_price
        assert df_grouped.num_of_source.sum() == expected_num_of_source

    @patch('pandas.read_sql_query')
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_df_with_group_by_date_with_banana(self, conn, mock_read_sql_query, load_daily_tran_fixtures_of_banana):
        # Arrange
        df = self.get_daily_trans_df()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})
        df_fulfilled = h.fulfilled_df
        expected_avg_price = self.get_calculated_avg_price(df_fulfilled)

        # Act
        df_grouped = h.df_with_group_by_date

        # Assert
        assert all(df_grouped['avg_avg_weight'] == 1)
        assert df_grouped.sum_volume.sum() == df_fulfilled.vol_for_calculation.sum()
        assert round(df_grouped.avg_price.sum(), 2) == expected_avg_price

    @patch('pandas.read_sql_query')
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_df_with_group_by_date_with_hog(self, conn, mock_read_sql_query, load_daily_tran_fixtures_of_hog):
        # Arrange
        df = self.get_daily_trans_df()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})
        df_fulfilled = h.fulfilled_df
        expected_avg_price = self.get_calculated_avg_price(df_fulfilled)
        expected_avg_weight = (
            round(
                df_fulfilled.groupby('date')
                .sum()
                .assign(avg_weight=lambda x: x.wt_for_calculation / x.volume
                        )
                .avg_weight
                .sum(),
                2
            )
        )

        # Act
        df_grouped = h.df_with_group_by_date

        # Assert
        assert round(df_grouped.avg_avg_weight.sum(), 2) == expected_avg_weight
        assert df_grouped.sum_volume.sum() == df_fulfilled.vol_for_calculation.sum()
        assert round(df_grouped.avg_price.sum(), 2) == expected_avg_price

    @patch('pandas.read_sql_query')
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_get_df_query_by_date(self, conn, mock_read_sql_query, load_daily_tran_fixtures_of_eggplant):
        # Arrange
        f = TestSimplifyDailyReportFactory.get_daily_report_factory()
        df = self.get_daily_trans_df()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})
        df_expected = h.df_with_group_by_date

        # Case1: no date
        df_result = h._get_df_query_by_date()

        # Assert
        assert set(pd.to_datetime(df_result.date)).issubset(set(pd.to_datetime(df_expected.date)))

        # Case2: last week date
        df_result = h._get_df_query_by_date(f.last_week_start.date(), f.last_week_end.date())

        # Assert
        assert set(pd.to_datetime(df_result.date)).issubset(set(pd.to_datetime(pd.Series(f.last_week_date))))

        # Case3: this week date
        df_result = h._get_df_query_by_date(f.this_week_start.date(), f.this_week_end.date())

        # Assert
        assert set(pd.to_datetime(df_result.date)).issubset(set(pd.to_datetime(pd.Series(f.this_week_date))))

    @patch('pandas.read_sql_query')
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_get_avg_price_by_eggplant(self, conn, mock_read_sql_query, load_daily_tran_fixtures_of_eggplant):
        # Arrange
        df = self.get_daily_trans_df()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})
        df_grouped = h.df_with_group_by_date
        total_price = df_grouped.avg_price * df_grouped.sum_volume

        # Act
        result_price = h.get_avg_price()

        # Assert
        assert result_price != 0
        assert result_price == (total_price.sum() / df_grouped.sum_volume.sum())

    @patch('pandas.read_sql_query')
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_get_avg_price_by_hog(self, conn, mock_read_sql_query, load_daily_tran_fixtures_of_hog):
        # Arrange
        df = self.get_daily_trans_df()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})
        df_grouped = h.df_with_group_by_date
        total_price = df_grouped.avg_price * df_grouped.sum_volume * df_grouped.avg_avg_weight
        total_volume_weight = df_grouped.sum_volume * df_grouped.avg_avg_weight

        # Act
        result_price = h.get_avg_price()

        # Assert
        assert result_price != 0
        assert result_price == (total_price.sum() / total_volume_weight.sum())

    @patch('pandas.read_sql_query')
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_get_avg_price_by_banana(self, conn, mock_read_sql_query, load_daily_tran_fixtures_of_banana):
        # Arrange
        df = self.get_daily_trans_df()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})
        df_grouped = h.df_with_group_by_date

        # Act
        result_price = h.get_avg_price()

        # Assert
        assert result_price != 0
        assert result_price == df_grouped.avg_price.mean()

    @patch('pandas.read_sql_query')
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_get_avg_volume_by_eggplant(self, conn, mock_read_sql_query, load_daily_tran_fixtures_of_eggplant):
        # Arrange
        df = self.get_daily_trans_df()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})
        df_grouped = h.df_with_group_by_date

        # Act
        result_volume = h.get_avg_volume()

        # Assert
        assert result_volume != 0
        assert result_volume == df_grouped.sum_volume.mean()

    @patch('pandas.read_sql_query')
    @patch('apps.dailytrans.reports.dailyreport.Database.get_db_connection', return_value=MagicMock)
    def test_get_avg_volume_by_banana(self, conn, mock_read_sql_query, load_daily_tran_fixtures_of_banana):
        # Arrange
        df = self.get_daily_trans_df()
        mock_read_sql_query.return_value = df
        h = DailyTranHandler('', {})
        df_grouped = h.df_with_group_by_date

        # Act
        result_volume = h.get_avg_volume()

        # Assert
        assert result_volume != 0
        assert result_volume == df_grouped.sum_volume.mean()
