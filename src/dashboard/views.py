import itertools
import pickle
from datetime import datetime, timedelta
from functools import wraps

import requests
from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import (
    redirect,
)
from django.utils import translation
from django.views.generic.base import TemplateView

from apps.configs.models import (
    Config,
    Chart,
    Type,
    Festival,
    FestivalName,
    AbstractProduct,
    Last5YearsItems,
)
from apps.watchlists.models import Watchlist
from dashboard.caches import redis_instance as cache
from dashboard.celery import app
from .utils import (
    jarvismenu_extra_context,
    product_selector_ui_extra_context,
    watchlist_base_chart_tab_extra_context,
    chart_tab_extra_context,
    watchlist_base_chart_contents_extra_context,
    product_selector_base_extra_context,
    watchlist_base_integration_extra_context,
    product_selector_base_integration_extra_context,
)


def login_required(view):
    """
    Custom login_required to handle ajax request
    Check user is login and is_active
    """

    @wraps(view)
    def inner(request, *args, **kwargs):
        if not request.user.is_authenticated() or not request.user.is_active:
            if request.is_ajax():
                # if is ajax return 403
                return JsonResponse({'login_url': settings.LOGIN_URL}, status=403)
            else:
                # if not ajax redirect login page
                return redirect(settings.LOGIN_URL)
        return view(request, *args, **kwargs)

    return inner


class LoginRequiredMixin(object):
    @classmethod
    def as_view(cls, **kwds):
        return login_required(super().as_view(**kwds))


class BrowserNotSupport(TemplateView):
    redirect_field_name = 'redirect_to'
    template_name = 'browser-not-support.html'


class Index(LoginRequiredMixin, TemplateView):
    redirect_field_name = 'redirect_to'
    template_name = 'index.html'

    def get(self, request, *args, **kwargs):
        user_language = kwargs.get('lang')
        translation.activate(user_language)
        request.session[translation.LANGUAGE_SESSION_KEY] = user_language
        return super(Index, self).get(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super(Index, self).get_context_data(**kwargs)

        # watchlist options use for watchlist shortcut render
        watchlists = Watchlist.objects.order_by('id').all()

        if not self.request.user.info.watchlist_viewer:
            watchlists = watchlists.exclude(watch_all=True)

        context['watchlists'] = watchlists

        # filter watchlist item or use default
        watchlist_id = kwargs.get('wi')
        watchlist = Watchlist.objects.filter(id=watchlist_id).first() or Watchlist.objects.get(is_default=True)
        context['user_watchlist'] = watchlist

        # classify config into different folder manually
        configs = watchlist.related_configs()

        # render config as folder on left panel menu(1st level)
        # 合計項目
        context['totals'] = configs.filter(id__in=[2, 3, 4])

        # 農產品
        context['agricultures'] = configs.filter(id__in=[1, 5, 6, 7])

        # 畜禽產品
        context['livestocks'] = configs.filter(id__in=[8, 9, 10, 11, 12, 14])

        # 漁產品
        if configs.filter(id=13).first():
            context['fisheries'] = configs.get(id=13).first_level_products(watchlist=watchlist)

        return context

    def render_to_response(self, context, **response_kwargs):
        return super(Index, self).render_to_response(context, **response_kwargs)


class About(LoginRequiredMixin, TemplateView):
    redirect_field_name = 'redirect_to'
    template_name = 'ajax/about.html'


class DailyReport(LoginRequiredMixin, TemplateView):
    redirect_field_name = 'redirect_to'
    template_name = 'ajax/daily-report.html'


class FestivalReport(LoginRequiredMixin, TemplateView):
    redirect_field_name = 'redirect_to'
    template_name = 'ajax/festival-report.html'

    # roc_year_sel='all'

    def post(self, request, **kwargs):
        self.kwargs['POST'] = request.POST
        self.roc_year_sel = self.kwargs['POST']['roc_year_sel']
        return self.render_to_response(self.get_context_data())

    def get_context_data(self, **kwargs):
        context = super(FestivalReport, self).get_context_data(**kwargs)
        # 節日名稱
        festival_list = FestivalName.objects.filter(enable=True).order_by('id')
        context['festival_list'] = festival_list
        # 年度
        roc_year_set = set()
        for y in Festival.objects.values('roc_year'):
            roc_year_set.add(y['roc_year'])
        context['roc_year_list'] = sorted(roc_year_set, reverse=True)
        # 自選農產品清單
        item_list = AbstractProduct.objects.filter(type=1, track_item=True) | AbstractProduct.objects.filter(
            id__range=[130001, 130005], type=2, track_item=True) | AbstractProduct.objects.filter(
            id__range=[90008, 90016], type=2, track_item=True) | AbstractProduct.objects.filter(
            id__range=[100004, 100006], type=2, track_item=True) | AbstractProduct.objects.filter(
            id__range=[110003, 110006], type=2, track_item=True)  # 批發品項+產地(牛5,雞8,鴨3,鵝4)

        context['item_list'] = item_list
        return context


def sort_items(items_list: list):
    # manually sort items
    new_fish = items_list.pop(84)

    # 龍虎斑(1174)(批發)
    i = items_list.pop(90)
    items_list.insert(84, i)

    i = items_list.pop(92)
    items_list.insert(74, i)

    i = items_list.pop(7)
    items_list.insert(66, i)

    i = items_list.pop(15)
    items_list.insert(65, i)

    i = items_list.pop(13)
    items_list.insert(65, i)

    i = items_list.pop(0)
    items_list.insert(71, i)

    i = items_list.pop(0)
    items_list.insert(71, i)

    i = items_list.pop(1)
    items_list.insert(0, i)

    # 花椰菜, 胡瓜, 花胡瓜, 絲瓜, 茄子
    i = items_list.pop(5)
    items_list.insert(23, i)

    i = items_list.pop(5)
    items_list.insert(23, i)

    i = items_list.pop(5)
    items_list.insert(23, i)

    i = items_list.pop(5)
    items_list.insert(23, i)

    i = items_list.pop(5)
    items_list.insert(23, i)

    items_list.insert(85, new_fish)


class Last5YearsReport(LoginRequiredMixin, TemplateView):
    redirect_field_name = 'redirect_to'
    template_name = 'ajax/last5years-report.html'

    def post(self, request, **kwargs):
        self.kwargs['POST'] = request.POST
        self.roc_year_sel = self.kwargs['POST']['item_id_list']
        return self.render_to_response(self.get_context_data())

    def get_context_data(self, **kwargs):
        context = super(Last5YearsReport, self).get_context_data(**kwargs)
        all_items = cache.get(Last5YearsItems.LAST5_YEARS_ITEMS_CACHE_KEY)

        all_items = self._get_items() if all_items is None else pickle.loads(all_items)
        context['items_list'] = all_items

        return context

    @staticmethod
    def _get_items():
        """
        Get last 5 years items from database and store in cache
        """

        result = {}

        # 品項
        qs = Last5YearsItems.objects.filter(enable=True).order_by('sort_value')

        # because qs has duplicate items, so we need to remove duplicate items
        items_list = []

        for i in qs:
            if i not in items_list:
                items_list.append(i)

        for i in items_list:
            pids = i.product_id.all()
            sources = i.source.all()
            pid_list = [str(p.id) for p in pids]
            source_list = [str(s.id) for s in sources]
            pid = ','.join(pid_list)
            source = ','.join(source_list)

            result[i.name] = {'product_id': pid, 'source': source}

        # store in cache
        cache.set(Last5YearsItems.LAST5_YEARS_ITEMS_CACHE_KEY, result, dump=True)

        return result


class ProductSelector(LoginRequiredMixin, TemplateView):
    redirect_field_name = 'redirect_to'
    template_name = 'ajax/product-selector.html'

    def get_context_data(self, **kwargs):
        context = super(ProductSelector, self).get_context_data(**kwargs)
        context['configs'] = Config.objects.order_by('id')
        context['types'] = Type.objects.order_by('id')
        return context


class ProductSelectorUI(LoginRequiredMixin, TemplateView):
    redirect_field_name = 'redirect_to'
    template_name = 'ajax/product-selector-ui.html'

    def post(self, request, **kwargs):
        self.kwargs['POST'] = request.POST
        return self.render_to_response(self.get_context_data())

    def get_context_data(self, **kwargs):
        context = super(ProductSelectorUI, self).get_context_data(**kwargs)
        extra_context = product_selector_ui_extra_context(self)
        context.update(extra_context)
        return context


class JarvisMenu(LoginRequiredMixin, TemplateView):
    redirect_field_name = 'redirect_to'
    template_name = 'ajax/jarvismenu.html'

    def get_context_data(self, **kwargs):
        context = super(JarvisMenu, self).get_context_data(**kwargs)
        extra_context = jarvismenu_extra_context(self)
        context.update(extra_context)
        return context


class ChartTabs(LoginRequiredMixin, TemplateView):
    redirect_field_name = 'redirect_to'
    template_name = 'ajax/chart-tab.html'
    watchlist_base = False

    def get_context_data(self, **kwargs):
        context = super(ChartTabs, self).get_context_data(**kwargs)
        if self.watchlist_base:
            extra_context = watchlist_base_chart_tab_extra_context(self)
        else:
            extra_context = chart_tab_extra_context(self)
        context.update(extra_context)
        return context


class ChartContents(LoginRequiredMixin, TemplateView):
    redirect_field_name = 'redirect_to'
    no_data = False  # custom
    watchlist_base = False
    product_selector_base = False

    def get_template_names(self):
        if self.no_data:
            return 'ajax/no-data.html'
        else:
            chart_id = self.kwargs.get('ci')
            chart = Chart.objects.get(id=chart_id)
            return chart.template_name

    def post(self, request, **kwargs):
        self.kwargs['POST'] = request.POST
        return self.render_to_response(self.get_context_data())

    def get_context_data(self, **kwargs):
        context = super(ChartContents, self).get_context_data(**kwargs)
        if self.watchlist_base:
            extra_context = watchlist_base_chart_contents_extra_context(self)
            context.update(extra_context)
        elif self.product_selector_base:
            extra_context = product_selector_base_extra_context(self)
            context.update(extra_context)

        # no data checking, if series_options is empty, render no-data template
        if not context['series_options']:
            self.no_data = True

        return context


class IntegrationTable(LoginRequiredMixin, TemplateView):
    redirect_field_name = 'redirect_to'
    no_data = False  # custom
    to_init = True  # custom  # default is True
    watchlist_base = False
    product_selector_base = False

    def get_template_names(self):
        # set template_name if no assign yet

        if self.no_data:
            return 'ajax/no-data.html'

        if self.to_init:
            return 'ajax/integration-panel.html'
        else:
            return 'ajax/integration-row.html'

    def post(self, request, **kwargs):
        self.kwargs['POST'] = request.POST
        return self.render_to_response(self.get_context_data())

    def get_context_data(self, **kwargs):
        context = super(IntegrationTable, self).get_context_data(**kwargs)
        if self.watchlist_base:
            extra_context = watchlist_base_integration_extra_context(self)
            context.update(extra_context)
        elif self.product_selector_base:
            extra_context = product_selector_base_integration_extra_context(self)
            context.update(extra_context)

        # no data checking, if series_options or option is empty, render no-data template
        if self.to_init:
            if not context['series_options']:
                self.no_data = True
        else:
            if not context['option']:
                self.no_data = True

        return context


@login_required
def get_celery_task_schedule(request):
    data = request.GET or request.POST
    task_name = data.get('taskName')
    task_key = data.get('taskKey')

    if not task_key or not task_name:
        return JsonResponse({'error': 'Invalid parameters'})
    
    try:
        url = f'http://apsvp-flower:5555/api/tasks?taskname={task_name}&limit=2'
        response = requests.get(url)
    except Exception:
        return JsonResponse({'error': 'Failed to get data from flower server'})

    try:
        if response.status_code == 200 and response.json():
            d = dict(response.json())
            dict_prev_task = d.popitem()[1]
            try:
                dict_curr_task = d.popitem()[1]
            except Exception:
                dict_curr_task = dict_prev_task

            if isinstance(dict_prev_task, dict) or isinstance(dict_curr_task, dict):
                if dict_curr_task.get('state') == 'SUCCESS':
                    original_datetime = datetime.fromtimestamp(dict_curr_task.get('succeeded')) + timedelta(hours=-8)
                    succeeded = ((datetime.fromtimestamp(dict_curr_task.get('succeeded')) + timedelta(hours=-8))
                                 .strftime('%Y/%m/%d %H:%M:%S'))
                else:
                    original_datetime = datetime.fromtimestamp(dict_prev_task.get('succeeded')) + timedelta(hours=-8)
                    succeeded = ((datetime.fromtimestamp(dict_prev_task.get('succeeded')) + timedelta(hours=-8))
                                 .strftime('%Y/%m/%d %H:%M:%S'))

                if dict_curr_task.get('state') == 'SUCCESS':
                    return JsonResponse({
                        'succeeded': succeeded,
                        'state': dict_curr_task.get('state'),
                        'nextTime': get_task_next_time(task_key, original_datetime),
                    })
                elif dict_curr_task.get('state') == 'STARTED':
                    return JsonResponse({
                        'state': dict_curr_task.get('state'),
                        'succeeded': succeeded,
                    })

        return JsonResponse({'error': 'No data found'})
    except Exception as e:
        return JsonResponse({'error': str(e)})


def get_task_next_time(task_key: str, original_datetime: datetime):
    schedule = app.conf.beat_schedule[task_key]['schedule']
    hours = list(schedule.hour)
    minutes = list(schedule.minute)

    for hour, minute in itertools.product(hours, minutes):
        dt = datetime(
            year=original_datetime.year,
            month=original_datetime.month,
            day=original_datetime.day,
            hour=hour,
            minute=minute,
            second=0,
            microsecond=0
        )

        if dt > original_datetime:
            if 'feed' not in task_key:
                if original_datetime.weekday() == 5:
                    dt = dt + timedelta(days=2)
                elif original_datetime.weekday() == 6:
                    dt = dt + timedelta(days=1)

                return dt.strftime('%Y/%m/%d %H:%M:%S')

            return dt.strftime('%Y/%m/%d %H:%M:%S')

    # friday
    if original_datetime.weekday() == 4:
        next_time = original_datetime + timedelta(days=3)
    # saturday
    elif original_datetime.weekday() == 5:
        next_time = original_datetime + timedelta(days=2)
    # sunday to thursday
    else:
        next_time = original_datetime + timedelta(days=1)

    return next_time.replace(hour=hours[0], minute=minutes[0], second=0, microsecond=0).strftime('%Y/%m/%d %H:%M:%S')
