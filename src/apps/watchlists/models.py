import pickle
from typing import List, Union

from django.conf import settings
from django.db.models import (
    Model,
    CASCADE,
    SET_NULL,
    CharField,
    BooleanField,
    DateTimeField,
    ForeignKey,
    ManyToManyField,
    FloatField,
    QuerySet,
    TextField,
    DateField,
    PositiveIntegerField,
    Q,
)
from django.utils import timezone
from django.utils.translation import ugettext_lazy as _

from apps.configs.models import Config, AbstractProduct
from dashboard.caches import redis_instance as cache

COMPARATOR_CHOICES = [
    ('__lt__', _('<')),
    ('__lte__', _('<=')),
    ('__gt__', _('>')),
    ('__gte__', _('>=')),
]

COLOR_CHOICES = [
    ('default', 'Default'),
    ('info', 'Info'),
    ('success', 'Success'),
    ('warning', 'Warning'),
    ('danger', 'Danger'),
]

RELATED_CONFIGS_CACHE_KEY = "watchlist{watchlist_id}_related_configs"
CHILDREN_CACHE_KEY = "watchlist{watchlist_id}_children"
FILTER_BY_PRODUCT_CACHE_KEY = "product{product_id}_filter_by_product"


class Watchlist(Model):
    name = CharField(max_length=120, unique=True, verbose_name=_('Name'))
    user = ForeignKey(settings.AUTH_USER_MODEL, null=True, on_delete=SET_NULL, verbose_name=_('User'))
    is_default = BooleanField(default=False, verbose_name=_('Is Default'))
    watch_all = BooleanField(default=False, verbose_name=_('Watch All'))
    start_date = DateField(auto_now=False, default=timezone.now().today, verbose_name=_('Start Date'))
    end_date = DateField(auto_now=False, default=timezone.now().today, verbose_name=_('End Date'))
    create_time = DateTimeField(auto_now_add=True, null=True, blank=True, verbose_name=_('Updated'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))

    class Meta:
        verbose_name = _('Watchlist')
        verbose_name_plural = _('Watchlists')

    def save(self, *args, **kwargs):
        # set false if default watchlist exist
        if self.is_default:
            try:
                watchlist = Watchlist.objects.get(is_default=True)

                if self != watchlist:
                    watchlist.is_default = False
                    watchlist.save()
            except Watchlist.DoesNotExist:
                pass

        # remove watch_all=True list before add one
        if self.watch_all:
            try:
                watchlist = Watchlist.objects.get(watch_all=True)
                if self != watchlist:
                    watchlist.delete()
            except Watchlist.DoesNotExist:
                pass

        super(Watchlist, self).save(*args, **kwargs)

    def __str__(self):
        return str(self.name)

    def __unicode__(self):
        return str(self.name)

    def children(self):
        """
        得到該監控清單底下的所有監控品項(WatchListItems)
        """

        cache_key = CHILDREN_CACHE_KEY.format(watchlist_id=self.id)
        items = cache.get(cache_key)

        if items is None:
            items = WatchlistItem.objects.filter(parent=self)
            cache.set(cache_key, items, dump=True)
        else:
            items = pickle.loads(items)

        return items

    def related_configs(self):
        """
        得到特定監控清單的所有品項分類(Config)
        """

        configs = cache.get(RELATED_CONFIGS_CACHE_KEY.format(watchlist_id=self.id))

        if configs is None:
            ids = self.children().values_list('product__config__id', flat=True).distinct()
            configs = Config.objects.filter(id__in=ids).order_by('id')

            cache.set(RELATED_CONFIGS_CACHE_KEY.format(watchlist_id=self.id), configs, dump=True)
        else:
            configs = pickle.loads(configs)

        return configs

    @property
    def related_product_ids(self):
        """
        得到所有監控品項(WatchlistItems)的所有階層(children & parents)品項(AbstractProducts) ID
        """

        ids = []

        for child in self.children():
            item: WatchlistItem = child
            ids = ids + list(item.product.related_product_ids)

        return ids


class WatchlistItemQuerySet(QuerySet):
    def filter_by_product(self, **kwargs):
        """
        for case like WatchlistItem.objects.filter(parent=self).filter_by_product(product__id=1)
        """

        product: Union[AbstractProduct, None] = (
                kwargs.get('product')
                or AbstractProduct.objects.filter(id=kwargs.get('product__id')).first()
        )

        if product:
            cache_key = FILTER_BY_PRODUCT_CACHE_KEY.format(product_id=product.id)
            items = cache.get(cache_key)

            if items is None:
                # 簡而言之，過濾出 X 品項，或是向上找出 parent 為 X 品項的子品項
                items = self.filter(
                    Q(product=product)
                    | Q(product__parent=product)
                    | Q(product__parent__parent=product)
                    | Q(product__parent__parent__parent=product)
                    | Q(product__parent__parent__parent__parent=product)
                )

                cache.set(cache_key, items, dump=True)
            else:
                items = pickle.loads(items)

            return items

        return self.none()

    def get_unit(self):
        """
        for case like QuerySet.get_unit()
        if QuerySet products has multiple types, search for parent unit by config.type_level
        if single type, return first product unit
        limit: same type of products(in single product chain) only support same unit
        """

        config = self.first().product.config
        if self.values('product__type').count() <= 0:
            return self.first().unit

        if config.type_level == 1:
            unit = config.first_level_products().first().unit
        elif config.type_level == 2:
            unit = config.first_level_products().first().children().first().unit
        else:
            raise NotImplementedError('Can not locate product to access Unit object')
        return unit


class WatchlistItem(Model):
    product = ForeignKey('configs.AbstractProduct', on_delete=CASCADE, verbose_name=_('Product'))
    sources = ManyToManyField('configs.Source', verbose_name=_('Source'))
    parent = ForeignKey('watchlists.Watchlist', null=True, on_delete=CASCADE, verbose_name=_('Parent'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))

    objects = WatchlistItemQuerySet.as_manager()

    class Meta:
        verbose_name = _('Watchlist Item')
        verbose_name_plural = _('Watchlist Items')

    def __str__(self):
        return str(self.product.name)

    def __unicode__(self):
        return str(self.product.name)


class MonitorProfile(Model):
    product = ForeignKey('configs.AbstractProduct', on_delete=CASCADE, verbose_name=_('Product'))
    watchlist = ForeignKey('watchlists.Watchlist', on_delete=CASCADE, verbose_name=_('Watchlist'))
    type = ForeignKey('configs.Type', null=True, blank=True, on_delete=SET_NULL, verbose_name=_('Type'))
    price = FloatField(verbose_name=_('Price'))
    comparator = CharField(max_length=6, default='__lt__', choices=COMPARATOR_CHOICES, verbose_name=_('Comparator'))
    color = CharField(max_length=20, default='danger', choices=COLOR_CHOICES, verbose_name=_('Color'))
    info = TextField(null=True, blank=True, verbose_name=_('Monitor Info'))
    action = TextField(null=True, blank=True, verbose_name=_('Action'))
    period = TextField(null=True, blank=True, verbose_name=_('Period'))
    is_active = BooleanField(default=False, verbose_name=_('Is Active'))
    months = ManyToManyField('configs.Month', verbose_name=_('Monitor Months'))
    always_display = BooleanField(default=False, verbose_name=_('Always Display'))
    row = PositiveIntegerField(null=True, blank=True, verbose_name=_('Row'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))

    class Meta:
        verbose_name = _('Monitor Profile')
        verbose_name_plural = _('Monitor Profile')

    def __str__(self):
        return str('product: %s, watchlist: %s, price: %s' % (self.product.name, self.watchlist.name, self.price))

    def __unicode__(self):
        return str('product: %s, watchlist: %s, price: %s' % (self.product.name, self.watchlist.name, self.price))

    def sibling(self):
        return MonitorProfile.objects.exclude(id=self.id).filter(type=self.type, product=self.product, watchlist=self.watchlist)

    def watchlist_items(self):
        return WatchlistItem.objects.filter(product__parent=self.product)

    def product_list(self) -> List[AbstractProduct]:
        """
        此 method 只會由 `apps.dailytrans.dailyreport.py` 使用，用於產製日報表的品項
        """
        
        items = WatchlistItem.objects.filter_by_product(product=self.product).filter(parent=self.watchlist)
        # items = self.watchlist_items().filter(parent=self.watchlist)

        return [item.product for item in items] if items else [self.product]

    def sources(self):
        """
        此 method 只會由 `apps.dailytrans.dailyreport.py` 使用，用於產製日報表時，取得品項的來源
        """

        sources = []
        items = WatchlistItem.objects.filter_by_product(product=self.product).filter(parent=self.watchlist)

        for i in items:
            item: WatchlistItem = i
            sources += item.sources.all()

        return list(set(sources))

    def active_compare(self, price):
        if self.comparator == '__gt__':
            return price > self.price
        if self.comparator == '__gte__':
            return price >= self.price
        if self.comparator == '__lt__':
            return price < self.price
        if self.comparator == '__lte__':
            return price <= self.price

    @property
    def format_price(self):
        d = dict(COMPARATOR_CHOICES)
        comparator = d[str(self.comparator)]
        return '{0}{1:g}{2}'.format(comparator, self.price, self.product.unit.price_unit)

    @property
    def less(self):
        return ['__lt__', '__lte__']

    @property
    def greater(self):
        return ['__gt__', '__gte__']

    @property
    def price_range(self):

        low_price = None
        up_price = None

        if self.comparator in self.less:
            sibling = self.sibling().filter(comparator__in=self.less).order_by('price')
            last_obj = sibling.filter(price__lt=self.price).last()
            if last_obj:
                low_price = last_obj.price
            else:
                low_price = 0
            up_price = self.price

        if self.comparator in self.greater:
            sibling = self.sibling().filter(comparator__in=self.greater).order_by('price')
            next_obj = sibling.filter(price__gt=self.price).first()
            if next_obj:
                up_price = next_obj.price
            else:
                up_price = 2 ** 50
            low_price = self.price

        return [low_price, up_price]

    @property
    def low_price(self):
        return self.price_range[0]

    @property
    def up_price(self):
        return self.price_range[1]
