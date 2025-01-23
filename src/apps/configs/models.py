import pickle

from django.db.models import (
    Model,
    QuerySet,
    SET_NULL,
    CharField,
    DateTimeField,
    ForeignKey,
    ManyToManyField,
    IntegerField,
    BooleanField,
    Q,
)
from django.db.models.signals import post_save
from django.utils import timezone
from django.utils.translation import ugettext_lazy as _
from model_utils.managers import InheritanceManager

from dashboard.caches import redis_instance as cache

CHILDREN_CACHE_KEY = "watchlist{watchlist_id}_product{product_id}_children"
CHILDREN_ALL_CACHE_KEY = "product{product_id}_children_all"
TYPES_CACHE_KEY = "product{product_id}_types"
TYPE_CACHE_KEY = "product{product_id}_type"
SOURCES_WITH_WATCHLIST_CACHE_KEY = "product{product_id}_sources_with_watchlist"
SOURCES_CACHE_KEY = "product{product_id}_sources"
FIRST_LEVEL_PRODUCTS_WITH_WATCHLIST_CACHE_KEY = "watchlist{watchlist_id}_config{config_id}_lv1_products"
FIRST_LEVEL_PRODUCTS_CACHE_KEY = "config{config_id}_lv1_products"
PRODUCTS_CACHE_KEY = "config{config_id}_products"
TYPES_FILTER_BY_WATCHLIST_ITEMS = "types_filter_by_watchlist_items{hash_id}"


class AbstractProduct(Model):
    name = CharField(max_length=50, verbose_name=_('Name'))
    code = CharField(max_length=50, verbose_name=_('Code'))
    config = ForeignKey('configs.Config', null=True, on_delete=SET_NULL, verbose_name=_('Config'))
    type = ForeignKey('configs.Type', null=True, on_delete=SET_NULL, verbose_name=_('Type'))
    unit = ForeignKey('configs.Unit', null=True, blank=True, verbose_name=_('Unit'))
    parent = ForeignKey('self', null=True, blank=True, on_delete=SET_NULL, verbose_name=_('Parent'))
    track_item = BooleanField(default=True, verbose_name=_('Track Item'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))

    objects = InheritanceManager()

    class Meta:
        verbose_name = _('Abstract Product')
        verbose_name_plural = _('Abstract Products')
        ordering = ('id',)

    def __str__(self):
        return str(self.name)

    def __unicode__(self):
        return str(self.name)

    def get_cache_key(self, watchlist=None):
        return (
            CHILDREN_CACHE_KEY.format(watchlist_id=watchlist.id, product_id=self.id)
            if watchlist
            else f"product{self.id}_children"
        )

    def children(self, watchlist=None):
        """
        取得某個品項底下的第一層所有子品項(parent=self)
        """

        cache_key = self.get_cache_key(watchlist)
        products = cache.get(cache_key)

        if products is None:
            products = AbstractProduct.objects.filter(parent=self).select_subclasses()

            if watchlist and not watchlist.watch_all:
                products = products.filter(id__in=watchlist.related_product_ids)

            cache.set(cache_key, products.order_by('id'), dump=True)
        else:
            products = pickle.loads(products)

        return products.order_by('id')

    def children_all(self):
        """
        取得某個品項底下所有層別的所有子品項
        """

        cache_key = CHILDREN_ALL_CACHE_KEY.format(product_id=self.id)
        products = cache.get(cache_key)

        if products is None:
            # 簡而言之，過濾出 X 品項，或是向上找出 parent 為 X 品項的子品項
            products = AbstractProduct.objects.filter(
            Q(parent=self)
            | Q(parent__parent=self)
            | Q(parent__parent__parent=self)
            | Q(parent__parent__parent__parent=self)
            | Q(parent__parent__parent__parent__parent=self)
        ).select_subclasses().order_by('id')

            cache.set(cache_key, products, dump=True)
        else:
            products = pickle.loads(products)

        return products

    def types(self, watchlist=None):
        if self.has_child:
            cache_key = TYPES_CACHE_KEY.format(product_id=self.id)
            types = cache.get(cache_key)

            if types is None:
                products = self.children()

                if watchlist and not watchlist.watch_all:
                    products = products.filter(id__in=watchlist.related_product_ids)

                type_ids = products.values_list('type__id', flat=True)
                types = Type.objects.filter(id__in=type_ids)

                cache.set(cache_key, types, dump=True)
            else:
                types = pickle.loads(types)

            return types
        elif self.type:
            cache_key = TYPE_CACHE_KEY.format(product_id=self.id)
            _type = cache.get(cache_key)

            if _type is None:
                _type = Type.objects.filter(id=self.type.id)
                cache.set(cache_key, _type, dump=True)
            else:
                _type = pickle.loads(_type)

            return _type
        else:
            return self.objects.none()

    def sources(self, watchlist=None):
        """
        得到某個品項的所有來源或是監控品項的來源

        :param watchlist: Watchlist: 監控清單
        """

        if watchlist:
            return watchlist.children().filter(product__id=self.id).first().sources.all()
        else:
            return Source.objects.filter(configs__id__exact=self.config.id).filter(type=self.type).order_by('id')

    @property
    def to_direct(self):
        """
        set true to navigate at front end
        """
        type_level = self.config.type_level
        return self.level >= type_level

    @property
    def has_source(self):
        return self.sources().count() > 0

    @property
    def has_child(self):
        return self.children().count() > 0

    @property
    def level(self):
        level = 1

        lock = False
        product = self

        while not lock:

            if product.parent is not None:
                product = product.parent
                level = level + 1
            else:
                lock = True

        return level

    @property
    def related_product_ids(self):
        """
        找出指定品項的所有階層(parents and children) ID。

        e.g.:
        children: 落花生(帶殼) -> None
        parents:  落花生(帶殼) -> 落花生 -> 花果菜類
        ids = [落花生(帶殼).id, 落花生.id, 花果菜類.id]
        """

        ids = list(self.children().values_list('id', flat=True))

        lock = False
        product = self

        while not lock:
            # add self id
            ids.append(product.id)

            if product.parent is not None:
                product = product.parent
            else:
                lock = True

        return ids


class Config(Model):
    name = CharField(max_length=50, unique=True, verbose_name=_('Name'))
    code = CharField(max_length=50, unique=True, null=True, verbose_name=_('Code'))
    charts = ManyToManyField('configs.Chart', blank=True, verbose_name=_('Chart'))
    type_level = IntegerField(choices=[(1, 1), (2, 2)], default=1, verbose_name=_('Type Level'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))

    class Meta:
        verbose_name = _('Config')
        verbose_name_plural = _('Configs')

    def __str__(self):
        return str(self.name)

    def __unicode__(self):
        return str(self.name)

    def get_cache_key(self, watchlist=None):
        return (
            FIRST_LEVEL_PRODUCTS_WITH_WATCHLIST_CACHE_KEY.format(watchlist_id=watchlist.id, config_id=self.id)
            if watchlist
            else FIRST_LEVEL_PRODUCTS_CACHE_KEY.format(config_id=self.id)
        )

    def products(self):
        cache_key = PRODUCTS_CACHE_KEY.format(config_id=self.id)
        products = cache.get(cache_key)

        if products is None:
            # Use select_subclasses() to return subclass instance
            products = AbstractProduct.objects.filter(config=self).select_subclasses().order_by('id')
            cache.set(cache_key, products, dump=True)
        else:
            products = pickle.loads(products)

        return products

    def first_level_products(self, watchlist=None):
        """
        此 method 主要用於取得左側選單取得第一層品項，幾乎為固定品項:
        合計項目: 蔬菜-批發合計、水果-批發合計、花卉-批發合計、毛豬合計
        農產品: 糧價、蔬菜、水果、花卉
        畜禽產品: 毛豬、羊、雞、鴨、牛
        漁產品: 養殖類、蝦類、貝類

        主要由 `dashboard.views.Index` 與 `apps.dashboard.views.JarvisMenu` 間接呼叫
        """

        # using redis to reduce database handling and using pickle to serialize the data
        cache_key = self.get_cache_key(watchlist)
        products = cache.get(cache_key)

        if products is None:
            # Use select_subclasses() to return subclass instance
            products = AbstractProduct.objects.filter(config=self).filter(parent=None).select_subclasses()

            if watchlist and not watchlist.watch_all:
                products = products.filter(id__in=watchlist.related_product_ids)

            cache.set(cache_key, products.order_by('id'), dump=True)
        else:
            products = pickle.loads(products)

        return products.order_by('id')

    def types(self):
        products_qs = self.products().values('type').distinct()
        types_ids = [p['type'] for p in products_qs]

        return Type.objects.filter(id__in=types_ids)

    @property
    def to_direct(self):
        """
        set true to navigate at front end
        """
        return False


class SourceQuerySet(QuerySet):
    """ for case like Source.objects.filter(config=config).filter_by_name(name) """
    def filter_by_name(self, name):
        if not isinstance(name, str):
            raise TypeError
        name = name.replace('台', '臺')

        return self.filter(name=name) or self.filter(alias__icontains=name)


class Source(Model):
    name = CharField(max_length=50, verbose_name=_('Name'))
    alias = CharField(max_length=255, null=True, blank=True, verbose_name=_('Alias'))
    code = CharField(max_length=50, null=True, blank=True, verbose_name=_('Code'))
    configs = ManyToManyField('configs.Config', verbose_name=_('Config'))
    type = ForeignKey('configs.Type', null=True, blank=True, on_delete=SET_NULL, verbose_name=_('Type'))
    enable = BooleanField(default=True, verbose_name=_('Enabled'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))

    objects = SourceQuerySet.as_manager()

    class Meta:
        verbose_name = _('Source')
        verbose_name_plural = _('Sources')
        ordering = ('id',)

    def __str__(self):
        flat = self.configs_flat
        return f'{str(self.name)}({flat}-{self.type.name})'

    def __unicode__(self):
        flat = self.configs_flat
        return f'{str(self.name)}({flat}-{self.type.name})'

    @property
    def simple_name(self):
        return self.name.replace('臺', '台')

    @property
    def configs_flat(self):
        return ','.join(config.name for config in self.configs.all())

    @property
    def to_direct(self):
        """
        set true to navigate at front end
        """
        return True


class TypeQuerySet(QuerySet):
    def filter_by_watchlist_items(self, **kwargs):
        items = kwargs.get('watchlist_items')
        if not items:
            raise NotImplementedError

        ids = items.values_list('product__type__id', flat=True)
        cache_key = TYPES_FILTER_BY_WATCHLIST_ITEMS.format(hash_id=hash(str(ids)))
        types = cache.get(cache_key)

        if types is None:
            types = self.filter(id__in=ids)
            cache.set(cache_key, types, dump=True)
        else:
            types = pickle.loads(types)

        return types


class Type(Model):
    name = CharField(max_length=50, unique=True, verbose_name=_('Name'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))

    objects = TypeQuerySet.as_manager()

    class Meta:
        verbose_name = _('Type')
        verbose_name_plural = _('Types')

    def __str__(self):
        return str(self.name)

    def __unicode__(self):
        return str(self.name)

    def sources(self):
        return Source.objects.filter(type=self)

    @property
    def to_direct(self):
        """
        set true to navigate at front end
        """
        return True


class Unit(Model):
    price_unit = CharField(max_length=50, null=True, blank=True, verbose_name=_('Price Unit'))
    volume_unit = CharField(max_length=50, null=True, blank=True, verbose_name=_('Volume Unit'))
    weight_unit = CharField(max_length=50, null=True, blank=True, verbose_name=_('Weight Unit'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))

    class Meta:
        verbose_name = _('Unit')
        verbose_name_plural = _('Units')

    def __str__(self):
        return '%s, %s, %s' % (self.price_unit, self.volume_unit, self.update_time)

    def __unicode__(self):
        return '%s, %s, %s' % (self.price_unit, self.volume_unit, self.update_time)

    def attr_list(self):
        lst = []
        for attr, value in self.__dict__.items():
            if attr in ['price_unit', 'volume_unit', 'weight_unit'] and value:
                lst.append((Unit._meta.get_field(attr).verbose_name.title(), value))
        return lst


class Chart(Model):
    name = CharField(max_length=120, unique=True, verbose_name=_('Name'))
    code = CharField(max_length=50, unique=True, null=True, verbose_name=_('Code'))
    template_name = CharField(max_length=255, verbose_name=_('Template Name'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))

    class Meta:
        verbose_name = _('Chart')
        verbose_name_plural = _('Charts')

    def __str__(self):
        return str(self.name)

    def __unicode__(self):
        return str(self.name)


class Month(Model):
    name = CharField(max_length=120, unique=True, verbose_name=_('Name'))

    class Meta:
        verbose_name = _('Month')
        verbose_name_plural = _('Months')

    def __str__(self):
        return str(self.name)

    def __unicode__(self):
        return str(self.name)


class Festival(Model):
    roc_year = CharField(max_length=3, default=timezone.now().year-1911, verbose_name=_('ROC Year'))
    name = ForeignKey('configs.FestivalName', null=True, blank=True, on_delete=SET_NULL, verbose_name=_('Name'))
    enable = BooleanField(default=True, verbose_name=_('Enabled'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))
    create_time = DateTimeField(auto_now_add=True, null=True, blank=True, verbose_name=_('Created'))

    class Meta:
        verbose_name = _('Festival')
        verbose_name_plural = _('Festivals')
        ordering = ('id',)

    def __str__(self):
        return self.roc_year + '_' + self.name.name

    def __unicode__(self):
        return self.roc_year + '_' + self.name.name


class FestivalName(Model):
    name = CharField(max_length=20, verbose_name=_('Name'))
    enable = BooleanField(default=True, verbose_name=_('Enabled'))
    lunarmonth = CharField(max_length=2, default='01', verbose_name=_('LunarMonth'))
    lunarday = CharField(max_length=2, default='01', verbose_name=_('LunarDay'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))
    create_time = DateTimeField(auto_now_add=True, null=True, blank=True, verbose_name=_('Created'))

    class Meta:
        verbose_name = _('FestivalName')
        ordering = ('id',)

    def __str__(self):
        return self.name

    def __unicode__(self):
        return self.name


class FestivalItems(Model):
    name = CharField(max_length=20, verbose_name=_('Name'))
    enable = BooleanField(default=True, verbose_name=_('Enabled'))
    order_sn = IntegerField(default=9, verbose_name=_('Order SN'))
    festivalname = ManyToManyField('configs.FestivalName', verbose_name=_('FestivalName'))
    product_id = ManyToManyField('configs.AbstractProduct', verbose_name=_('Product_id'))
    source = ManyToManyField('configs.Source', verbose_name=_('Source'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))
    create_time = DateTimeField(auto_now_add=True, null=True, blank=True, verbose_name=_('Created'))

    class Meta:
        verbose_name = _('FestivalItem')
        ordering = ('order_sn',)

    def __str__(self):
        return self.name

    def __unicode__(self):
        return self.name


class Last5YearsItems(Model):
    LAST5_YEARS_ITEMS_CACHE_KEY = 'last5_years_items'

    name = CharField(max_length=60, verbose_name=_('Name'))
    enable = BooleanField(default=True, verbose_name=_('Enabled'))
    product_id = ManyToManyField('configs.AbstractProduct', verbose_name=_('Product_id'))
    source = ManyToManyField('configs.Source', verbose_name=_('Source'))
    sort_value = IntegerField(blank=True, null=True, verbose_name=_('Sort Value'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))
    create_time = DateTimeField(auto_now_add=True, null=True, blank=True, verbose_name=_('Created'))

    class Meta:
        verbose_name = _('Last5YearsItems')

    def __str__(self):
        return self.name

    def __unicode__(self):
        return self.name


def instance_post_save(sender, instance, created, **kwargs):
    if kwargs.get('raw'):
        instance.save()
        return
    else:
        cache.delete_keys_by_model_instance(instance, Last5YearsItems, key=Last5YearsItems.LAST5_YEARS_ITEMS_CACHE_KEY)


post_save.connect(instance_post_save, sender=Last5YearsItems)