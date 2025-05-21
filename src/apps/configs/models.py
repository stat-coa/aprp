import pickle

from dashboard.caches import redis_instance as cache
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
from django.forms.models import model_to_dict
from django.utils import timezone
from django.utils.translation import ugettext_lazy as _
from model_utils.managers import InheritanceManager


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
    """
    name: 稉種(蓬萊)
    code: pt_2japt
    config: 糧價
    type: 批發
    unit: 元/公斤, None, None
    parent: 稉種(蓬萊)
    track_item: True
    name: 硬秈(在來)
    code: tsait
    config: 糧價
    type: None
    unit: 元/公斤, None, None
    parent: 白米
    track_item: False
    """
    name = CharField(max_length=50, verbose_name=_('Name'))
    code = CharField(max_length=50, verbose_name=_('Code'))
    config = ForeignKey('configs.Config', null=True, on_delete=SET_NULL, verbose_name=_('Config'))
    type = ForeignKey('configs.Type', null=True, on_delete=SET_NULL, verbose_name=_('Type'))
    unit = ForeignKey('configs.Unit', null=True, blank=True, verbose_name=_('Unit'))
    parent = ForeignKey('self', null=True, blank=True, on_delete=SET_NULL, verbose_name=_('Parent'))
    track_item = BooleanField(default=True, verbose_name=_('Track Item'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))

    objects = InheritanceManager() # 能讓父類別 QuerySet() 取出所有子類別實例

    class Meta:
        verbose_name = _('Abstract Product')
        verbose_name_plural = _('Abstract Products')
        ordering = ('id',) # , 不是多打，這是 tuple

    def __str__(self):
        return str(self.name)

    def get_cache_key(self, watchlist=None):
        return (
            f'watchlist{watchlist.id}_product{self.id}_children'

            if watchlist
            else f'product{self.id}_children'
        )

    def children(self, watchlist=None):
        """ 取得某個品項底下的第一層所有子品項 (parent=self)，並把結果用快取保存，就不用每次都透過資料庫取出子品項 """

        # 沒傳 watchlist : products{self.id}_children
        # 有傳 watchlist : watchlist{watchlist.id}_product{self.id}_children
        cache_key = self.get_cache_key(watchlist)
        products = cache.get(cache_key)

        # 抓取 self 第一層子品項， select_subclasses() 能讓 QuerySet() 在 SQL 取出父表和第一層所有子表
        # 就可以直接實例化子類別
        # 如果沒有加上 select_subclasses()，只會實例化父類 AbstractProduct，要再多查詢一次
        if products is None:
            products = AbstractProduct.objects.filter(parent=self).select_subclasses()

            # 有 watchlist 且 watch_all = False 時，會把查到的結果放進快取
            if watchlist and not watchlist.watch_all:
                products = products.filter(id__in=watchlist.related_product_ids)

            cache.set(cache_key, products.order_by('id'), dump=True)

        else:
            products = pickle.loads(products) # 反序列化去讀取快取內容

        return products.order_by('id')

    def children_all(self):
        """ 取得某個品項底下所有層別的所有子品項 """

        cache_key = f'product{self.id}_children_all'
        products = cache.get(cache_key)

        if products is None:
            # 簡而言之，過濾出 X 品項，或是向上找出 parent 為 X 品項的子品項 # todo
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
        """
        取得某個品項或是子品項的 Type，並把結果快取起來，整理流程有三個步驟:

        假如有子品項:
            1. 先抓第一層的子品項並取出 Type
            2. 回傳所有不重複的 Type
            3. 最終會回傳一個多筆 Type 的 QuerySet

        假如沒有子品項:
            1. 只抓自己的 Type
            2. 最終會回傳單筆 QuerySet

        假如沒有子品項也沒有 Type:
            1. 回傳空的 QuerySet
            2. 最終會回傳 QuerySet.none()
        """
        if self.has_child:
            cache_key = f'product{self.id}_types'
            types = cache.get(cache_key)

            if types is None:
                products = self.children()

                if watchlist and not watchlist.watch_all:
                    products = products.filter(id__in=watchlist.related_product_ids)

                # flat= True 能讓回傳結果變成一個 list，但是只能查詢一個欄位，同時取兩個以上欄位會 TypeError
                # [(3,), (5,), (5,), (7,)] -> [3, 5, 5, 7]
                type_ids = products.values_list('type__id', flat=True)
                types = Type.objects.filter(id__in=type_ids)

                # dump=True 保證 寫入前先 pickle，讀出後手動 unpickle
                # 讓快取內容安全又不會意外觸發額外查詢
                cache.set(cache_key, types, dump=True)
            else:
                types = pickle.loads(types)

            return types

        elif self.type:
            cache_key = f'product{self.id}_type'

            _type = cache.get(cache_key)

            if _type is None:
                _type = Type.objects.filter(id=self.type.id)
                cache.set(cache_key, _type, dump=True)
            else:
                _type = pickle.loads(_type)

            return _type

        else: # 沒有子品項也沒有 Type
            return self.objects.none() # 回傳一個空的 QuerySet

    def sources(self, watchlist=None):
        """
        得到某個品項的所有來源或是監控品項的來源

        :param watchlist: Watchlist: 監控清單
        """

        if watchlist:
            # watchlist 跟 source 建立了多對多的屬性名
            return watchlist.children().filter(product__id=self.id).first().sources.all()
        else:
            # exact 為 SQL 的 = (完全相等)
            # 沒有寫 __exact，底層也會自動當成 __exact 來處理
            return Source.objects.filter(configs__id=self.config.id).filter(type=self.type).order_by('id')

    @property
    def to_direct(self):
        """
        set true to navigate at front end
        決定前端是否顯示直接導覽或是立即查看的按鈕
        """
        type_level = self.config.type_level
        return self.level >= type_level

    @property
    def has_source(self):
        """ 判斷商品是否有來源，如果有來源則會顯示筆數 """
        return self.sources().count() > 0

    @property
    def has_child(self):
        """ 判斷這個商品是否有子品項，如果有子品項則會顯示筆數 """
        return self.children().count() > 0

    @property
    def level(self):
        """
        預設自己是最下層的子節點，product 會從自己開始往上爬，lock 會控制迴圈何時結束

        ex: product = self 會是子節點 B (id=3)
        根節點 (id=1)
          │
          └─ 子節點 A (id=2)
          │
          └─ 子節點 B (id=3)
        """
        level = 1
        product = self
        lock = False

        # 如果有父節點會把 product 換為父節點，每次 product = product.parent 會往上爬一層，直到自己成為父節點或是只有根節點
        # level 每次往上爬一層則會 +1，如果是 1 表示本身就是根節點
        while not lock:
            if product.parent is not None:
                product = product.parent
                level = level + 1
            else:
                lock = True # lock = True 表示到了根節點則跳出迴圈

        return level

    @property
    def related_product_ids(self):
        """
        找出指定品項的所有階層 (parents and children) ID

        e.g.:
        children: 落花生(帶殼) -> None
        parents:  落花生(帶殼) -> 落花生 -> 花果菜類
        ids = [落花生(帶殼).id, 落花生.id, 花果菜類.id]
        """

        ids = list(self.children().values_list('id', flat=True)) # 會先取得第一層的子品項 id 轉成 list
        lock = False
        product = self

        while not lock:
            # add self id
            ids.append(product.id)

            # 從 self 品項開始往上找父品項並加進去
            if product.parent is not None:
                product = product.parent
            else:
                lock = True # 父品項為 None 時結束

        return ids # ids = [落花生(帶殼).id, 落花生.id, 花果菜類.id]


class Config(Model):
    """
    name: 毛豬
    code: COG08
    charts: configs.Chart.None
    type_level: 1
    """
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

    def get_cache_key(self, watchlist=None):
        return (
            f'watchlist{watchlist.id}_config{self.id}_lv1_products'

            if watchlist
            else f'config{self.id}_lv1_products'
        )

    def products(self):
        cache_key = f'config{self.id}_products'

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
        """
        products_qs
        value('type') 會把 self.products() 轉成像 [{ 'type':3 }, { 'type': 5}], 的格式，只包含 type 欄位
        並確保同個 type id 只出現一次

        type_ids 為串列推導式
        products_qs 為 ex:  [{ 'type':3 }, { 'type': 5}]
        p 單獨用字典的形式拆開 ex: { 'type':3 } 跟 { 'type': 5}
        p['type'] ex: [3,5]

        等同用 for 迴圈:
        types_ids = []
        for p in products_qs:
        type_id = p['type']
        types_ids.append(type_id)
        """
        products_qs = self.products().values('type').distinct() # 確保同一個 type id 只會出現一次
        types_ids = [p['type'] for p in products_qs]

        return Type.objects.filter(id__in=types_ids)

    @property
    def to_direct(self):
        """
        set true to navigate at front end
        如果為 True 前端就會導向某個頁面
        預設為 False，如果想要在某些情況下導像，就需要透過子類別或是其他機制複寫這個屬性
        """
        return False


class SourceQuerySet(QuerySet):
    """ for case like Source.objects.filter(config=config).filter_by_name(name) """
    def filter_by_name(self, name):
        # 如果傳入的名稱不是字串，會有 TypeError 的提示
        if not isinstance(name, str):
            raise TypeError

        name = name.replace('台', '臺')

        # contains 為 SQL 的 LIKE '%Value%' 等同於「欄位包含這段文字」
        # i 為忽略大小寫， alias__icontains 意思為選出所有 alias 欄位中，包含 name 字串(不論大小寫)的資料
        return self.filter(name=name) or self.filter(alias__icontains=name)


class Source(Model):
    """
    name: 臺北一
    alias: None
    code: 104
    configs: configs.Config.None
    type: 批發
    enable: True
    """
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
        如果為 True 前端就會導向某個頁面
        預設為 False，如果想要在某些情況下導像，就需要透過子類別或是其他機制複寫這個屬性
        """
        return True


class TypeQuerySet(QuerySet):
    """ 根據一組 watchlist_items，把用得到的 type 都篩選出來，並把結果快取起來，下次查詢同一批 item 就不用重跑資料庫 """

    def filter_by_watchlist_items(self, **kwargs):
        items = kwargs.get('watchlist_items')
        if not items:
            raise NotImplementedError

        ids = items.values_list('product__type__id', flat=True)
        cache_key = f'types_filter_by_watchlist_items{hash(str(ids))}'
        types = cache.get(cache_key)

        if types is None:
            types = self.filter(id__in=ids)
            cache.set(cache_key, types, dump=True)
        else:
            types = pickle.loads(types)

        return types


class Type(Model):
    """ name: 批發, name: 產地, name: 零售 """
    name = CharField(max_length=50, unique=True, verbose_name=_('Name'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))

    objects = TypeQuerySet.as_manager()

    class Meta:
        verbose_name = _('Type')
        verbose_name_plural = _('Types')

    def __str__(self):
        return str(self.name)

    def sources(self):
        return Source.objects.filter(type=self)

    @property
    def to_direct(self):
        """
        set true to navigate at front end
        如果為 True 前端就會導向某個頁面
        預設為 False，如果想要在某些情況下導像，就需要透過子類別或是其他機制複寫這個屬性
        """
        return True


class Unit(Model):
    """
    price_unit: 元/公斤, 元/把, 元/隻
    volume_unit: 公斤, 頭, 隻, 把, None
    weight_unit: 公斤, None
    """
    price_unit = CharField(max_length=50, null=True, blank=True, verbose_name=_('Price Unit'))
    volume_unit = CharField(max_length=50, null=True, blank=True, verbose_name=_('Volume Unit'))
    weight_unit = CharField(max_length=50, null=True, blank=True, verbose_name=_('Weight Unit'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))

    class Meta:
        verbose_name = _('Unit')
        verbose_name_plural = _('Units')

    def __str__(self):
        return f'{self.price_unit}, {self.volume_unit}, {self.update_time}'


    # attr 為 attribute 動態取得已設定且不為空的單位欄位，並回傳 list ex. [('價格單位', '元/隻')]
    def attr_list(self):
        data = model_to_dict(self, fields=['price_unit', 'volume_unit', 'weight_unit'])
        return [
            (self._meta.get_field(attr).verbose_name.title(), value)
            for attr, value in data.items()
            if value
        ]


class Chart(Model):
    """
    name: 近2週每日量價, 歷年每日量價走勢, 歷年各月量價分布, 重要產銷事件簿
    code: CHT01, CHT02, CHT03, CHT04, CHT05
    template_name: ajax/chart-1-content.html, ajax/chart-2-content.html, ajax/chart-3-content.html,
    ajax/chart-4-content.html, ajax/chart-5-content.html
    """
    name = CharField(max_length=120, unique=True, verbose_name=_('Name'))
    code = CharField(max_length=50, unique=True, null=True, verbose_name=_('Code'))
    template_name = CharField(max_length=255, verbose_name=_('Template Name'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))

    class Meta:
        verbose_name = _('Chart')
        verbose_name_plural = _('Charts')

    def __str__(self):
        return str(self.name)


class Month(Model):
    """ name: 1月, 2月, 3月, 4月, 5月, 6月, 7月, 8月, 9月, 10月, 11月, 12月 """
    name = CharField(max_length=120, unique=True, verbose_name=_('Name'))

    class Meta:
        verbose_name = _('Month')
        verbose_name_plural = _('Months')

    def __str__(self):
        return str(self.name)


class Festival(Model):
    """
    roc_year: 112
    name: 中秋節
    enable: True
    """
    roc_year = CharField(max_length=3, default=timezone.now().year - 1911, verbose_name=_('ROC Year'))
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


class FestivalName(Model):
    """
    name: 中秋節
    enable: True
    lunar_month: 08
    lunarday: 15
    """
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


class FestivalItems(Model):
    """
    name: 雞蛋(產地價)
    enable: True
    order_sn: 403
    festivaname: configs.FestivalName.None
    product_id: configs.AbstractProduct.None
    source: configs.Source.None
    """
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


class Last5YearsItems(Model):
    """
    name: 金目鱸(池邊)
    enable: True
    product_id: configs.AbstractProduct.None
    source: configs.Source.None
    {'sort_value: None'}
    """
    LAST5_YEARS_ITEMS_CACHE_KEY = 'last5_years_items'

    name = CharField(max_length=60, verbose_name=_('Name'))
    enable = BooleanField(default=True, verbose_name=_('Enabled'))
    product_id = ManyToManyField('configs.AbstractProduct', verbose_name=_('Product_id'))
    source = ManyToManyField('configs.Source', verbose_name=_('Source'), blank=True)
    sort_value = IntegerField(blank=True, null=True, verbose_name=_('Sort Value'))
    update_time = DateTimeField(auto_now=True, null=True, blank=True, verbose_name=_('Updated'))
    create_time = DateTimeField(auto_now_add=True, null=True, blank=True, verbose_name=_('Created'))

    class Meta:
        verbose_name = _('Last5YearsItems')

    def __str__(self):
        return self.name

def instance_post_save(sender, instance, created, **kwargs):
    if kwargs.get('raw'):
        instance.save()
        return
    else:
        cache.delete_keys_by_model_instance(instance, Last5YearsItems, key=Last5YearsItems.LAST5_YEARS_ITEMS_CACHE_KEY)

post_save.connect(instance_post_save, sender=Last5YearsItems)