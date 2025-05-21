import pickle

from typing import List, Optional
from apps.configs.models import Config, AbstractProduct
from dashboard.caches import redis_instance as cache
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
    """
    name: 114下半年
    user: admin
    is_default: True
    watch_all: False
    start_date: 2025-07-01
    end_date: 2025-12-31
    creat_time: None
    update_time: 2025-04-14 03:20:26.943711+00:00
    """
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
        # is_default 確保同一時間只有一筆清單為預設
        # 如果這個實例是預設監控清單
        if self.is_default:
            try:
                # 嘗試找出資料已經存在的預設清單
                watchlist = Watchlist.objects.get(is_default=True)

                # 如果找到了，而且正在 save 的這個不是同一筆
                if self != watchlist:
                    # 把舊的那筆 is_default 設為 False，保持只有一個預設清單
                    watchlist.is_default = False
                    watchlist.save()
            except Watchlist.DoesNotExist:
                # 如果沒有預設清單就甚麼都不做
                pass

        # remove watch_all=True list before add one
        # 如果這個實例被為 watchall=True
        if self.watch_all:
            try:
                # 嘗試找出已經存在的 watch_all 清單
                watchlist = Watchlist.objects.get(watch_all=True)
                # 如果找到了，而且跟目前的不是同一筆
                if self != watchlist:
                    # 把舊的那筆 watch_all 刪除，保持只有一個 watch_all 清單
                    watchlist.delete()
            # 如果沒有任何 watch_all，就甚麼都不做
            except Watchlist.DoesNotExist:

                pass
        # 最後呼叫父類別的 save()，把這個實例存進資料庫
        super(Watchlist, self).save(*args, **kwargs)

    def __str__(self):
        return str(self.name)

    def children(self):
        """
        得到該監控清單底下的所有監控品項 (WatchListItems)
        取出並快取 WatchList 底下所有對應的 WatchlistItem，並在快取有東西時直接從快取讀取
        """

        cache_key = f"watchlist{self.id}_children"
        items = cache.get(cache_key)

        # 如果快取沒有東西，會拿所有 parent FK 指向 Watchlist 的 WatchlistItem
        # 把查詢後的 QuerySet (或轉成list) 存進快取，並用相同的 cache key，並開始 dump = True (會 pickle 後存入)
        # dump = True 會告訴快取後端用 pickle 把 items 轉成 bytes，再存進記憶體
        if items is None:
            items = WatchlistItem.objects.filter(parent=self)
            cache.set(cache_key, items, dump=True)

        # 下次呼叫 cache.get() 拿到的就是那串 serialized bytes
        # pickle.loads() 能還原原本的 QuerySet / list，不用再跑一次資料庫查詢
        else:
            items = pickle.loads(items)

        return items

    def related_configs(self):
        """ 得到特定監控清單的所有品項分類 (Config) """

        configs = cache.get(f'watchlist{self.id}_related_configs')

        # 假如 configs 為 None
        # self.children() 會回傳一個 QuerySet，裡面包含所有 WatchlistItem，parent FK 指向這個 Watchlist
        # value_list() 為 Django ORM 的方法，用來取出指定欄位的值，不會回傳整個物件
        # product__config__id 為跨表關聯，WatchlistItem 的 product 欄位(一個 Product 實例)
        # -> config 欄位 (Product 關聯的 Config 實例) -> id 欄位 ( Config 的 id )
        # distinct() 去除重複值，確保 id 只出現一次
        if configs is None:
            ids = self.children().values_list('product__config__id', flat=True).distinct()

            # 用 id 去查詢對應的 Config 並進行 id 排序
            configs = Config.objects.filter(id__in=ids).order_by('id')

            # dump = True 會告訴快取後端用 pickle 把 items 轉成 bytes，再存進記憶體
            cache.set(f"watchlist{self.id}_related_configs", configs, dump=True)


        # pickle.loads() 能還原原本的 QuerySet / list，不用再跑一次資料庫查詢
        else:
            configs = pickle.loads(configs)

        return configs

    @property
    def related_product_ids(self):
        """ 得到所有監控品項(WatchlistItems)的所有階層(children & parents)品項(AbstractProducts) ID """

        ids = []
        # self.children() 會回傳一個 WatchlistItem 的實例，每個都包含一個 product
        for child in self.children():
            item: WatchlistItem = child # 註記這個變數是 WatchlistItem 型別，方便後續使用 WatchlistItem 的屬性
            ids = ids + list(item.product.related_product_ids)
            # item.product.related_product_ids 本身為該品項
            # 及其上下層的 (parents & children) 品項的 ID 清單
            # 會轉成 list 並加在同一個 ids 清單裡面

        return ids


class WatchlistItemQuerySet(QuerySet):
    def filter_by_product(self, **kwargs):
        """ for case like WatchlistItem.objects.filter(parent=self).filter_by_product(product__id=1) """
        product: Optional[AbstractProduct] = (
                kwargs.get('product')
                or AbstractProduct.objects.filter(id=kwargs.get('product__id')).first()
        )

        if product:
            cache_key = f'product{product.id}_filter_by_product'
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
    """
    product: 梨-寶島甘露梨
    source: configs.Source.None
    parent: 114下半年
    update_time: None
    """
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


class MonitorProfile(Model):
    """
    product: 龍虎斑
    watchlist: 114下半年
    type: 產地
    comparator: __lt__
    color: danger
    info: 1.本案表列「啟動價格係指監控價格」，即每日通報價臨近監控價格，則啟動先期調節措施
    2.監控價格之計算：
    (1)推估生產者最低之直接生產成本，項目包含種苗、飼料餌料、水電油及人力等4項
    (2)監控價格=最低直接生產成本*95％*110%
    action: 1.因漁產品具高替代性，價格上漲時消費者多選擇其他價廉魚產品替代，故產銷穩定多以連續14日低於監控價格啟動
    2.池邊交易價低於監控價105%持續2週以上：
    適時辦理電商活動、百大企業及公(私)部門團購、輔導漁會等單位促進校園午餐、國軍副食、
    矯正機關採購、通路合作展售促銷、輔導生產者加入農民市集等加強多元通路行銷
    3.緊急調節：經先期加強行銷措施後仍持續低於監控價2週以上，或政策決定時，鼓勵漁民業團體、加工廠收購、凍儲或獎勵外銷
    period: 全年
    is_active: False
    months: configs.Month.None
    always_display: False
    row: 127
    update_time: None
    """
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
        return f'product: {self.product.name}, watchlist: {self.watchlist.name}, price: {self.price}'

    def sibling(self):
        return (MonitorProfile.objects.exclude(id=self.id)
                .filter(type=self.type, product=self.product, watchlist=self.watchlist))

    def watchlist_items(self):
        return WatchlistItem.objects.filter(product__parent=self.product)

    def product_list(self) -> List[AbstractProduct]:
        """ 此 method 只會由 `apps.dailytrans.dailyreport.py` 使用，用於產製日報表的品項 """
        
        items = (WatchlistItem.objects.filter_by_product(product=self.product)
                .filter(parent=self.watchlist))
        # items = self.watchlist_items().filter(parent=self.watchlist)

        return [item.product for item in items] if items else [self.product]

    def sources(self):
        """ 此 method 只會由 `apps.dailytrans.dailyreport.py` 使用，用於產製日報表時，取得品項的來源 """

        sources = []
        items = (WatchlistItem.objects.filter_by_product(product=self.product)
                .filter(parent=self.watchlist))

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
