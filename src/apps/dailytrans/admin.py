from django.contrib import admin
from django.forms import ModelForm, ValidationError
from django.forms.utils import ErrorList
from django.utils.translation import ugettext_lazy as _
from rangefilter.filter import DateRangeFilter

from apps.configs.models import AbstractProduct, Source
from .models import DailyTran, DailyReport, FestivalReport


class DailyTranModelForm(ModelForm):
    class Meta:
        model = DailyTran
        exclude = []

    def __init__(self, data=None, files=None, auto_id='id_%s', prefix=None, initial=None, error_class=ErrorList,
                 label_suffix=None, empty_permitted=False, instance=None):
        super().__init__(data, files, auto_id, prefix, initial, error_class, label_suffix, empty_permitted, instance)
        self.fields['product'].choices = self.product_field_choices
        self.fields['source'].choices = self.source_field_choices

    @property
    def product_field_choices(self):
        qs_products = AbstractProduct.objects.all()

        return [(obj.id, f"{obj.id} ({obj.name})") for obj in qs_products]

    @property
    def source_field_choices(self):
        qs_sources = Source.objects.all()

        choices = [("", "---------")]
        choices += [(obj.id, f"{obj.id} ({obj.name})") for obj in qs_sources]

        return choices

    def clean(self):
        product = self.cleaned_data.get('product')
        source = self.cleaned_data.get('source')
        valid = True
        if not product:
            raise ValidationError('There is no such content object')

        try:
            if source.config != product.config:
                valid = False
        except AttributeError:
            pass

        try:
            if source.config.id != product.category.config.id:
                valid = False
        except AttributeError:
            pass

        if not valid:
            raise ValidationError("Source are incorrect")

        return self.cleaned_data


class DailyTranAdmin(admin.ModelAdmin):
    form = DailyTranModelForm
    list_display = ('date',
                    'update_time',
                    'create_time',
                    'not_updated',
                    'productname',
                    'source',
                    'up_price',
                    'mid_price',
                    'low_price',
                    'avg_price',
                    'avg_weight',
                    'volume')
    list_editable = ('up_price', 'mid_price', 'low_price', 'avg_price', 'avg_weight', 'volume')
    list_filter = (('date', DateRangeFilter), 'product__type__name', 'product__config__name', 'source')
    search_fields = ['product__name', 'product__parent__name', 'product__code']

    def productname(self, obj):
        # For crop wholesale type 1
        if obj.product.config.id == 5 and obj.product.type.id == 1:
            if obj.product.name == obj.product.code:
                return f"{obj.product.parent.name}({obj.product.name})"
            else:
                return f"{obj.product.name}({obj.product.code})"
        # For hog
        elif obj.product.config.id == 8:
            if obj.product.id == 70001:
                return obj.product.code
            elif obj.product.id == 70004 or obj.product.id == 70005:
                return f"{obj.product.parent.name}({obj.product.name})"
            else:
                return obj.product.name
        # For chicken and gooses
        elif obj.product.config.id == 10 or obj.product.config.id == 12:
            return f"{obj.product.code}"
        # For origin seafood
        elif obj.product.config.id == 13 and obj.product.type.id == 2:
            return f"{obj.product.parent.name}({obj.product.name})"
        else:
            return obj.product.name
    productname.short_description = _('Product')


class DailyReportAdmin(admin.ModelAdmin):
    list_display = (
        'date',
        'update_time',
        'create_time',
    )


class FestivalReportAdmin(admin.ModelAdmin):
    list_display = (
        'festival_id',
        'file_id',
        'file_volume_id',
        'update_time',
        'create_time',
    )
    
admin.site.register(DailyTran, DailyTranAdmin)
admin.site.register(DailyReport, DailyReportAdmin)
admin.site.register(FestivalReport, FestivalReportAdmin)
