from django import forms
from django.contrib import admin
from django.forms import ModelForm
from django.forms.utils import ErrorList

from .models import (
    AbstractProduct, Chart, Config, Festival, FestivalName,
    FestivalItems, Last5YearsItems, Source, Type, Unit,
)


class AbstractProductModelForm(ModelForm):
    class Meta:
        model = AbstractProduct
        exclude = ['update_time']

    def __init__(
            self, data=None, files=None, auto_id='id_%s', prefix=None,
            initial=None, error_class=ErrorList, label_suffix=None,
            empty_permitted=False, instance=None
    ):
        super().__init__(
            data, files, auto_id, prefix, initial, error_class,
            label_suffix, empty_permitted, instance
        )

        # forcing add the `id` field can be edited
        self.fields['id'] = forms.IntegerField(widget=forms.TextInput())
        self.fields['parent'].choices = self.parent_field_choices

    @property
    def parent_field_choices(self):
        qs_products = AbstractProduct.objects.all()

        return [(obj.id, f"{obj.id} ({obj.name})") for obj in qs_products]


    def save(self, commit=True):
        instance = super().save(commit=False)
        instance.id = self.cleaned_data['id']

        if commit:
            instance.save()
        return instance


class AbstractProductAdmin(admin.ModelAdmin):
    form = AbstractProductModelForm
    list_display = [
        'id', 'name', 'code', 'config', 'type', 'parent', 'track_item', 'unit'
    ]
    list_editable = ['name', 'code', 'track_item']
    list_filter = ('config', 'type', 'track_item')
    fields = [
        'id', 'name', 'code', 'config', 'type', 'parent', 'track_item', 'unit'
    ]

    search_fields = (
        'id',
        'code',
        'name',
    )


class ConfigModelForm(ModelForm):
    class Meta:
        model = Config
        exclude = ['update_time']


class ConfigAdmin(admin.ModelAdmin):
    model = ConfigModelForm
    list_display = ['id', 'name', 'code', 'update_time']
    list_editable = ['name', 'code']


class SourceModelForm(ModelForm):

    class Meta:
        model = Source
        exclude = ['update_time']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not (self.instance and self.instance.pk):
            self.fields["id"] = forms.IntegerField(
                required=True,
                widget=forms.TextInput()
            )



    def save(self, commit=True):
        instance = super().save(commit=False)
        # 只在新增時才從表單指定 id
        if not instance.pk:
            instance.id = self.cleaned_data["id"]

        if commit:
            instance.save()
            self.save_m2m()
        return instance



class SourceAdmin(admin.ModelAdmin):
    form = SourceModelForm
    list_display = ['id', 'name', 'alias', 'code', 'update_time']
    list_editable = ['name', 'code']
    # fields = [
    #     'id', 'name', 'code', 'config', 'type', 'parent', 'track_item', 'unit'
    # ]
    def get_fields(self, request, obj=None):
        fields = list(super().get_fields(request, obj))
        if obj is None:
            # 新增：確保 id 在最前面
            if "id" not in fields:
                fields.insert(0, "id")
            else:
                fields.remove("id")
                fields.insert(0, "id")
        else:
            # 編輯：完全不顯示/不包含 id
            fields = [f for f in fields if f != "id"]
        return fields


class TypeModelForm(ModelForm):
    class Meta:
        model = Type
        exclude = ['update_time']


class TypeAdmin(admin.ModelAdmin):
    form = TypeModelForm
    list_display = ['id', 'name', 'update_time']
    list_editable = ['name']


class UnitModelForm(ModelForm):
    class Meta:
        model = Unit
        exclude = ['update_time']


class UnitAdmin(admin.ModelAdmin):
    form = UnitModelForm
    list_display = [
        'id', 'price_unit', 'volume_unit', 'weight_unit', 'update_time'
    ]
    list_editable = ['price_unit', 'volume_unit', 'weight_unit']


class ChartModelForm(ModelForm):
    class Meta:
        model = Unit
        exclude = ['update_time']


class ChartAdmin(admin.ModelAdmin):
    form = UnitModelForm
    list_display = ['id', 'name', 'update_time']
    list_editable = ['name']


class FestivalModelForm(ModelForm):

    class Meta:
        model = Festival
        exclude = ['update_time']


class FestivalAdmin(admin.ModelAdmin):
    form = FestivalModelForm
    list_display = [
        'id', 'roc_year', 'name', 'enable', 'update_time', 'create_time'
    ]
    list_editable = ['roc_year', 'name', 'enable']


class FestivalNameModelForm(ModelForm):

    class Meta:
        model = FestivalName
        exclude = ['update_time']


class FestivalNameAdmin(admin.ModelAdmin):
    form = FestivalNameModelForm
    list_display = [
        'id', 'name', 'lunarmonth', 'lunarday', 'enable',
        'update_time', 'create_time'
    ]
    list_editable = ['name', 'lunarmonth', 'lunarday', 'enable']

class FestivalItemsModelForm(ModelForm):

    class Meta:
        model = FestivalItems
        exclude = ['update_time']

class FestivalItemsAdmin(admin.ModelAdmin):
    form = FestivalItemsModelForm
    list_display = [
        'id', 'order_sn', 'name', 'enable', 'update_time', 'create_time'
    ]
    list_editable = ['name', 'enable']

class Last5YearsItemsModelForm(ModelForm):
    class Meta:
        model = Last5YearsItems
        exclude = ['update_time']

    def __init__(
            self, data=None, files=None, auto_id='id_%s', prefix=None,
            initial=None,error_class=ErrorList, label_suffix=None,
            empty_permitted=False, instance=None
    ):
        super().__init__(
            data, files, auto_id, prefix, initial, error_class,
            label_suffix, empty_permitted, instance
        )

        self.fields['product_id'].choices = self.product_field_choices
        self.fields['source'].choices = self.source_field_choices

    @property
    def product_field_choices(self):
        qs_products = AbstractProduct.objects.all()

        return [(obj.id, f"{obj.id} ({obj.name})") for obj in qs_products]

    @property
    def source_field_choices(self):
        qs_sources = Source.objects.all()

        return [(obj.id, f"{obj.id} ({obj.name})") for obj in qs_sources]


class Last5YearsItemsAdmin(admin.ModelAdmin):
    form = Last5YearsItemsModelForm
    list_display = [
        'id', 'name', 'enable', 'sort_value', 'update_time', 'create_time'
    ]
    list_editable = ['name', 'enable', 'sort_value']
    fields = ['product_id', 'source', 'name', 'enable', 'sort_value']

    search_fields = (
        'id',
        'name',
    )
    
admin.site.register(AbstractProduct, AbstractProductAdmin)
admin.site.register(Config, ConfigAdmin)
admin.site.register(Source, SourceAdmin)
admin.site.register(Type, TypeAdmin)
admin.site.register(Unit, UnitAdmin)
admin.site.register(Chart, ChartAdmin)
admin.site.register(Festival, FestivalAdmin)
admin.site.register(FestivalName, FestivalNameAdmin)
admin.site.register(FestivalItems, FestivalItemsAdmin)
admin.site.register(Last5YearsItems, Last5YearsItemsAdmin)