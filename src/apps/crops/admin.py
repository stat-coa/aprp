from django import forms
from django.contrib import admin
from django.forms import ModelForm
from django.forms import TextInput
from django.db import models
from django.forms.utils import ErrorList

from .models import Crop


class CropModelForm(ModelForm):
    class Meta:
        model = Crop
        exclude = ['update_time']

    def __init__(self, data=None, files=None, auto_id='id_%s', prefix=None, initial=None, error_class=ErrorList,
                 label_suffix=None, empty_permitted=False, instance=None):
        super().__init__(data, files, auto_id, prefix, initial, error_class, label_suffix, empty_permitted, instance)

        self.fields['id'] = forms.IntegerField(widget=forms.TextInput())
        self.fields['parent'].choices = self.parent_field_choices

    @property
    def parent_field_choices(self):
        qs_products = Crop.objects.all()

        return [(obj.id, f"{obj.name} (id: {obj.id})") for obj in qs_products]

    def save(self, commit=True):
        instance = super().save(commit=False)
        instance.id = self.cleaned_data['id']

        if commit:
            instance.save()
        return instance


class CropAdmin(admin.ModelAdmin):
    form = CropModelForm
    formfield_overrides = {
        models.CharField: {'widget': TextInput(attrs={'size': '20'})},
    }
    list_display = ['id', 'name', 'code', 'type', 'parent', 'track_item', 'update_time']
    list_editable = ['name', 'code', 'type', 'parent', 'track_item']
    fields = ['id', 'name', 'code', 'type', 'parent', 'track_item']

    search_fields = (
        'id',
        'code',
        'name',
    )


admin.site.register(Crop, CropAdmin)
