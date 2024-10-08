"""
Abstract classes here as AbstractProduct which inherit from third
party package model_utils.InheritanceManage, will not work with
abstract attribute specified true:

    class Meta:
        abstract = True

For this reason, its subclass will cause create issue when load
fixtures data by running this command:

    manage.py loaddata <file-name>

Which will not raise errors also not writing into database, see the same issue
on stackoverflow:

    https://stackoverflow.com/questions/41092363/why-does-django-loaddata-fail-silently-for-custom-user-model

The solution here is provide post_save signal and execute save(), because loaddata
command will not call pre_save() and save()
"""
from django.db.models.signals import post_save
from django.utils.translation import ugettext_lazy as _

from apps.configs.models import AbstractProduct
from dashboard.caches import redis_instance as cache


class Seafood(AbstractProduct):
    class Meta:
        verbose_name = _('Seafood')
        verbose_name_plural = _('Seafoods')


def instance_post_save(sender, instance, created, **kwargs):
    if kwargs.get('raw'):
        instance.save()
        return
    else:
        cache.delete_keys_by_model_instance(instance, AbstractProduct)


post_save.connect(instance_post_save, sender=Seafood)
