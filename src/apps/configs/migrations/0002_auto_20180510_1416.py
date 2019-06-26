# -*- coding: utf-8 -*-
# Generated by Django 1.9 on 2018-05-10 06:16
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('configs', '0001_initial'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='config',
            name='unit',
        ),
        migrations.RemoveField(
            model_name='unit',
            name='name',
        ),
        migrations.AddField(
            model_name='unit',
            name='price_unit',
            field=models.CharField(blank=True, max_length=50, null=True, verbose_name='Price Unit'),
        ),
        migrations.AddField(
            model_name='unit',
            name='volume_unit',
            field=models.CharField(blank=True, max_length=50, null=True, verbose_name='Volume Unit'),
        ),
        migrations.AddField(
            model_name='unit',
            name='weight_unit',
            field=models.CharField(blank=True, max_length=50, null=True, verbose_name='Weight Unit'),
        ),
    ]