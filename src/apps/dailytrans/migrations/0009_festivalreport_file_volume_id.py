# -*- coding: utf-8 -*-
# Generated by Django 1.9.13 on 2021-01-27 07:17
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('dailytrans', '0008_festivalreport'),
    ]

    operations = [
        migrations.AddField(
            model_name='festivalreport',
            name='file_volume_id',
            field=models.CharField(blank=True, max_length=120, null=True, verbose_name='File Volume ID'),
        ),
    ]