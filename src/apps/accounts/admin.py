from django.conf.urls import url
from django.core.urlresolvers import reverse
from django.contrib import admin, messages
from django.contrib.admin import SimpleListFilter
from django.contrib.auth.admin import UserAdmin as DefaultUserAdmin
from django.contrib.auth.models import Group, User
from django.shortcuts import redirect
from django.utils.translation import ugettext_lazy as _
from . import models


admin.site.unregister(User)

# 後台管理介面重置密碼
@admin.register(User)
class UserAdmin(DefaultUserAdmin):

    change_form_template = 'admin/change_password.html'

    def get_urls(self):
        urls = super(UserAdmin, self).get_urls()
        custom = [
            url(
                r'^(?P<object_id>[^/]+)/reset-password/$',
                self.admin_site.admin_view(self.reset_password_view),
                name='auth_user_reset_password',
            ),
        ]
        return custom + urls

    def reset_password_view(self, request, object_id, *args, **kwargs):
        user = User.objects.get(pk=object_id)
        default_password = f"{user.username}{user.username}"
        user.set_password(default_password)
        user.is_active = False
        user.save()

        self.message_user(
            request,
        f"使用者 {user.username} 密碼已重置為 {default_password}",
            level=messages.SUCCESS
        )

        return redirect(
            reverse('admin:auth_user_change', args=[object_id])
        )


class GroupListFilter(SimpleListFilter):
    title = _('Group')

    # Parameter for the filter that will be used in the URL query.
    parameter_name = 'category'

    def lookups(self, request, model_admin):
        """
        Returns a list of tuples. The first element in each
        tuple is the coded value for the option that will
        appear in the URL query. The second element is the
        human-readable name for the option that will appear
        in the right sidebar.
        """
        return [(group.id, group.name) for group in Group.objects.all()]

    def queryset(self, request, queryset):
        """
        Returns the filtered queryset based on the value
        provided in the query string and retrievable via
        `self.value()`.
        """
        # Compare the requested value (either '80s' or 'other')
        # to decide how to filter the queryset.
        if self.value():
            return queryset.filter(user__groups__id=self.value())
        else:
            return queryset


class UserInformationAdmin(admin.ModelAdmin):
    model = models.UserInformation
    list_filter = (
        GroupListFilter,
        'amislist_viewer',
        'festivalreport_viewer',
        'festivalreport_refresh'
    )
    search_fields = (
        'id',
        'user__first_name',
        'user__last_name',
        'user__username'
    )


admin.site.register(models.ActivationProfile)
admin.site.register(models.GroupInformation)
admin.site.register(models.UserInformation, UserInformationAdmin)
admin.site.register(models.ResetPasswordProfile)
admin.site.register(models.ResetEmailProfile)
admin.site.register(models.AbstractProfile)
