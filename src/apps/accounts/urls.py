from django.conf.urls import url
from . import views


urlpatterns = [
    url(r'^password_change/$', views.change_password, name='password_change'),
    url(r'^login/', views.login_view, name='login'),
    url(r'^logout/', views.logout_view, name='logout'),
    url(r'^logout/', views.logout_view, name='logout'),
    url(r'^password_change/done/$',views.change_password_done, name='password_change_done')
]
