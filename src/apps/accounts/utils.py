import random
import string
import datetime
from typing import Callable, List

from django.conf import settings
from django.contrib.sites.models import Site
from django.contrib.auth.models import Group
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils.html import strip_tags
from django.core.cache import cache
from django.utils import timezone


KEY_MIN = getattr(settings, "KEY_MIN", 30)


def code_generator(
        size=KEY_MIN, chars=string.ascii_lowercase + string.digits
):
    return ''.join(random.choice(chars) for _ in range(size))


def upload_location(instance, filename):
    model = instance.__class__
    last_obj = model.objects.order_by("id").last()

    if last_obj:
        new_id = last_obj.id + 1
    else:
        new_id = 1

    return "profile/%s/%s" % (new_id, filename)


def send_email(url, user, input_content):
    context = {
        'user': user.first_name,
        'url': url,
        'login_url': Site.objects.get_current().domain + '/accounts/login/',
    }
    context.update(input_content)
    html_content = render_to_string('mail_content.html', context)
    content = strip_tags(html_content)
    mail = EmailMultiAlternatives(
        input_content["mail_title"],
        content,
        settings.EMAIL_HOST_USER,
        [user.email]
    )
    mail.attach_alternative(html_content, "text/html")
    mail.send(fail_silently=False)


def _chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def notify_group(group_name: str, subject: str, context: dict, bcc=True, template="new-product-codes.html") -> int:
    """
    把信寄給 Django 群組內所有啟用使用者：
    - HTML 內容由 template + context 產生
    - 純文字內容自動用 strip_tags 從 HTML 萃取
    回傳實際寄出的批次數。
    """

    group = Group.objects.filter(name=group_name).first()
    if not group:
        return 0
    
    qs = group.user_set.filter(is_active=True).exclude(email__isnull=True).exclude(email__exact="")

    emails = sorted({u.email.strip() for u in qs if u.email})
    if not emails:
        return 0
    

    html_body = render_to_string(template, context)
    text_body = strip_tags(html_body)
    sender = getattr(settings, "DEFAULT_FROM_EMAIL", settings.EMAIL_HOST_USER)
    print(settings.DEFAULT_FROM_EMAIL, settings.EMAIL_HOST_USER)
    print(emails)

    sent = 0
    for batch in _chunk(emails, 50):  # 避免 SMTP RCPT 上限
        mail = EmailMultiAlternatives(
            subject=subject,
            body=text_body,          # 純文字版本
            from_email=sender,
            to=[] if bcc else batch,
            bcc=batch if bcc else None,
        )
        mail.attach_alternative(html_body, "text/html")  # HTML 版本
        mail.send(fail_silently=True)
        sent += 1
        
    return sent


def mail_once_today(key_parts: list, send_callable: Callable[[], None]) -> bool:
    """
    key_parts: 用來組 cache key 的片段（api_name, config.code, type)
    send_callable: 寄信的函式（無參數），只有第一次會被呼叫
    回傳 True 表示今天剛寄過；False 表示今天早就寄過而略過
    """
    today = timezone.localtime(timezone.now()).date()
    key = "newprod:mail:" + ":".join(map(str, key_parts)) + f":{today.isoformat()}"

    # TTL 設到今晚 23:59:59 再多 60 秒緩衝
    # 如果TTL 時間過了，快取就會被刪除
    now = timezone.localtime(timezone.now())
    midnight = (now + datetime.timedelta(days= 1)).replace(hour=0, minute=0, second=0, microsecond=0)
    ttl = int((midnight - now).total_seconds()) + 60

    if cache.add(key, 1, timeout=ttl):  # 原子操作，只有第一個會成功
        send_callable()
        return True
    return False


def mail_new_product_once_today(
    api_name: str,
    config_code: str,
    config_name: str,
    type_id: str,
    type_name: str,
    new_product_codes: List[str],
    new_product_names: List[str],   # <== 新增
):
    """
    Wrap name/code pairs into rows
    that emailer can sending table of new products row by row
    """
    rows = list(zip(new_product_codes, new_product_names))

    mailed = mail_once_today(
        key_parts=[api_name, config_code, type_id],
        send_callable=lambda: notify_group(
            "Gmail測試群組",
            f"[{config_name}-{type_name}] 新增品項偵測",
            {
                "api_name": f"{config_name}-{type_name}: {api_name}",
                # "codes": new_product_codes,
                # "names": new_product_names,
                "rows": rows
            },
        ),
    )
    return mailed