from apps.dailytrans.builders.naifchickens import Api
from apps.dailytrans.builders.utils import (
    director,
    date_generator,
    DirectData,
    date_delta,
)
from .models import Naifchickens
import pandas as pd
import numpy as np
# from datetime import datetime,timedelta,date
import time
import os
import requests
import pytesseract
from PIL import Image
from django.conf import settings
import re
from bs4 import BeautifulSoup as bs
import json


import logging
db_logger = logging.getLogger('aprp')

MODELS = [Naifchickens]
CONFIG_CODE = 'COG16'
LOGGER_TYPE_CODE = 'LOT-naifchickens'

DELTA_DAYS = 1


@director
def direct(start_date=None, end_date=None, *args, **kwargs):

    data = DirectData(CONFIG_CODE, 1, LOGGER_TYPE_CODE)

    login_fail_flag = False

    for model in MODELS:
        api = Api(model=model, **data._asdict())

        #畜產會環南家禽批發市場交易行情表為一整個月份的數據,因此爬取網頁數據後組合成 json 格式以便後續流程
        data_list = []

        #pytesseract執行檔絕對路徑,系統需要額外安裝 tesseract-ocr
        pytesseract.pytesseract.tesseract_cmd = settings.PYTESSERACT_PATH

        headers = {'User-Agent': settings.USER_AGENT,}

        #中央畜產會會員登入頁面
        naif_login_url = settings.DAILYTRAN_BUILDER_API['naifchickens']
        ss = requests.Session()
        r1 = ss.get(naif_login_url, headers=headers)

        #儲存驗證碼圖片
        r2 = ss.get('https://www.naif.org.tw/libs/numToPic.aspx', headers=headers)
        with open('captcha.jpg', 'wb') as f:
            for chunk in r2:
                f.write(chunk)
        
        #OCR自動識別驗證碼
        captcha = Image.open('captcha.jpg')
        code = pytesseract.image_to_string(captcha).strip()
        if len(code) > 4:
            code = code[:4]

        #登入會員系統
        post_url = 'https://www.naif.org.tw/memberLogin.aspx'

        postdata = {
        'url': '/memberLogin.aspx',
        'myAccount': settings.NAIF_ACCOUNT,
        'myPassword': settings.NAIF_PASSWORD,
        'code': code,
        'btnSend': '登入會員',
        'frontTitleMenuID': 105,
        'frontMenuID': 148,
        }

        r3 = ss.post(post_url, headers=headers, data=postdata)

        if start_date:
            start_year = start_date.year
            start_month = start_date.month
        if end_date:
            end_year = end_date.year
            end_month = end_date.month

        if not start_date and not end_date:
            delta_start_date, delta_end_date = date_delta(DELTA_DAYS)
            start_year = delta_start_date.year
            start_month = delta_start_date.month
            end_year = delta_end_date.year
            end_month = delta_end_date.month 

        chickens_dict = {
                    '120':'環南-白肉雞',
                    '137':'環南-土雞',
                }   

        for k,v in chickens_dict.items():

            for y in range(start_year, end_year+1):
                if y == start_year and y == end_year:
                    s_month = start_month
                    e_month = end_month
                elif y == start_year:
                    s_month = start_month
                    e_month = 12
                elif y == end_year:
                    s_month = 1
                    e_month = end_month
                else:
                    s_month = 1
                    e_month = 12
                for m in range(s_month, e_month+1):
                    if len(str(m)) == 1:
                        m = '0' + str(m)

                    
                    #台北市環南家禽批發市場交易行情表頁面
                    if k == '120':  #環南_白肉雞
                        r4 = ss.get(f'https://www.naif.org.tw/infoWhiteChicken.aspx?sYear={y}&sMonth={m}&btnSearch=%E6%90%9C%E5%B0%8B&frontMenuID=120&frontTitleMenuID=37', headers=headers)
                    if k == '137':   #環南_土雞
                        r4 = ss.get(f'https://www.naif.org.tw/infoEndemicChicken.aspx?sYear={y}&sMonth={m}&btnSearch=%E6%90%9C%E5%B0%8B&frontMenuID=137&frontTitleMenuID=37', headers=headers)
                    soup = bs(r4.text, 'html.parser')
                    table = soup.find('table', {'cellspacing': '1'})
                    alert = soup.find('script').string

                    #登入失敗
                    if alert and '請先登入會員!!' in alert:
                        db_logger.warning(f'Login failed for naif, captcha : {code}',extra={
                                                                                                'logger_type': data.logger_type_code,
                                                                                            })
                        login_fail_flag = True
                        break

                    #登入成功
                    if not alert and table:
                        trs = table.find_all('tr')[1:]
                        for tr in trs:

                            #組合json格式用預設格式的
                            default_dict1 = {
                                                "date": "",
                                                "item": v,
                                                "price": 0,
                                                "weight":0,
                                                "volume":0,
                                                "priceUnit": "元/公斤"
                                            }
                            
                            d = tr.find('th').getText()
                            tds = tr.find_all('td')

                            avg_price = tds[4].getText().replace(",","").strip()
                            volume = tds[6].getText().replace(",","").strip()
                            weight = tds[7].getText().replace(",","").strip()
                            if avg_price:
                                try:
                                    avg_price = float(avg_price)
                                except ValueError:
                                    db_logger.warning(f'{y}/{m}/{d} {v} price Value Error : {tds[4].getText().replace(",","").strip()}',extra={
                                                                                                    'logger_type': data.logger_type_code,
                                                                                                })
                                    avg_price = None

                            if volume:
                                try:
                                    volume = float(volume)
                                except ValueError:
                                    db_logger.warning(f'{y}/{m}/{d} {v} volume Value Error : {tds[6].getText().replace(",","").strip()}',extra={
                                                                                                    'logger_type': data.logger_type_code,
                                                                                                })
                                    volume = None

                            if weight:
                                try:
                                    weight = float(weight)
                                except ValueError:
                                    db_logger.warning(f'{y}/{m}/{d} {v} weight Value Error : {tds[7].getText().replace(",","").strip()}',extra={
                                                                                                    'logger_type': data.logger_type_code,
                                                                                                })
                                    weight = None

                            #組合成API可用的格式
                            if avg_price and volume and weight:
                                default_dict1['date'] = f'{y}/{m}/{d}'
                                default_dict1['price'] = avg_price
                                default_dict1['volume'] = volume
                                default_dict1['weight'] = weight
                                data_list.append(default_dict1)

                    time.sleep(5)

                #登入失敗時離開
                else:
                    continue
                break

        if not login_fail_flag:
            api.load(json.dumps(data_list))

            #刪除驗證碼圖片檔案    
            os.remove('captcha.jpg')

    return data