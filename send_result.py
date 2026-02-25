# -*- coding: utf-8 -*-
import requests
import json

APP_ID = "cli_a916d15a7f38dbd6"
APP_SECRET = "QlWJNDr6A545YqJo3hwNfg30Titp6uPG"
OPEN_ID = "ou_c819a2c735bb23e6718ce47308f1f3e6"

def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, json=data)
    result = resp.json()
    if result.get("code") == 0:
        return result.get("tenant_access_token")
    raise Exception("获取token失败: {}".format(result))

def send_message(content, open_id=OPEN_ID):
    token = get_tenant_access_token()
    url = "https://open.feishu.cn/open-apis/im/v1/messages"
    params = {"receive_id_type": "open_id"}
    headers = {"Authorization": "Bearer {}".format(token), "Content-Type": "application/json; charset=utf-8"}

    data = {
        "receive_id": open_id,
        "msg_type": "text",
        "content": json.dumps({"text": content})
    }

    resp = requests.post(url, params=params, headers=headers, json=data)
    result = resp.json()
    if result.get("code") == 0:
        print("发送成功!")
    else:
        print("发送失败: {}".format(result))
    return result

if __name__ == "__main__":
    msg = """🐂 A股缠论第三买点筛选结果

分析日期: 2026-02-26

最接近第三买点的10只股票:

1. 603111 康尼机电   ZG:7.18  回抽最低:7.17  距ZG:-0.14%
2. 603323 苏农银行   ZG:5.05  回抽最低:5.04  距ZG:-0.20%
3. 600452 涪陵电力   ZG:12.05 回抽最低:11.98 距ZG:-0.58%
4. 600764 中国海防   ZG:29.10 回抽最低:28.82 距ZG:-0.96%
5. 601006 大秦铁路   ZG:5.09  回抽最低:5.03  距ZG:-1.18%
6. 603967 中创物流   ZG:13.03 回抽最低:12.80 距ZG:-1.77%
7. 603359 东珠生态   ZG:7.03  回抽最低:6.86  距ZG:-2.42%
8. 600653 申华控股   ZG:1.96  回抽最低:1.91  距ZG:-2.55%
9. 000019 深粮控股   ZG:7.16  回抽最低:6.92  距ZG:-3.35%
10. 600719 大连热电   ZG:6.65  回抽最低:6.40  距ZG:-3.76%

重点推荐:
- 603111 康尼机电: 回抽最低仅比ZG低0.14%，几乎触及第三买点
- 603323 苏农银行: 回抽最低仅比ZG低0.20%，非常接近第三买点

说明: 距ZG%表示回抽最低价与中枢上沿的距离，越接近0%越接近第三买点"""

    send_message(msg)
