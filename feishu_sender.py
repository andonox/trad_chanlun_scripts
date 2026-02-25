# -*- coding: utf-8 -*-
"""
飞书消息发送脚本
"""
import requests
import json

APP_ID = "cli_a916d15a7f38dbd6"
APP_SECRET = "QlWJNDr6A545YqJo3hwNfg30Titp6uPG"
OPEN_ID = "ou_c819a2c735bb23e6718ce47308f1f3e6"

def get_tenant_access_token():
    """获取tenant_access_token"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, json=data)
    result = resp.json()
    if result.get("code") == 0:
        return result.get("tenant_access_token")
    raise Exception("获取token失败: {}".format(result))

def send_message(content, open_id=OPEN_ID):
    """发送消息给用户"""
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
    test_msg = "Test message from Feishu API"

    send_message(test_msg)
