import configuration.globalVars as globalVars
import requests
import json


def auth(tenantName):
    url = globalVars.chameleonAuthURL

    body = {
            "auth": {
                "tenantName": tenantName,
                "passwordCredentials": {
                    "username": globalVars.chameleonCloudUsername,
                    "password": globalVars.chameleonCloudPassword
                    }
                }
            }

    my_headers = {"Content-Type": 'application/json'}

    json_body = json.dumps(body)

    r = requests.post(url, json_body, headers=my_headers)

    globalVars.tenant_id = r.json()['access']['token']['tenant']['id']
    token_id = r.json()['access']['token']['id']
    return token_id
