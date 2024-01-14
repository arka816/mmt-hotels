import requests

url = 'https://mapi.makemytrip.com/autosuggest/v5/search'
city = 'darjeeling'

pathParams = {
    'q': city,
    'sf': 'true',
    'sgr': 't',
    'language': 'eng',
    'region': 'in',
    'currency': 'INR',
    'idContext': 'B2C',
    'countryCode': 'IN'
}

pathParams = {
    'cc': 1,
    'exp': 3,
    'exps': 'expscore1',
    'expui': 'v2',
    'hcn': 1,
    'q': city,
    'sgr': 't',
    'region': 'in',
    'language': 'eng',
    'currency': 'inr'
}



headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json;charset=utf-8',
    'currency': 'INR',
    'language': 'eng',
    'origin': 'https://www.makemytrip.com',
    'referer': 'https://www.makemytrip.com/',
    'region': 'in',
    'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'server': 'b2c',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'
}

headers = {
    # ":authority" : "mapi.makemytrip.com",
    # ":method" : "GET",
    # ":path" : '/autosuggest/v5/search?q=darjeeling&sf=true&sgr=t&language=eng&region=in&currency=INR&idContext=B2C&countryCode=IN',
    # ":scheme" : "https",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding" : "gzip, deflate, br",
    "Accept-Language" : "en-US,en;q=0.9",
    "Cache-Control" : "no-cache",
    "Cookie" : "dvid=db63e622-99ee-4d08-a5db-a7fd3bcfc39e; s_ecid=MCMID%7C22764246529846830562146980723033618240; _fbp=fb.1.1688759463430.120505978; mmt-auth=MAT159e7a28d45f4c1b0d888628bc8f641335065444bd74a8eb4aba055fd11e7a313a9af9a0f398a65c53df685c93cd6905aP; myBusinessSubscription=b2c; mcid=72ae5ee8-a3ed-403c-acf0-5bbb47a75d16; _uetvid=3b4c27f0490911ee94093b38f4757c25; ccde=IN; lang=eng; _gcl_au=1.1.758069399.1700335565; MMT_LOYALTY=INACTIVE; session-id=db63e622-99ee-4d08-a5db-a7fd3bcfc39e_dweb_1700840074323_5d4f; _gcl_aw=GCL.1701095504.CjwKCAiAmZGrBhAnEiwAo9qHiQlxAmRiNfjjKn2WlPORCX_hgN9vGDiKosLH8hxjdte5zyHbFbIuIhoCc5wQAvD_BwE; AMCV_1E0D22CE527845790A490D4D%40AdobeOrg=-1712354808%7CMCIDTS%7C19695%7CMCMID%7C22764246529846830562146980723033618240%7CMCAAMLH-1702211296%7C3%7CMCAAMB-1702211296%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1701613696s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C4.3.0; _clck=1o0rvau%7C2%7Cfh8%7C0%7C1337; visitNumber=%7B%22number%22%3A13%2C%22time%22%3A1701609746732%7D; MMYTUUID=24617072-3124-5341-5771-506570472469.1701609749377649; s_pers=%20s_vnum%3D1704047400973%2526vn%253D2%7C1704047400973%3B%20s_depth%3D3%7C1701611548272%3B%20s_lv%3D1701609749239%7C1796217749239%3B%20s_lv_s%3DLess%2520than%25201%2520day%7C1701611549239%3B%20gpv_pn%3Dfunnel%253Adomestic%2520hotels%253Ahoteldetails%7C1701611549247%3B%20s_invisit%3Dtrue%7C1701611549250%3B%20s_nr3650%3D1701609749252-Repeat%7C2016969749252%3B%20s_nr30%3D1701609749256-Repeat%7C1704201749256%3B%20s_nr120%3D1701609749259-Repeat%7C1711977749259%3B%20s_nr7%3D1701609749261-Repeat%7C1702214549261%3B; _abck=BE8ACD636F8B8F3ED0781B72ACD831EA~-1~YAAQslstF3At/POLAQAATjWRUAvICoXF2rlw8ooFbI1nYoJKpVM3g8IP069HUtjUc17/zvdeFM9wvKWrqv7PkvllmpfG0b//PqCJ6/i/pmQVFyQM8OvghNoXX9PEidUYF1zz7yQTTgqcBjc01swNEKxIpc7J9jPKr3vE232j5F3fv8w76QUpaaKKGgLIoXwysTAvksgWL7a5LeGIcO5BbWB4+P/ydX26U/CnakS/myslbDvnsryeNc3ZgqaYp1VnwDjrDFASSuAlXFuKskFELZ+CLr756NvCdz19WN6pd4pjIHzCfnuHJQ6N1ZvOLAUcD2Yj3aQSaxAGuoeJrtLsE0eyHwOGm+oA0A4cidEy9t9t6lnBbhyeYBo3DhtY7eh75GN7bxAncF6ZgFR+tLA=~-1~-1~-1; ak_bmsc=2AD47BBE1D2FE89238DEC00DF2A76243~000000000000000000000000000000~YAAQslstF3Et/POLAQAATjWRUBZe7NFxV8/RHVZNquYlM0FXtix/11N4rjSvI4ak/pjiZY9iEDGGkUivYNNxKHqD6ppaDV5ySHgSKUuB9cYEqWFIKC869p/Rq/hZjMLmgAjJ3Iczh6O7CdKsji592nMVSvF+uGKcfAMo7ABIysWCsSTKgtU2QBE2aYJ64r4wB/nzkS2AO/LV0KcXaLLlTfI3tDLsb9E8SDcXp624Vsc8MQGC/+2F+LqiTQT1ut3wgCKiJcjUalqz1fM2TupcH41P7hKUBOb6d0d0T3OBtoEDGKb8DgBKMCUrFzbE+C13yfCRFPlkAr6HEXnPfXuVanHBUCh/sBDxnOO8uEEZ57wJfzDdyW6o37ytUqkdKs7zEebbGLO8MW/pEE0hmg==; bm_sz=6E2A8933341DC495EDD1110E5D974B15~YAAQslstF3It/POLAQAATjWRUBaXHXJDEVn6Dts0kv1bszz+GlGj1+8SC8rXGU91Yjd7/WLluRqoSIZPyUkbEWM8nqso2qciJ1uqpv2+CynoTnGQgiWpb2QPifLe3AMGi9r/aMqdJqnV/o0XhqZlVeogkwTg2apaR6bwIfaPJOgT/sa9Dc4fo/TTajLxuuYjYLja37U4gp82vx/KZlkRPTI8aoIizgZU4krZ75wLUmVVIh514rtKZnnz08J0AhonQF29thy1mn7IT6IHT9XydGhPkXHLcfdRzmznJHK5N5d9w3fCH1hd~3356464~4338738",
    "Dnt" : "1",
    "Pragma" : "no-cache",
    "Sec-Ch-Ua" : '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "Sec-Ch-Ua-Mobile" : "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest" : "document",
    "Sec-Fetch-Mode" : "navigate",
    "Sec-Fetch-Site" : "none",
    "Sec-Fetch-User" : "?1",
    "Upgrade-Insecure-Requests" : "1",
    "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


try:
    r = requests.get(url, params=pathParams, headers=headers)
    print(r)
    print(r.status_code)
    print(r.content)
    if r.status_code == 200:
        data = r.json()
        if len(data) > 0:
            for suggestion in data:
                if 'id' in suggestion and 'countryCode' in suggestion and suggestion['countryCode'] == 'IN':
                    print(suggestion['id'])
        else:
            # TODO: throw exception
            pass
    else:
        # TODO: throw exception
        pass
except Exception as ex:
    # TODO: throw exception
    print(ex)