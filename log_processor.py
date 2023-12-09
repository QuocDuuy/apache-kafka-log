# Import thư viện cần thiết
import re
import json
from user_agents import parse
from datetime import datetime
import requests
import pandas as pd
from pandas import ExcelWriter

# update hàm xử lý dấu
def no_accent_vietnamese(s):
    s = re.sub('[áàảãạăắằẳẵặâấầẩẫậ]', 'a', s)
    s = re.sub('[éèẻẽẹêếềểễệ]', 'e', s)
    s = re.sub('[óòỏõọôốồổỗộơớờởỡợ]', 'o', s)
    s = re.sub('[íìỉĩị]', 'i', s)
    s = re.sub('[úùủũụưứừửữự]', 'u', s)
    s = re.sub('[ýỳỷỹỵ]', 'y', s)
    s = re.sub('đ', 'd', s)
    return s

def get_location(ip_address):
    if ip_address == "":
        location = None
        return location
    while True:
        count = 0
        if count >= 10:
            break
        try:
            response = requests.post("http://ip-api.com/batch", json=[{"query": ip_address}]).json()
            break
        except:
            count += 1
            continue
    if response[0]['status'] == 'fail':
        location = None
        return location
    location = {}
    for ip_info in response:
        #for k,v in ip_info.items():
            #print(k,v)
        location['country'] = no_accent_vietnamese(ip_info['country'])
        location['region'] = no_accent_vietnamese(ip_info['regionName'])
        location['city'] = no_accent_vietnamese(ip_info['city'])
        return location


# Hàm xử lý log
def process_log(topic_name_list, list_api):
    # global content, post_type
    # writer = ExcelWriter("output_log_data.xlsx")
    """
        Hàm xử lý log từ API trên K8s
    """
    # Phân tích log
    log_regex = re.search(
        #r'(?P<timestamp>\d{4}\-\d{2}\-\d{2}\s+\d{2}\:\d{2}\:\d{2}\.\d{3})\s+(?P<level>\w+)\s+\d+\s+---\s+(?P<processor>\[.*?\])\s+(?P<serverLayer>[^ *]*)\s+: :::LOG-REQ-RESP:::\{\"\w+\"\:\"(?P<query>.*?)\",\"\w+\"\:\"(?P<uri>.*?)\",\"\w+\"\:\"(?P<pathInfo>.*?)\",\"\w+\"\:\"(?P<url>.*?)\",\"\w+\"\:\"(?P<header>\[.*?\])\",\"\w+\"\:\"(?P<method>.*?)\",\"\w+\"\:\"(?P<className>.*?)\",\"\w+\"\:\"(?P<serverName>.*?)\",\"\w+\"\:\"(?P<user>.*?)\",(?P<body>.*),"timeReq"\:(?P<timeReq>.*),"timeResp"\:(?P<timeResp>.*)}'

        # r'(?P<timestamp>\d{4}\-\d{2}\-\d{2}\s+\d{2}\:\d{2}\:\d{2}\.\d{3})\s+(?P<level>\w+)\s+\d+\s+---\s+(?P<processor>\[.*?\])\s+(?P<serverLayer>[^ *]*)\s+: :::LOG-REQ-RESP:::\{\"\w+\"\:\"(?P<query>.*?)\",\"\w+\"\:\"(?P<uri>.*?)\",\"\w+\"\:\"(?P<pathInfo>.*?)\",\"\w+\"\:\"(?P<url>.*?)\",\"\w+\"\:(?P<header>\{.*?\}),\"\w+\"\:\"(?P<method>.*?)\",\"\w+\"\:\"(?P<className>.*?)\",\"\w+\"\:\"(?P<serverName>.*?)\",\"\w+\"\:\"(?P<user>.*?)\",(?P<body>.*)\}\s+'
        r'(?P<timestamp>\d{4}\-\d{2}\-\d{2}\s+\d{2}\:\d{2}\:\d{2}\.\d{3})\s+(?P<level>\w+)\s+\d+\s+---\s+(?P<processor>\[.*?\])\s+(?P<serverLayer>[^ *]*)\s+: :::LOG-REQ-RESP-HALOKI:::\{\"\w+\"\:\"(?P<query>.*?)\",\"\w+\"\:\"(?P<uri>.*?)\",\"\w+\"\:\"(?P<pathInfo>.*?)\",\"\w+\"\:\"(?P<url>.*?)\",\"\w+\"\:(?P<header>\{.*?\}),\"\w+\"\:\"(?P<method>.*?)\",\"\w+\"\:\"(?P<className>.*?)\",\"\w+\"\:\"(?P<serverName>.*?)\",\"\w+\"\:\"(?P<user>.*?)\",(?P<body>.*)\}', topic_name_list
    ) # Tìm kiếm các trường dữ liệu lớn nhất trong file log

    if log_regex is None:
            log_regex = re.search(
        #r'(?P<timestamp>\d{4}\-\d{2}\-\d{2}\s+\d{2}\:\d{2}\:\d{2}\.\d{3})\s+(?P<level>\w+)\s+\d+\s+---\s+(?P<processor>\[.*?\])\s+(?P<serverLayer>[^ *]*)\s+: :::LOG-REQ-RESP:::\{\"\w+\"\:\"(?P<query>.*?)\",\"\w+\"\:\"(?P<uri>.*?)\",Python/cleaning/topic/local_process.py\"\w+\"\:\"(?P<pathInfo>.*?)\",\"\w+\"\:\"(?P<url>.*?)\",\"\w+\"\:\"(?P<header>\[.*?\])\",\"\w+\"\:\"(?P<method>.*?)\",\"\w+\"\:\"(?P<className>.*?)\",\"\w+\"\:\"(?P<serverName>.*?)\",\"\w+\"\:\"(?P<user>.*?)\",(?P<body>.*),"timeReq"\:(?P<timeReq>.*),"timeResp"\:(?P<timeResp>.*)}'
        r'(?P<timestamp>\d{4}\-\d{2}\-\d{2}\s+\d{2}\:\d{2}\:\d{2}\.\d{3})\s+(?P<level>\w+)\s+\d+\s+---\s+(?P<processor>\[.*?\])\s+(?P<serverLayer>[^ *]*)\s+: :::LOG-REQ-RESP:::\{\"\w+\"\:\"(?P<query>.*?)\",\"\w+\"\:\"(?P<uri>.*?)\",\"\w+\"\:\"(?P<pathInfo>.*?)\",\"\w+\"\:\"(?P<url>.*?)\",\"\w+\"\:\"(?P<header>\[.*?\])\",\"\w+\"\:\"(?P<method>.*?)\",\"\w+\"\:\"(?P<className>.*?)\",\"\w+\"\:\"(?P<serverName>.*?)\",\"\w+\"\:\"(?P<user>.*?)\",(?P<body>.*)\}'
        , topic_name_list
    ) # Tìm kiếm các trường dữ liệu lớn nhất trong file log
    
    if log_regex != None:

        #timestamp = log_regex.group(1)
        level = log_regex.group(2)
        processor = log_regex.group(3)
        server_layer = log_regex.group(4)
        query = log_regex.group(5)
        uri = log_regex.group(6)
        path_info = log_regex.group(7)
        url = log_regex.group(8)
        header = log_regex.group(9)
        method = log_regex.group(10)
        className = log_regex.group(11)
        serverName = log_regex.group(12)
        user_info = log_regex.group(13)
        body_time = log_regex.group(14)

        if uri in list_api:

            #json_obj include bodyReq, bodyResp, TimeReq, TimeResp, these object have correct json format.
            json_obj = '{' + body_time + '}'  
            #Try-except to solve log pattern wrong but true with above regex.
            try:
                json_obj = json.loads(json_obj)
            except:
                #wrong format json
                #solve {"bodyReq": , ...}
                print('wrong format json: ', uri)
                print(json_obj)
                idx = json_obj.find('"bodyReq":,')
                json_obj = json_obj[:idx] + '"bodyReq":"",' + json_obj[idx+11:]
                try:
                    json_obj = json.loads(json_obj)
                except:
                    print(uri, "not in the format, not collected")
                    return None, None
            bodyReq, bodyResp, timeReq, timeResp = json_obj['bodyReq'], json_obj['bodyResp'], json_obj['timeReq'], json_obj['timeResp']

            #General information of behaviour
            timestamp = datetime.strptime(timeReq, '%Y-%m-%dT%H:%M:%S.%fZ')
            try:
                header = json.loads(header)
            except:
                print(uri, 'wrong json format!')
                return None, None
            if header.get('api_app_key') != None:
                platform = 'mobi'
            elif "Postman" in header['user-agent'][0]: # Xét điều kiện user agent để lấy dữ liệu platform
                platform = "postman"
            else:
                platform = 'web'
            if re.search(r".*pn100\D*(?P<pn100>\w+).*", user_info) == None:
                user_id = user_info
            else:
                user_id = re.search(r".*pn100\D*(?P<pn100>\w+).*", user_info).group(1)
            user_agent_str = header['user-agent'][0]
            user_agent = parse(user_agent_str)
            device_info = {}

            device_info['os'] = user_agent.os.family
            device_info['brand'] = user_agent.device.brand
            
            if user_agent.is_mobile:
                device_info['type'] = 'mobile'
            elif user_agent.is_tablet:
                device_info['type'] = 'tablet'
            #user_agent.is_touch_capable
            elif user_agent.is_pc:
                device_info['type'] = 'pc'
            elif user_agent.is_bot:
                device_info['type'] = 'bot'
            if header.get('ipaddress') != None:
                location = get_location(header['ipaddress'][0])
            else:
                location = None



            ############################### USER ##################################################################################
            if uri == "/users/usrInf/v1": # Xét điều kiện uri để lấy dữ liệu login
                status = bodyResp['status']['success'] # Lấy trạng thái login
                if status == True:
                    action = "login"
                    target_type = "user"
                    # Chuyển log thành json theo dạng dict
                    result = {
                        "timestamp": timestamp
                        , "action": action
                        , "device_info": device_info
                        , "location": location
                        , "user_id": user_id
                        , "target_type": target_type
                        , "platform": platform
                    }
                    # Trả về kết quả chuyển từ dict sang json
                    table = 'user'
                    return result, table
                return None, None
            #Register
            elif uri == '/users/usrSignup/v1':
                status = bodyResp['status']['success']
                if status == True:
                    action = 'register'
                    target_type = 'user'
                    result = {"timestamp": timestamp, "action": action, "device_info": device_info, "location": location, "user_id": user_id, "target_type": target_type, "platform": platform}
                    table = 'user'
                    #update table_1 in user-info
                    # table_1 = 'user_info'
                    return result, table
                return None, None
            #Logout
            elif uri == '/users/usrLogout/v1':
                status = bodyResp['status']['success']
                if status == True:
                    action = 'logout'
                    target_type = 'user'
                    result = {"timestamp": timestamp, "action": action, "device_info": device_info, "location": location, "user_id": user_id, "target_type": target_type, "platform": platform}
                    table = 'user'
                    return result, table
                return None, None

            ############################### SOCIAL ##################################################################################
            #Create post
            elif uri == '/posts/socPostCreat/v1': #"/posts/socPostCreat/v1"
                status = bodyResp['status']['success']
                if status == True:
                    isShare = bodyResp['elements'][0]['isShare']
                    #isShare = 1 --> share
                    if isShare == 1:
                        post_type = bodyResp['elements'][0]['sfeed']['typfeed']
                        action = 'share'

                        if post_type == 'exp' or post_type == 'nor':
                            target_id = bodyResp['elements'][0]['post']
                            target_type = 'post'
                            result = {"timestamp": timestamp, "action": action, "device_info": device_info, "location": location, "user_id": user_id, "post_type": post_type, "target_type": target_type, "target_id": target_id,"platform": platform}
                            table = 'social'
                            # add df
                            # df = pd.DataFrame(result)
                            # df.to_excel(writer, "social", index=False)
                            # writer.save()
                            return result, table

                    #isShare = 0 --> Create post
                    elif isShare == 0:
                        post_type = bodyResp['elements'][0]['typpost']
                        action = 'create_post'
                        if post_type == 'exp':
                            target_id = bodyResp['elements'][0]['id']
                            target_type = 'post'
                            content = bodyResp['elements'][0]['content'] #đã fix
                            title = bodyResp['elements'][0]['title']
                            result = {"timestamp": timestamp, "action": action, "device_info": device_info, "location": location, "user_id": user_id, "post_type": post_type, "target_type": target_type, "target_id": target_id, "content": content, "title": title, "platform": platform}
                            table = 'social'
                            # add df
                            # data = pd.DataFrame(result)
                            # data.to_excel(writer, "social", index=False)
                            # writer.save()
                            return result, table
                        elif post_type == 'nor':
                            target_id = bodyResp['elements'][0]['id']
                            target_type = 'post'
                            # note test
                            try:
                                content = bodyResp['elements'][0]['content']
                            except KeyError:  pass
                            result = {"timestamp": timestamp, "action": action, "device_info": device_info, "location": location, "user_id": user_id, "post_type": post_type, "target_type": target_type, "target_id": target_id, "content": content, "platform": platform}
                            table = 'social'
                            # df = pd.DataFrame(result)
                            # df.to_excel(writer, "social", index=False)
                            # writer.save()
                            return result, table
                return None, None
            elif uri == '/like/socHaha/v1':
                status = bodyResp['status']['success']
                if status == True:           
                    if bodyResp['elements'][0]['target'] == bodyResp['elements'][0]['source']:
                        action = 'haha'
                        post_type = bodyResp['elements'][0]['typpost']
                        target_id = bodyResp['elements'][0]['target']
                        target_type = 'post'
                        result = {"timestamp": timestamp, "action": action, "device_info": device_info, "location": location, "user_id": user_id, "post_type": post_type, "target_type": target_type, "target_id": target_id, "platform": platform}
                        table = 'social'
                        # df = pd.DataFrame(result)
                        # df.to_excel(writer, "social", index=False)
                        # writer.save()
                        return result, table
                    else:
                        action = 'haha'
                        post_type = None
                        target_id = bodyResp['elements'][0]['target']
                        target_type = 'comm'
                        result = {"timestamp": timestamp, "action": action, "device_info": device_info, "location": location, "user_id": user_id, "post_type": post_type, "target_type": target_type, "target_id": target_id, "platform": platform}
                        table  = 'social'
                        # df = pd.DataFrame(result)
                        # df.to_excel(writer, "social", index=False)
                        # writer.save()
                        return result, table
            elif uri == '/comm/socCommCreat/v1': # đã thêm content
                status = bodyResp['status']['success']
                if status == True:
                    if bodyResp['elements'][0]['target'] == bodyResp['elements'][0]['source']:
                        action = 'comment'
                        try:
                            post_type = bodyResp['elements'][0]['typpost']
                        except KeyError: pass
                        target_id = bodyResp['elements'][0]['target']
                        try:
                            content = bodyResp['elements'][0]['content']
                        except KeyError: pass
                        target_type = 'post'
                        result = {"timestamp": timestamp, "action": action, "device_info": device_info, "location": location, "user_id": user_id, "post_type": post_type, "target_type": target_type, "target_id": target_id, "content": content, "platform": platform}
                        table = 'social'
                        # df = pd.DataFrame(result)
                        # df.to_excel(writer, "social", index=False)
                        # writer.save()
                        return result, table
                    else:
                        action = 'comment'
                        post_type = None
                        target_id = bodyResp['elements'][0]['target']
                        target_type = 'comm'
                        result = {"timestamp": timestamp, "action": action, "device_info": device_info, "location": location, "user_id": user_id, "post_type": post_type, "target_type": target_type, "target_id": target_id, "platform": platform}
                        table = 'social'
                        # df = pd.DataFrame(result)
                        # df.to_excel(writer, "social", index=False)
                        return result, table
            ############################### HALOKI #################################################################################
            elif uri == "/acc/hlkAccountInf/v1": # Xét điều kiện uri để lấy dữ liệu login
                status = bodyResp['status']['success'] # Lấy trạng thái login
                # sẽ có trường hợp status == true chứ không phải True
                if status == True or status == "true":
                    ### USER INFOS ###
                    currency = bodyResp['elements'][0]["accInf"]["currency"]

                    #key_business
                    id_haloki =  bodyResp["elements"][0]['userInf']['pn300']
                    
                    #tên người dùng
                    full_name = bodyResp["elements"][0]['userInf']['nv206']
                    # user_name = bodyResp['elements'][0]["accInf"]["name"]

                    #gender
                    gender = bodyResp["elements"][0]['userInf']['nv207']
                    if gender == "62B393050A34FC560E946E355DBFDD0D":
                        gender = "other"
                    elif gender == "9FB2928F56DBE34AAC17FDC977870C85":
                        gender = "male"
                    elif gender == "9642B32CDAB01C8664B70D7C9E27C870":
                        gender = "female"

                    #country    
                    country_code = bodyResp["elements"][0]["userInf"]["country"]                    

                    ### Account Info ###
                    #bodyResp['elements'][0]["accInf"]["nn310"]
                    current_bal = bodyResp['elements'][0]["balInf"]["nn355"]

                    # history created
                    _history = bodyResp["elements"][0]['userInf']['dl146']
                    history_create = datetime.fromtimestamp(_history[:-3]).strftime("%Y-%m-%d %H:%M:%S")

                    # verify status
                    verify_status = bodyResp["elements"][0]["accInf"]["nn303"]
                    if verify_status == 0:
                        verify_status = "not yet"
                    elif verify_status == 1:
                        verify_status = "verified"
                    else: verify_status = "waiting"


                    # Chuyển log thành json theo dạng dict
                    result = {
                        "timestamp": timestamp
                        ,"user_id": user_id
                        ,"id_haloki":id_haloki
                        ,"name":full_name
                        ,"country": country_code
                        ,"status":verify_status
                        ,"device": device_info
                        ,"location": location
                        # ,"money":current_bal
                        # ,"currency": currency
                    }
                   
                    # Trả về kết quả chuyển từ dict sang json
                    table = 'haloki-accounts-test'
                    return result, table
                return None, None
            
            elif uri == '/sendM/hlkSendMsInf/v1':
                status = bodyResp["status"]["success"]
                if status == True or status == "true":
                    
                    total = bodyResp["elements"][0]["total"]

                    for data in bodyResp["elements"][0]["data"]["n310"]:# lấy toàn bộ key n310
                        # các trường trong n310
                        bank_name = data["bank_name"]
                        

            elif uri == '/hlkTranf/hlkTransferInf/v1':
                status = bodyResp["status"]["success"]
                if status == True or status == "true":
                    # đã có user_id:
                    # timestamp có luôn: timestamp
                    # mã giao dịch (đoán mò)
                    trans_code = bodyResp["elements"][0]["n400"]["n401"]["nv408"]
                    #id đối tượng (đoán mò)
                    target_id = bodyResp["elements"][0]["n400"]["pn300"]
                    # stk
                    phone_number_get = bodyResp["elements"][0]["n400"]["n401"]["nv407"]
                    phone_number = phone_number_get
                    result = {
                        "timestamp": timestamp
                        ,"user_id": user_id
                        ,"transaction_id": trans_code
                        ,"target_id": target_id
                        ,"phone_number": phone_number
                    }
                    table = "haloki-transfer-test"
                    return result, table
            
            elif uri == "hlkTransSendMsInf/v1":
                status = bodyResp["status"]["successs"]
                if status == True or status == "true":
                    pass
            
            elif uri == "/sendM/hlkSendMsInf/v1":
                status = bodyResp["status"]["successs"]
                if status == True or status == "true":
                    pass
            else:
                return None, None
        return None, None
        # Bởi vì 4 trường bên dưới được định dạng chuẩn theo json nên chúng ta sẽ không cần phải định dạng lại và parse thẳng theo hàm json.load