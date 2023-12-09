# Import thư viện Kafka, json và os
from kafka import KafkaConsumer
import json
import os
from log_processor import *
import pymongo

# Danh sách tên các topic trên kafka-server (K8s)
topic_name_list = ["log-haloki"]#, 'log-user'] "log-tour", "log-hotel", "log-flight","log-user", "log-oauth", ]

# Kêt nối với kafka-server(k8s)
consumer = KafkaConsumer (
    bootstrap_servers = '10.10.11.237:9094,10.10.11.238:9094,10.10.11.239:9094'
    , group_id= 'log_collector_ai_da_duy'
    , auto_offset_reset = 'earliest' #auto_offset_reset = 'latest'
    #1, enable_auto_commit=True
    #security_protocol =  'SASL_PLAINTEXT',
    #sasl_mechanism = 'SCRAM-SHA-512',
    #sasl_plain_username='admin-hahalolo',
    #sasl_plain_password='Hahalolo@2021'
    )

# Hàm lấy thông tin từ topic (hứng topic từ K8s) và lưu vào file
def consume_logs(topic_names, list_api):
    # myclient = pymongo.MongoClient("mongodb://data-warehouse:Hahalolo%402022@10.10.12.201:27017,10.10.12.202:27017,10.10.12.203:27017/?replicaSet=test&authSource=ai-data-warehouse")
    if usage == 'user':
        myclient = pymongo.MongoClient("mongodb://localhost:27017")
        mydb = myclient["datalake"]
        consumer.subscribe(topic_names)
    elif usage == 'haloki':
        myclient = pymongo.MongoClient("mongodb://test-haloki-data-analyst:Hahalolo%402022@10.10.11.100:27017/?authSource=test-haloki-data-analyst")
        mydb = myclient["test-haloki-data-analyst"]
        consumer.subscribe(topic_names)

    for message in consumer:
        msg = json.loads(message.value.decode("utf-8"))
        #print(msg['log'])
        # xét điều kiện log từ API với dạng LOG-REQ-RESP
        if usage == "haloki":
            try:
                if "LOG-REQ-RESP-HALOKI" in msg['log']:
                    result, table = process_log(msg["log"], list_api_haloki) # msg["log"] là kiểu str, xử lý log
                    # result = process_log(msg["log"], list_api)
                    if result != None:
                        print(result)
                        print("Logs collected!\n=============================")
                        haloki = mydb[table]
                        # user_info = mydb[table_1]
                        # user_behaviour = mydb["user_analytics"]
                        haloki.insert_one(result)
            except: pass
        elif usage == "user":
            if "LOG-REQ-RESP" in msg['log']:
                result, table = process_log(msg["log"], list_api) # msg["log"] là kiểu str, xử lý log
                # result = process_log(msg["log"], list_api)
                if result != None:
                    print(result)
                    print("Logs collected!\n=============================")
                    user_behaviour = mydb[table]
                    # user_info = mydb[table_1]
                    # user_behaviour = mydb["user_analytics"]
                    user_behaviour.insert_one(result)

# # Chạy hàm main
if __name__== "__main__":
        os.system("cls")
        print("Usage: \n1. user \n2. haloki")
        usage = int(input())
        if usage == 1:
            list_api = ['/users/usrInf/v1', '/users/usrLogout/v1', '/users/usrSignup/v1', '/posts/socPostCreat/v1', '/like/socHaha/v1', '/comm/socCommCreat/v1']
            consume_logs(topic_name_list, list_api)
            
        elif usage == 2:
            list_api_haloki = ['/acc/hlkAccountInf/v1'] #, '/sendM/hlkSendMsInf/v1', '/hlkTranf/hlkTransferInf/v1', "/sendM/hlkSendMsInf/v1"
            consume_logs(topic_name_list, list_api_haloki)
            