import pandas as pd
from pymongo import MongoClient
from datetime import datetime as dt
import json
import csv
import os
# #connect to database
# my_mongodb = MongoClient("mongodb://test-ai:Hahalolo%402022@10.10.12.201:27017,10.10.12.202:27017,10.10.12.203:27017/?replicaSet=test&authSource=test-ai")
# my_col = my_mongodb['test-ai']
# my_doc = my_col['user_post']
# my_mongodb = MongoClient("mongodb://localhost:27017")
# my_col = my_mongodb['datalake']
# my_doc = my_col['social']

# xuất file
def extractDbToFile(db_url, db_port, db_name, db_col):
    global element
    #kết nối tới mongodb
    client = MongoClient(db_url, db_port)
    db = client[db_name]
    coll = db[db_col]
    
    # convert DB records into dataframe
    df = pd.DataFrame(list(coll.find()))
    # if usage == 1:
    #     del df["Unnamed: 0"]
    # elif usage == 2:
    #     if collections == 4:
    #         for element in df['plaid_api']:
    #             # print(element)
    #             if element == "nan":
    #                 del element
    #                 print(element)
                # df_tam = pd.DataFrame(element, columns=["plaid_api"])
                # print(df_tam)

    # convert DF to excel format
    if df.empty != None:
        print("Convert successfully!")
        return df.to_csv(f"output_data_extracted-{usage}-{db_col}.csv", index=None)
    else: return "DF not found"

# import file
def importFiletoDB(csv_path, db_url, db_port, db_name, db_col):
    """ Imports a csv file at path csv_name to a mongo colection
    returns: count of the documants in the new collection
    """
    # kết nối tới mongodb
    client = MongoClient(db_url, db_port)
    db = client[db_name]
    coll = db[db_col]

    # tạo file csv từ file excel
    read_file = pd.read_excel("output_data.xlsx") # tên file quy định hiện tại
    read_file.to_csv("output_data_converted.csv",
                            index=None,
                            header=True
                            )
    df = pd.read_csv("output_data_converted.csv")
# print(df)
    header = ["_id", "timestamp", "action", "device_info", "location", "user_id", "post_type", "target_type", "target_id", "content", "platform", "title"]
# print(header)
    csvfile = open(csv_path, 'r')
    reader = csv.DictReader( csvfile )

    
    for each in reader:
        row={}
        for field in header:
            row[field]=each[field]
            
        print(row)
        coll.insert_one(row)
        # data = pd.read_csv(csv_path)
        # payload = json.loads(data.to_json(orient='records'))
        # coll.remove()
        # coll.insert_many(payload)
        # return coll.count()

if __name__ == '__main__':
    while True:
        os.system("cls")
        print("Metrics: \n1. export file" "\n2. import file" )
        
        result = int(input()) #input của metrics
        if result == 1:
            print("Usages: \n1. user" "\n2. haloki \n3. social")
            usage = int(input()) #input của usage

            # user
            if usage == 1:
                extractDbToFile("mongodb://root:%601234qwer%60@10.10.11.100:27017/?authSource=admin", 27017, "test-api-ai", "user")
            
            # haloki
            elif usage == 2:
                
                # đổi path sang thư mục chứa haloki
                store_path = os.chdir("D:/Code/Python/Intern/Haloki")
                
                count = 0
                chose_lst = ["sessionView_test", "sessionLog_test","reqTrans_test","count_api_test","loginTest","TestSend", "TestTransfer","verifyTest"]
                for n in chose_lst:
                    
                    # for i in range(len(chose_lst)):
                    # for n in chose_lst:
                    print(f"Collection {count}: {n}".format(n))
                    count += 1
        
                collections = int(input())
                extractDbToFile("mongodb://test-haloki-data-analyst:Hahalolo%402022@10.10.11.100:27017/?authSource=test-haloki-data-analyst",27017,"test-haloki-data-analyst",chose_lst[collections])
            
            # social
            elif usage == 3:
                extractDbToFile("localhost", 27017, "datalake","social")                
        else: 
            importFiletoDB("kafka\output_data.csv","localhost", 27017, "datalake", "social")
        n = input("Do you want to continue? (y/n): ")
        if n == "n":
            break