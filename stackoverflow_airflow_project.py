import pandas as pd
import tweepy
from datetime import datetime
import json
import s3fs
import requests
from datetime import timedelta
import time

def run_stackoverflow_etl():

    #Get today's date in local time
    d = datetime.today() 
    y = datetime.today()  -  timedelta(1)

    #Convert it to Y-M-D format
    t=d.date().strftime("%Y-%m-%d")
    t1=y.date().strftime("%Y-%m-%d")

    #Converting to GMT and then EPOCH format
    to=t +" "+"4:30:00"
    q='%Y-%m-%d %H:%M:%S'
    epochto = int(time.mktime(time.strptime(to,q)))
    fr= t1 +" "+"5:30:00"
    epochfrom = int(time.mktime(time.strptime(fr,q)))

    url = "https://api.stackexchange.com/2.3/posts?fromdate="+str(epochfrom)+"&todate="+str(epochto)+"&order=desc&sort=activity&site=stackoverflow"


    r = requests.get(url)

    data = r.json()["items"]

    # print("hold")

    df = {
        "owner_id":[],
        "owner_profile_link":[],
        "score":[],
        "post_id":[],
        "post_type": [],
        "creation_date":[],
        "post_link":[]

    }

    for items in data:
        df["owner_id"].append(items["owner"]["user_id"])
        df["owner_profile_link"].append(items["owner"]["link"])
        df["score"].append(items["score"])
        df["post_id"].append(items["post_id"])
        df["creation_date"].append(datetime.fromtimestamp(items["creation_date"]))
        df["post_link"].append(items["link"])
        df["post_type"].append(items["post_type"])

    final_data = pd.DataFrame(df)
    final_data.to_csv("stackoverflow_data.csv")

# print(df)

