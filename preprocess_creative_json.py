"""
@author: Santoshi
"""

import json
import pandas as pd


def process_json_file():
    
    with open('/home/santoshi/airflow/dags/dataset/creatives.json') as json_file:
        data = json.load(json_file)

    df4 = pd.DataFrame(columns=["id", "Status", "instagram_actor_id", "message"])
    for i in range(0, len(data)):
        try:
            if data[i]["object_story_spec"][0] and data[i]["object_story_spec"][0]["instagram_actor_id"] and  data[i]["object_story_spec"][0]["message"]:
                df4.loc[i] = [data[i]["id"],data[i]["status"],data[i]["object_story_spec"][0]["instagram_actor_id"], data[i]["object_story_spec"][0]["message"] ]
            elif data[i]["object_story_spec"][0] and data[i]["object_story_spec"][0]["instagram_actor_id"]:
                df4.loc[i] = [data[i]["id"],data[i]["status"],data[i]["object_story_spec"][0]["instagram_actor_id"],"" ]
            else:
                df4.loc[i] = [data[i]["id"],data[i]["status"],"","" ]
        except:
            df4.loc[i] = [data[i]["id"],data[i]["status"],"","" ]
        
    print(df4.head())
    print(df4.shape)
    df4.to_csv("/home/santoshi/airflow/dags/Result/creatives_processed.csv",index=False)
    