# -*- coding: utf-8 -*-
"""
@author: Santoshi
"""

import pandas as pd
import numpy as np
import re


def process_csv_file():
    data=pd.read_csv("/home/santoshi/airflow/dags/dataset/campaigns.csv")

    # Remove Priority column
    data.drop(['Priority'], axis=1, inplace=True)

    # combine "Landing Page URL 1"  & "Landing Page URL 2" in "Landing Page URL" column
    data["Landing Page URL 1"] = data['Landing Page URL 1'].replace(np.nan, "")
    data["Landing Page URL 2"] = data['Landing Page URL 2'].replace(np.nan, "")
    data["Landing Page URL"] = data["Landing Page URL 1"].astype(str) + data["Landing Page URL 2"].astype(str)

    # Prepend Country Code to Id & replace into Campaign ID
    data["Campaign Name"] = data["Country Code"].astype(str) +" "+ data["ID"].astype(str)

    print(data.head())

    data.to_csv("/home/santoshi/airflow/dags/Result/campaigns_processed.csv", index=False)

