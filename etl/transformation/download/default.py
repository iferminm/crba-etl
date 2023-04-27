from io import StringIO

import pandas as pd
import requests


def download_csv(endpoint,params=None,headers=None):
    csv_data = response = requests.get(endpoint, params=params, headers=headers).text
    raw_data = pd.read_csv(StringIO(csv_data), sep=",")
    #TODO establish Great Expectation to check sources  
    return raw_data
