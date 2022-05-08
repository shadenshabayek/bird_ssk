import ast
import csv
import datetime
import glob
#import dotenv
import json
import time
import os
import os.path
import pandas as pd
import numpy as np
import requests



def import_data(file_name):
    data_path = os.path.join(".", "data", file_name)
    df = pd.read_csv(data_path, low_memory=False)
    return df

def import_data_str(file_name):
    data_path = os.path.join(".", "data", file_name)
    df = pd.read_csv(data_path, dtype='str')
    return df
