import pandas as pd
import numpy as np
import zipfile
import os
import json
import time

##
filename = 'pickles/35513.pickle'


# %%
df = pd.read_pickle(filename)
json = df.to_csv('test.csv')