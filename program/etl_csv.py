#!/usr/bin/env python

import os
import pandas as pd
from clean_csv import reshape

def prepare_csv(source_path):
    dfs = []

    for file in os.listdir(source_path):
        if file.endswith(".csv"):
            df = reshape(source_path, file)
            dfs.append(df)

    df_combined = pd.concat(dfs)
    return df_combined
