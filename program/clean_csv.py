#!/usr/bin/env python
import pandas as pd

def reshape(path, file_name):
    print path+file_name
    _df = pd.read_csv(path+file_name, encoding = 'iso-8859-1')

    df = _df.set_index('TIME').stack()
    df = df.reset_index(name=0)
    df = df.rename(columns={'TIME':'fecha','level_1':'remota',0:'demanda_avg'})

    return df
