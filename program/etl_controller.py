#!/usr/bin/env python

from etl_csv import prepare_csv

def etl(source_path):
    df = prepare_csv(source_path)
    df.to_csv(source_path + 'all_data/all_data.csv', index=False)

    return len(df)

#if __name__ == "__main__":
#    result = etl()
#    print result
