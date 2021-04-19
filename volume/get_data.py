import pandas as pd
import numpy as np
import os
import argparse


def get_data(path):
    '''module dockstring'''
    df = pd.read_csv(path)
    y = df[["cons_general"]]
    y[y < 4] = 0
    y[y >= 4] = 1
    X = df.drop(columns=["cons_general"])
    return X, y, df

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", help="path to file")
    parser.add_argument("--date_time", help="date time")
    args = parser.parse_args()
    time = args.date_time
    # print(os.path.abspath(os.getcwd()))
    # print([f for f in os.listdir('.') if os.path.isfile(f)])
    # X, y, df = get_data("/usr/local/airflow/volume/data_processed.csv")
    X, y, df = get_data(args.data_dir + '/data_processed.csv')
    X.to_csv(parser.parse_args().data_dir + '/data_x_' + time, index=False)
    y.to_csv(parser.parse_args().data_dir + '/data_y_' + time, index=False)
    df.to_csv(parser.parse_args().data_dir + '/data_df_'+ time, index=False)