import argparse
import numpy as np
import pandas as pd
from sklearn import preprocessing
from sklearn.impute import SimpleImputer

def preproc(X):
    '''module dockstring'''
    X = preprocessing.scale(X)
    imp = SimpleImputer(missing_values=np.nan, strategy='mean')
    imp.fit(X)
    X = imp.transform(X)
    return X

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", help="path to file")
    parser.add_argument("--date_time", help="date time")
    args = parser.parse_args()
    time = args.date_time

    X = pd.read_csv(args.data_dir + '/data_x_' + time)

    X = pd.DataFrame(preproc(X))
    
    X.to_csv(args.data_dir + '/data_x_preproc_' + time, index=False)