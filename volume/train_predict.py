import sys
import json
import argparse
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_predict
from sklearn.metrics import confusion_matrix

def train_predict(X, y, penalty, C, solver):
    '''module dockstring'''
    # Linear model
    clf = LogisticRegression(penalty=penalty, C=C, solver=solver)
    yhat = cross_val_predict(clf, X, y, cv=10)
    return yhat, clf

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", help="path to file")
    parser.add_argument("--date_time", help="date time")
    args = parser.parse_args()
    time = args.date_time

    X = pd.read_csv(args.data_dir + '/data_x_preproc_' + time)
    y = pd.read_csv(args.data_dir + '/data_y_' + time)
    y = y['cons_general']
    # PENALTY = str(sys.argv[1]) if len(sys.argv) > 1 else 'l2'
    # C = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
    # SOLVER = str(sys.argv[3]) if len(sys.argv) > 3 else 'lbfgs'
    PENALTY = 'l2'
    C = 1.0
    SOLVER = 'lbfgs'
    y_hat, clf = train_predict(X=X, y=y, penalty=PENALTY, C=C, solver=SOLVER)
    acc = np.mean(y_hat==y)
    tn, fp, fn, tp = confusion_matrix(y, y_hat).ravel()
    specificity = tn / (tn + fp)
    sensitivity = tp / (tp + fn)
    with open(args.data_dir + '/metrics_{}.json'.format(time), 'w') as outfile:
        json.dump({
            "accuracy": acc,
            "specificity": specificity,
            "sensitivity":sensitivity
            }, outfile)