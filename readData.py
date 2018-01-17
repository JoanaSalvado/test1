# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 11:28:33 2018

@author: joana.salvado
"""


import pandas as pd
import proc as p

def readData(filename):
    """
    Read in our data from a CSV file, store it in our database 
    and create a dictionary of records, 
    where the key is a unique record ID and each value is dict
    """
    data_d = {}
    with open(filename) as f:
        df = pd.read_csv(f, header=0, dtype='str',sep=';')
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df_dict = df.to_dict(orient='index')
        for i,val in df_dict.iteritems():        
            clean_row = [(k, p.proc(v)) for (k, v) in val.iteritems()]
            row_id = val['line_nr']
            data_d[row_id] = dict(clean_row)
    return data_d
    return df


if __name__ == '__main__':
  readData()