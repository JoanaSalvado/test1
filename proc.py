# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 11:26:04 2018

@author: joana.salvado
"""

import re
from unidecode import unidecode

def proc(col):
    '''Given a column, unicode its values if possible'''
    if col != None:
        try:
            new_col = str(col)
            new_col = unidecode(new_col.decode('utf8'))
            new_col = new_col.strip().strip('"').strip("'").lower().strip()
            new_col = re.sub('  +', ' ', new_col)
            new_col = re.sub('\n', ' ', new_col)
        except UnicodeDecodeError:
            new_col = col
            print col
    else:
        new_col = col
    
    return new_col


if __name__ == '__main__':
  proc()