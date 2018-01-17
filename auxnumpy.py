# -*- coding: utf-8 -*-
"""
Created on Mon Jan 15 16:54:19 2018

@author: joana.salvado
"""

from psycopg2.extensions import AsIs



''' Functions to solve numpy-postgres sql quering error.
    transform float and int'''
    
def addapt_numpy_float32(numpy_float32):
    return AsIs(numpy_float32)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


