# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 11:27:50 2018

@author: joana.salvado
"""

def merge_dicts(*dict_args):
   """
   Given any number of dicts, shallow copy and merge into a new dict,
   precedence goes to key value pairs in latter dicts.
   """
   result = {}
   for dictionary in dict_args:
       result.update(dictionary)
   return result



if __name__ == '__main__':
  merge_dicts()