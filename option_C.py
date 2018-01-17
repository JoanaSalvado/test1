# -*- coding: utf-8 -*-
"""
Created on Wed Jan 03 15:45:05 2018

@author: joana.salvado
"""

import main_dedupe_method as mdm


def main(data_d,spot_d,spot_data,chosen_cols3,country_code):
    
    settings_file = 'postgres_settings_spot_C_%s' % (country_code)
    training_file = 'postgres_settings_spot_C_%s.json' %(country_code)
    
    fields = [
        {'field' : 'name', 'variable name': 'name', 'type': 'String'},
        {'field' : 'postcode', 'variable name': 'postcode', 'type': 'Exact', 'has missing':True},
        {'field' : 'town', 'variable name': 'town', 'type': 'String', 'has missing':True},
        {'type': 'Interaction','interaction variables': ['name', 'postcode']}
        ]
    
    mdm.main_dedupe_method(data_d,spot_d,spot_data,chosen_cols3,country_code,settings_file,training_file,fields)


if __name__ == '__main__':
  main()