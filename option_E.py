# -*- coding: utf-8 -*-
"""
Created on Wed Jan 03 15:46:14 2018

@author: joana.salvado
"""

import main_dedupe_method as mdm


def main(data_d,spot_d,spot_data,chosen_cols3,country_code):

    
    settings_file = 'postgres_settings_spot_E_%s' % (country_code)
    training_file = 'postgres_settings_spot_E_%s.json' %(country_code)
    
    fields = [
        {'field' : 'name', 'variable name': 'name', 'type': 'String'},
        {'field' : 'postcode', 'variable name': 'postcode', 'type': 'Exact', 'has missing':True},
        {'field' : 'town', 'variable name': 'town', 'type': 'String', 'has missing':True},
        {'field' : 'address', 'variable name': 'address', 'type': 'String', 'has missing':True},
        {'field' : 'vat', 'variable name': 'vat', 'type': 'String', 'has missing':True},
        {'type': 'Interaction','interaction variables': ['name', 'postcode','vat']}
        ]
    
    mdm.main_dedupe_method(data_d,spot_d,spot_data,chosen_cols3,country_code,settings_file,training_file,fields)


if __name__ == '__main__':
  main()
