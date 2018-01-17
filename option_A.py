# -*- coding: utf-8 -*-
"""
Created on Wed Jan 03 15:41:53 2018

@author: joana.salvado
"""

import main_dedupe_method as mdm


def main(data_d,spot_d,spot_data,chosen_cols3,country_code):
    
    
    settings_file = 'postgres_settings_spot_A_%s' % (country_code)
    training_file = 'postgres_settings_spot_A_%s.json' %(country_code)
    
    fields=[
            {'field' : 'name', 'variable name': 'name', 'type': 'String'},
            {'type': 'Interaction','interaction variables': ['name']}
            ]
    
    mdm.main_dedupe_method(data_d,spot_d,spot_data,chosen_cols3,country_code,settings_file,training_file,fields)


if __name__ == '__main__':
  main()