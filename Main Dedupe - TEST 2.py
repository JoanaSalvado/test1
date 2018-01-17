# -*- coding: utf-8 -*-
"""
Created on Thu Jan 04 09:53:04 2018

@author: joana.salvado
"""

import time
import proc as p
import readData as rd

import option_A
import option_B
import option_C
import option_D
import option_E

print '''############# Welcome to our Matching Algorithm  ################

=============== Initializing Part 1 - Data Retrieving =================\n'''

start_time = time.time()


# Query the user to select the country in analysis
country_code=raw_input('''
Please select the country code:
        Austria: at
        Belgium: be
        Czech Republic: ch
        Germany: de
        Spain: es
        Luxembourg: lu
        Netherlands: nl
        Poland: pl
        Portugal: pt
        United Kingdom: uk\n''')


# Query the user to select the file to input

input_file = raw_input('''Please inform the input file path:\n''')

#Save file data into a table
data_d = rd.readData(input_file)

chosen_cols2=data_d['1'].keys()


spot_data=[]

for k,v in data_d.iteritems():   
    new_row = [v[ky] for ky in v.iterkeys()]
    spot_data.append(new_row)
    

print "The selected file has this columns:\n",chosen_cols2

cases={'A':['name'],'B':['name','postcode'],'C':['name','postcode','town'],'D':['name','postcode','town','address'],'E':['name','postcode','town','address','vat']}

print "The possible options are:\n"
for k,v in cases.iteritems():
    print '%s:%s' % (k,v)
    
option = raw_input('Please select which option you want to choose:\n')

chosen_cols3=cases[option]
 
chosen_cols3.insert(0,"line_nr")
chosen_cols3.insert(1,"auth_nr")


spot_d={}
for k,v in data_d.iteritems():
    v['auth_nr'] = None
    clean_row = {ky:v[ky] for ky in chosen_cols3[1:]}
    row_id = v['line_nr']
    spot_d[row_id] = dict(clean_row)



if option == 'A':
    option_A.main(data_d,spot_d,spot_data,chosen_cols3,country_code)
elif option == 'B':
    option_B.main(data_d,spot_d,spot_data,chosen_cols3,country_code)
elif option == 'C':
    option_C.main(data_d,spot_d,spot_data,chosen_cols3,country_code)
elif option == 'D':
    option_D.main(data_d,spot_d,spot_data,chosen_cols3,country_code)
elif option == 'E':
    option_E.main(data_d,spot_d,spot_data,chosen_cols3,country_code)
else:
   print "Please choose a valid option"
    
