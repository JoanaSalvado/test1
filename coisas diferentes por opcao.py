settings_file = 'postgres_settings_spot_B_%s' % (country_code)
training_file = 'postgres_settings_spot_B_%s.json' %(country_code)

#A ==================================================================================================
settings_file = 'postgres_settings_spot_A_%s' % (country_code)
training_file = 'postgres_settings_spot_A_%s.json' %(country_code)


ids = []
    for k, v in all_data_d.items():
        ids.append([block_keys[v['name']],v['name'], k])



fields=[
            {'field' : 'name', 'variable name': 'name', 'type': 'String'},
            {'type': 'Interaction','interaction variables': ['name']}
            ]

			
# B =================================================================================================
settings_file = 'postgres_settings_spot_B_%s' % (country_code)
training_file = 'postgres_settings_spot_B_%s.json' %(country_code)

ids = []
    for k, v in all_data_d.items():
        ids.append([block_keys[v['name']],v['name'], k])
        if 'postcode' in v.keys():
            if v['postcode'] != None:
                ids.append([block_keys[v['postcode']],v['postcode'], k])

fields = [
        {'field' : 'name', 'variable name': 'name', 'type': 'String'},
        {'field' : 'postcode', 'variable name': 'postcode', 'type': 'Exact', 'has missing':True},
        {'type': 'Interaction','interaction variables': ['name', 'postcode']}
        ]

# C =================================================================================================
settings_file = 'postgres_settings_spot_C_%s' % (country_code)
training_file = 'postgres_settings_spot_C_%s.json' %(country_code)

ids = []
    for k, v in all_data_d.items():
        ids.append([block_keys[v['name']],v['name'], k])
        if v['town'] != None:
            ids.append([block_keys[v['town']],v['town'], k])
        if 'postcode' in v.keys():
            if v['postcode'] != None:
                ids.append([block_keys[v['postcode']],v['postcode'], k])

fields = [
        {'field' : 'name', 'variable name': 'name', 'type': 'String'},
        {'field' : 'postcode', 'variable name': 'postcode', 'type': 'Exact', 'has missing':True},
        {'field' : 'town', 'variable name': 'town', 'type': 'String', 'has missing':True},
        {'type': 'Interaction','interaction variables': ['name', 'postcode']}
        ]
		
# D =================================================================================================
settings_file = 'postgres_settings_spot_D_%s' % (country_code)
training_file = 'postgres_settings_spot_D_%s.json' %(country_code)

    ids = []
    for k, v in all_data_d.items():
        ids.append([block_keys[v['name']],v['name'], k])
        if v['town'] != None:
            ids.append([block_keys[v['town']],v['town'], k])
        if 'postcode' in v.keys():
            if v['postcode'] != None:
                ids.append([block_keys[v['postcode']],v['postcode'], k])

    fields = [
        {'field' : 'name', 'variable name': 'name', 'type': 'String'},
        {'field' : 'postcode', 'variable name': 'postcode', 'type': 'Exact', 'has missing':True},
        {'field' : 'town', 'variable name': 'town', 'type': 'String', 'has missing':True},
        {'field' : 'address', 'variable name': 'address', 'type': 'String', 'has missing':True},
        {'type': 'Interaction','interaction variables': ['name', 'postcode']}
        ]

# E =================================================================================================
# MANTER O E COMO ESTÁ? TALVEX PORQUE É O MAIS DIFERENTE (VAT)
settings_file = 'postgres_settings_spot_E_%s' % (country_code)
training_file = 'postgres_settings_spot_E_%s.json' %(country_code)	