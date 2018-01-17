# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 09:48:24 2018

@author: joana.salvado
"""
import re
import os
import dedupe
import time
from unidecode import unidecode
import psycopg2 as psy
import pandas as pd
import psycopg2.extras
from psycopg2.extensions import AsIs
import proc as p
import merge_dicts as md



def main_dedupe_method(data_d,spot_d,spot_data,chosen_cols3,country_code,settings_file,training_file,fields):
        
     
    print '''############# Welcome to our Matching Algorithm  ################
    =============== Initializing Part 1 - Data Retrieving =================\n'''
    start_time = time.time()
    
 
    print "Getting data from BP companies..."                
    
    # Select all companies from selected country
    con = psy.connect(database='msi_bp_%s' % (country_code), user = 'msi_rdr', host='basil.colours.local', password='r_d_r_04')
    con.set_client_encoding('UTF8')
    c = con.cursor(cursor_factory=psy.extras.RealDictCursor)
    
    
    q_companies = '''SELECT --DISTINCT on (auth.company_int_id_number)
	CONCAT(ROW_NUMBER() OVER(ORDER BY auth.company_int_id_number),'c') AS line_nr,
	auth.current_authority_pk as auth_nr,
	auth.s_acct_name as "name",
	auth.s_tax_number as vat,
	ads.address_line1 as address,
	ads.address_line3 as town,
	ads.postcode as postcode
	FROM gdm.authorities auth
	LEFT JOIN gdm.addresses ads
		ON ads.address_id_pk = auth.s_address_id
	WHERE auth.s_acct_name is not null
	ORDER BY auth.company_int_id_number'''    
    c.execute(q_companies)
    companies = c.fetchall()

    comp_d = {}
    for row in companies:
        clean_row = {ky: p.proc(row[ky]) for ky in chosen_cols3[1:]}
        row_id = row['line_nr']
        comp_d[row_id] = dict(clean_row)


    print "Saving all data into database..."

    all_data_d=md.merge_dicts(spot_d,comp_d)

    all_data=[]

    for k,v in all_data_d.iteritems():   
        new_row = [v[ky] for ky in chosen_cols3[1:]]
        new_row.insert(0,k)
        all_data.append(new_row)


    c2 = con.cursor()


    c2.execute('DROP TABLE IF EXISTS htc.all_data')

    field_string = ','.join('%s varchar(200)' % name for name in chosen_cols3)
    c2.execute('''CREATE TABLE htc.all_data
               (%s)''' %(field_string))

    num_cols = len(chosen_cols3)
    mog = "(" + ("%s,"*(num_cols -1)) + "%s)"
    args_str = ','.join(c2.mogrify(mog,x) for x in all_data)
    values = "("+ ','.join(x for x in chosen_cols3) +")"
    c2.execute("INSERT INTO htc.all_data %s VALUES %s" % (values, args_str))
    con.commit()


    print 'Data retrieval took ', time.time() - start_time, 'seconds (', (time.time() - start_time)/60, 'minutes)'
    start_time1 = time.time()


    print "Arranging data to create blocks..."

    u_keys = set()

    for n,m in all_data_d.iteritems():
        for k,v in m.iteritems():
            u_keys.add(v)

    u_keys = list(u_keys)


    block_keys = {}
    count = 0

    for k in u_keys:
        block_keys[k] = count
        count += 1

    
    print 'Checking blocks...'

    ids = []
    for k, v in all_data_d.items():
        ids.append([block_keys[v['name']],v['name'], k])
        if 'postcode' in v.keys():
            if v['postcode'] != None:
                ids.append([block_keys[v['postcode']],v['postcode'], k])
        if 'town' in v.keys():
            if v['town'] != None:
                ids.append([block_keys[v['town']],v['town'], k])


    print 'Writing tables...'

    column_names = ['block_key','block_id','record_id']

    c3 = con.cursor()

    print 'Writing htc.blocks_recordlink_test'

    ## Table with all blocks and companies ##
    c3.execute('DROP TABLE IF EXISTS htc.blocks_recordlink_test')
    field_string = ','.join('%s varchar(200)' % name for name in column_names)
    c3.execute('''CREATE TABLE htc.blocks_recordlink_test
               (%s)''' %(field_string))

    num_cols = len(column_names)
    mog = "(" + ("%s,"*(num_cols -1)) + "%s)"
    args_str = ','.join(c3.mogrify(mog,x) for x in ids)
    values = "("+ ','.join(x for x in column_names) +")"
    c3.execute("INSERT INTO htc.blocks_recordlink_test %s VALUES %s" % (values, args_str))
    con.commit()

    print 'Writing htc.plural_blocks'

    ## Table with all blocks that have more than one company ##

    c3.execute("DROP TABLE IF EXISTS htc.plural_blocks")
    c3.execute("DROP TABLE IF EXISTS htc.covered_blocks")

    c3.execute("""CREATE TABLE htc.plural_blocks as (
                SELECT b.block_id 
                FROM htc.blocks_recordlink_test b
                INNER JOIN (SELECT DISTINCT block_id 
                        FROM htc.blocks_recordlink_test 
                        WHERE record_id like '%c') bp
                ON bp.block_id = b.block_id
                INNER JOIN (SELECT DISTINCT block_id 
                        FROM htc.blocks_recordlink_test 
                        WHERE record_id not like '%c') comp
                ON comp.block_id = b.block_id
                GROUP BY b.block_id 
                HAVING COUNT(*) > 1)""")
    con.commit()

    print 'Writing htc.covered_blocks'

    ## Table with list of blocks for each company ##

    c3.execute("""CREATE TABLE htc.covered_blocks as (
                SELECT record_id, array_agg(r.block_key ORDER BY r.block_key) AS sorted_ids
                FROM htc.blocks_recordlink_test r
                INNER JOIN htc.plural_blocks bl
                ON r.block_id = bl.block_id
                GROUP BY record_id)""")
    con.commit()

    c3 = con.cursor(cursor_factory=psy.extras.RealDictCursor)

    print "Blocking..."

    ## Get all companies and blocks ##

    c3.execute("""SELECT br.record_id, m.*, br.block_key, cb.sorted_ids
               FROM htc.blocks_recordlink_test br
               LEFT JOIN 
		        (SELECT * FROM htc.all_data) m
                    ON m.line_nr = br.record_id
                INNER JOIN htc.covered_blocks cb
                    ON br.record_id = cb.record_id
                INNER JOIN htc.plural_blocks pb
                    ON br.block_id = pb.block_id
                ORDER BY br.block_key""")


    rec_ids = c3.fetchall()


    print 'Writing tables took ', time.time() - start_time1, 'seconds (', (time.time() - start_time1)/60, 'minutes)'
    start_time2 = time.time()

    print "Blocking..."
    ## Arrange data into the needed syntax to use in dedupe ##

    blocks = []
    last_key = 0
    count = 0
    bp_block = []
    comp_block = []


    for rec in rec_ids:
        if rec['block_key'] == last_key:
            if rec['record_id'][-1:] != 'c':
                bp_block.append(tuple([rec['record_id'],
                                       {re.sub('_bp','',your_key): p.proc(rec[your_key]) for your_key in chosen_cols3[1:]},
                                       set(rec['sorted_ids'][:rec['sorted_ids'].index(rec['block_key'])])
                                       ]))  
            else:
                comp_block.append(tuple([rec['record_id'],
                                         comp_d[rec['record_id']],
                                         set(rec['sorted_ids'][:rec['sorted_ids'].index(rec['block_key'])])
                                         ])) 

            
        else:
            if bp_block != [] and comp_block != []:
                blocks.append((bp_block, comp_block))
                bp_block = []
                comp_block = []
                if rec['record_id'][-1:] == 'c':
                    comp_block =[tuple([rec['record_id'],
                                        comp_d[rec['record_id']], 
                                        set(rec['sorted_ids'][:rec['sorted_ids'].index(rec['block_key'])])
                                        ])]
            else:
                            bp_block = [tuple([rec['record_id'],
                                               {re.sub('_bp','',your_key): p.proc(rec[your_key]) for your_key in chosen_cols3[1:]},
                                               set(rec['sorted_ids'][:rec['sorted_ids'].index(rec['block_key'])])
                                               ])]
    
            last_key = rec['block_key']

        count +=1

    blocks.append((bp_block, comp_block))

    blocks = tuple(blocks)


    del rec_ids
    del all_data

    con.close() 



    # Dinamically create fields accordingly to chosen columns
    # We need to see how can we say to our model if we only have name to work with



    print 'Blocking took ', time.time() - start_time2, 'seconds (', (time.time() - start_time2)/60, 'minutes)'
    start_time3 = time.time()


    print "=============== Initializing Part 2 - Dedupe ===============\n"

    print 'Entering Dedupe ...'

    #================================ DEDUPING WITH BLOCKS ==============================================

    if os.path.exists(settings_file):
        print 'Reading from: ', settings_file
        with open(settings_file) as sf :
        
            linker = dedupe.StaticRecordLink(sf)

    else:
        
        linker = dedupe.RecordLink(fields)
        
        linker.sample(data_d,comp_d)

        if os.path.exists(training_file):
            print 'Reading labeled examples from: ', training_file
            with open(training_file) as tf :
                linker.readTraining(tf)

        print 'Starting Active Labeling...'
        dedupe.consoleLabel(linker)
        linker.train()
        with open(training_file, 'w') as tf :
            linker.writeTraining(tf)

        with open(settings_file, 'w') as sf :
            linker.writeSettings(sf)

    print 'Creating blocks...'

    threshold = linker.thresholdBlocks(blocks, recall_weight=2)

    print 'Generating clusters...'

    clusters = linker.matchBlocks(blocks, threshold)
    clust=list(clusters)

    print 'Deduping took ', time.time() - start_time3, 'seconds (', (time.time() - start_time3)/60, 'minutes)'
    start_time4 = time.time()  

    print 'Writing results to database...'

    con = psy.connect(database='msi_bp_%s' % (country_code), user = 'msi_rdr', host='basil.colours.local', password='r_d_r_04')
    c = con.cursor()

    c.execute('DROP TABLE if exists htc.recordlink_blocks_result')
    con.commit()
    c.execute('CREATE TABLE htc.recordlink_blocks_result (spot_id varchar, comp_id varchar, score double precision)')


    #register_adapter(numpy.int64, addapt_numpy_int64)
    num_cols = 3
    mog = "(" + ("%s,"*(num_cols -1)) + "%s)"
    args_str = ','.join(c.mogrify(mog,x[0]+(float(x[1]),)) for x in clust)
    values = "("+ ','.join(x for x in ['spot_id','comp_id', 'score']) +")"
    c.execute("INSERT INTO htc.recordlink_blocks_result %s VALUES %s" % (values, args_str))


    c.execute('DROP TABLE if exists htc.recordlink_blocks_result_all')

    c.execute("""CREATE TABLE htc.recordlink_blocks_result_all as (
               SELECT adt.line_nr as spot_id,br.comp_id as comp_id,br.score as score,
               CASE WHEN br.comp_id IS NULL THEN 'Not a Client'
               ELSE 'Check Match Score'
               END AS "Result",
               adt.name as name_spot,b.name as name_comp, b.auth_nr as auth_nr_comp
               FROM htc.all_data adt
               LEFT JOIN htc.recordlink_blocks_result br
                   ON br.spot_id = adt.line_nr
                  LEFT JOIN 
                    (SELECT *
                    FROM htc.recordlink_blocks_result br
                    INNER JOIN htc.all_data adt
                    ON adt.line_nr= br.comp_id) b
                ON b.spot_id = adt.line_nr
                WHERE adt.line_nr not like '%c%')""")
    con.commit()
    con.close()



    print 'Writing results took ', time.time() - start_time4, 'seconds (', (time.time() - start_time4)/60, 'minutes)'
    print 'Ended in ' , time.time() - start_time, 'seconds (', (time.time() - start_time)/60, 'minutes)'



if __name__ == '__main__':
  main_dedupe_method()


