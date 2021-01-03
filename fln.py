#!/usr/bin/env python

############################################################################
#   Copyright (C) 2015 by Iman Darabi                                      #
#   <iman.darabi@gmail.com>, https://ir.linkedin.com/in/imandarabi         #
#                                                                          #
#    This program is free software; you can redistribute it and or modify  #
#    it under the terms of the GNU General Public License as published by  #
#    the Free Software Foundation; either version 2 of the License, or     #
#    (at your option) any later version.                                   #
#                                                                          #
#    This program is distributed in the hope that it will be useful,       #
#    but WITHOUT ANY WARRANTY; without even the implied warranty of        #
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         #
#    GNU General Public License for more details.                          #
#                                                                          #
############################################################################

from flnParser import *
from etc.flnConf import *
import time as t
from time import time
import string,random, sqlite3, logging, sys, getopt

def get_next_record_id (db_file):
    table = 'scopusRecords'

    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    c.execute('SELECT endID FROM scopusRecords ORDER BY endID DESC LIMIT 1;')
    try:
        tmp = c.fetchall()[0][0]
    except:
        tmp = 1

    conn.commit()
    conn.close()

    return tmp

def set_next_record_id (db_file, startID, endID, safDirPath):
    #sqlite sample -> http://sebastianraschka.com/Articles/2014_sqlite_in_python_tutorial.html#creating-a-new-sqlite-database
    table = 'scopusRecords'

    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    c.execute("INSERT OR IGNORE INTO {tn} (startID, endID, safDirPath) VALUES ({sid}, {eid}, '{path}')".\
              format(tn=table, sid=startID, eid=endID, path=safDirPath))

    conn.commit()
    conn.close()

def make_saf (issn_codes, identifier_codes):
    start_time = time()

    EMPTY_PATH_ERR = 0
    SIMILAR_REC_ERR = 0
    ISSN_ERR = 0

    SAF_generate = 0
    RECORDS_TO_FETCH = 50000 -1
    OUT_DIR= "/storage/"
    Authors, Abstract, ISBN, E_ISSN, P_ISSN, Language_of_Original_Document, Publisher, Title, Page_start, Page_end, Volume, Author_Keywords, Document_Type, DOI, EID, Issue, Page_count, PubMed_ID, Authors_with_affiliations, path = range(20)

    logging.basicConfig(filename=FLN_LOG, level=logging.DEBUG, format='%(asctime)s %(message)s')

    meta_data = pars_meta_data()
    saf = simple_archive_format()

    dbconn = meta_data.from_db ('database.falinoos.com', 'scopus', 'dspaceUser', 'QCWc+uh%Mvuf2U&9')
    dbconn.arraysize = 1024

    startID = get_next_record_id ("./in_out_dir/dbfalinoos.sqlite3") + 1
    endID = startID + RECORDS_TO_FETCH

    cursor = dbconn.cursor()
 
#    sql_str = """select Authors, Abstract, ISBN, E_ISSN, P_ISSN, Language_of_Original_Document, Publisher, Title, \
#    Page_start, Page_end, Volume, Author_Keywords, Document_Type, scp.DOI, EID, Issue, Page_count, PubMed_ID, \
#    Authors_with_affiliations, local_file_path from crawl.jdb_doi_view cra left join scopus.Scopus_articles scp ON (scp.doi = cra.doi ) WHERE  scp.id>=%#s AND scp.id<=%s AND Year ='2015' """ % (str(startID), str(endID))

    sql_str = """select Authors, Abstract, ISBN, E_ISSN, P_ISSN, Language_of_Original_Document, Publisher, Title, \
    Page_start, Page_end, Volume, Author_Keywords, Document_Type, scp.DOI, EID, Issue, Page_count, PubMed_ID, \
    Authors_with_affiliations, local_file_path from crawl.jdb_doi_view cra left join scopus.Scopus_metadata scp ON (scp.doi = cra.doi ) WHERE  scp.id>=%s AND scp.id<=%s limit 30""" % (str(startID), str(endID))

    try:
        cursor.execute (sql_str)
    except Exception as e:
        logging.info(" SQL error:% " % e)
        dbconn.commit()
        dbconn.close()
        return

    for row in cursor:
        bitstream_path = row[path]
        if not bitstream_path: # no need to continue if path field is empty ... ;)
            EMPTY_PATH_ERR += 1
            continue
        if not os.path.isfile(bitstream_path): # no need to continue if file does not exist ... ;)
            EMPTY_PATH_ERR += 1
            continue

        meta_data.metadata_dc_fields["dc.contributor.author"] = meta_data.metadata_prism_fields["prism.contributor.author"] = row[Authors]
        meta_data.metadata_dc_fields["dc.description.abstract"] = row[Abstract] #meta_data.metadata_prism_fields["prism.description.abstract"] = row[Abstract]
        meta_data.metadata_dc_fields["dc.identifier.isbn"] = meta_data.metadata_prism_fields["prism.identifier.isbn"] = row[ISBN]

        if row[P_ISSN]:         # strip() will delete any additional spaces
            meta_data.metadata_dc_fields["dc.identifier.issn"] = meta_data.metadata_prism_fields["prism.identifier.issn"] = row[P_ISSN].strip()
        elif row[E_ISSN]:
            meta_data.metadata_dc_fields["dc.identifier.issn"] = meta_data.metadata_prism_fields["prism.identifier.issn"] = row[E_ISSN].strip()
        else:
            continue            # no need to continue if there is no issn ... ;O

        meta_data.metadata_dc_fields["dc.language"] = meta_data.metadata_prism_fields["prism.language"] = row[Language_of_Original_Document]
        meta_data.metadata_dc_fields["dc.publisher"] = meta_data.metadata_prism_fields["prism.publisher"] = row[Publisher]
#        meta_data.metadata_dc_fields["dc.relation.isbasedon"] = meta_data.metadata_prism_fields["prism.relation.isbasedon"] = row[References]
        meta_data.metadata_dc_fields["dc.title"] = meta_data.metadata_prism_fields["prism.title"] = row[Title]

        meta_data.metadata_prism_fields["prism.startingPage"] = row[Page_start]
        meta_data.metadata_prism_fields["prism.endingPage"] = row[Page_end]
        meta_data.metadata_prism_fields["prism.volume"] = row[Volume]
        meta_data.metadata_prism_fields["prism.keyword"] = row[Author_Keywords]
        meta_data.metadata_prism_fields["prism.aggregation.type"] = row[Document_Type]
        meta_data.metadata_prism_fields["prism.doi"] = row[DOI]
        meta_data.metadata_prism_fields["prism.eid"] = row[EID]
        meta_data.metadata_prism_fields["prism.issueIdentifier"] = row[Issue]
        meta_data.metadata_prism_fields["prism.pageRange"] = row[Page_count]
        meta_data.metadata_prism_fields["prism.pmid"] = row[PubMed_ID]
        meta_data.metadata_prism_fields["prism.location"] = row[Authors_with_affiliations]

        issn = meta_data.metadata_prism_fields["prism.identifier.issn"]
        if issn not in issn_codes.keys(): # no need to continue if issn dose not exist in issn_codes ;)
            ISSN_ERR += 1
            continue
        
        code = issn_codes[issn][0]
        
        identifier = identifier_codes[int(code)][0]
        tmp = os.path.basename(bitstream_path)

        tmp_path = OUT_DIR+ str(startID)+ '-'+ str(endID)+ '/'+ str(identifier)+ '/'+ tmp
        item_dir = saf.create_item_dir (tmp_path)
        if item_dir == "":
            SIMILAR_REC_ERR += 1
            #                print "error: directory( '%s' )exists!" % tmp_path
            continue 

        item_dc_file = os.path.join (item_dir, 'dublin_core.xml')
        dc_content = saf.create_dc_file (meta_data)
        with open(item_dc_file, 'wt') as f:
            f.write(dc_content.encode('utf8'))
                
        item_prism_file =  os.path.join (item_dir, 'metadata_prism.xml')
        prism_content = saf.create_prism_file (meta_data)
        with open(item_prism_file, 'wt') as f:
            f.write(prism_content.encode('utf8'))
                
        item_contents_file = os.path.join (item_dir, 'contents')

        with open(item_contents_file, 'wt') as f:
            tmp = saf.create_contents_rec (bitstream_path)
            f.write(tmp.encode('utf8'))
            SAF_generate +=1 

    safDirPath = OUT_DIR+ str(startID)+ '-'+ str(endID)+ '/' 
    set_next_record_id("./in_out_dir/dbfalinoos.sqlite3", startID, endID, safDirPath)

    end_time = time()
    
    logging.info("stat: elapsedTime=%d startID=%d, endID=%d, SAF_DIR_generate=%d, safDirPath=%s EMPTY_PATH_ERR=%d, SIMILAR_REC_ERR=%d, ISSN_ERR=%d" \
                 % (int(end_time-start_time), startID, endID, SAF_generate, safDirPath, EMPTY_PATH_ERR, SIMILAR_REC_ERR, ISSN_ERR))
    dbconn.commit()
    dbconn.close()
            
def make_collection(csv_file):
    meta_data = pars_meta_data()

    code_subject = meta_data.load_code_subject(csv_file)
    out = meta_data.make_collection(code_subject)
    with open ('./in_out_dir/collection-input-for-dspace.xml', 'wt') as f:
        f.write(out)
    print '\n New collection XML file has been generated in %s!\n\n' % ('./in_out_dir/collection-input-for-dspace.xml')

def load_identifier_codes(xml_file, code_subject_csv):
    """
    output format: {code:[identifier,collection name]}
    """
    meta_data = pars_meta_data()

    code_subject = meta_data.load_code_subject(code_subject_csv)

    return meta_data.load_identifier(xml_file, code_subject)

def load_issn_codes (issn_codes_file):
    meta_data = pars_meta_data()
    return meta_data.load_issn_codes(issn_codes_file)

def import_items(base_dir, db_file):
    table = 'scopusRecords'
    meta_data = pars_meta_data()
    logging.basicConfig(filename=FLN_LOG, level=logging.DEBUG, format='%(asctime)s %(message)s')

    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    c.execute("SELECT safDirPath, dspaceWrite FROM {tn} WHERE dspaceWrite IS NULL ".\
              format(tn=table))
    
    cursor = c.fetchall()

    for path in cursor:
        path=path[0]

        if os.path.isdir(path):
            meta_data.import_item(path+'123456789/', SCRIPTS_PATH + 'import-1150000-2100000.sh')
        
        #c.execute("UPDATE {tn} SET dspaceWrite='ok' WHERE safDirPath='{idf_value}'".\
         #         format(tn=table, idf_value=path ))
        end_time = time()

    conn.commit()
    conn.close()

def metadata_collection_export(identifier_codes):
    # print 'this is metadata collection export'
    print "#!/bin/sh"
    for key in identifier_codes.keys():
        collection_id =  identifier_codes[key][0]
        cmd = "/dspace/bin/dspace metadata-export -f metadata/collection_%s.csv -i %s" % (collection_id.split('/')[-1] ,collection_id)
        print cmd


############################################
def main(argv):
    identifier_codes = load_identifier_codes("./in_out_dir/collection-output-from-dspace.xml", CODES_SUBJECTS)
    issn_codes = load_issn_codes(ISSN_CODES)

    try:
        opts, args = getopt.getopt(argv,"hmpsx",["help","make_collection", "import_items", "make_saf", "metadata_collection_export"])

    except getopt.GetoptError:
        print 'fln.py <command> -i <inputfile> -o <outputfile>'
        print "\t-m, --make_collection\tinput file: 'in_out_dir/codesANDsubjects.csv'  output file: './in_out_dir/collection-input-for-dspace.xml' "
        print "\t-p, --import_items\tcreate import items script from /storage/123456789/ path  and save status to in_out_dir/dbfalinoos.sqlite3"
        print "\t-s, --make_saf\t\tmake simple archive format files"
        print "\t-x, --metadata_collection_export\texport metadata from collections"
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print 'fln.py <command> -i <inputfile> -o <outputfile>'
            print "\t-m, --make_collection\tinput file: 'in_out_dir/codesANDsubjects.csv'  output file: './in_out_dir/collection-input-for-dspace.xml' "
            print "\t-p, --import_items\tcreate import items script from /storage/123456789/ path  and save status to in_out_dir/dbfalinoos.sqlite3"
            print "\t-s, --make_saf\t\tmake simple archive format files"
            print "\t-x, --metadata_collection_export\texport metadata from collections"
            sys.exit()

        elif opt in ("-m", "--make_collection"):
            make_collection (CODES_SUBJECTS)

        elif opt in ("-p", "--import_items"):
            print "importing items ..."
            import_items('/storage/123456789/', LOCAL_DB)
         
        elif opt in ("-s", "--make_saf"):
            print "Make SIMPLE ARCHIVE FORMAT ..."
            for i in range(100):
                make_saf (issn_codes, identifier_codes)

        elif opt in ("-x", "--metadata_collection_export"):
            metadata_collection_export(identifier_codes)

if __name__ == '__main__':
    main(sys.argv[1:])
