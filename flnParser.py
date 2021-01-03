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

import csv, os, xml.sax.saxutils, logging
import mysql.connector, subprocess
from mysql.connector import Error
from collections import OrderedDict
from xml.etree.ElementTree import parse

from etc.flnConf import *

metadata_dc_list = [ # this is default dubline core format of dspace 
        "dc.contributor.advisor", "dc.contributor.author", "dc.contributor.editor", "dc.contributor.illustrator", "dc.contributor.other",
        "dc.contributor", "dc.coverage.spatial", "dc.coverage.temporal", "dc.creator", "dc.date.accessioned", "dc.date.available",
        "dc.date.copyright", "dc.date.created", "dc.date.issued", "dc.date.submitted", "dc.date.updated", "dc.date", "dc.description.abstract",
        "dc.description.provenance", "dc.description.sponsorship", "dc.description.statementofresponsibility", "dc.description.tableofcontents",
        "dc.description.uri", "dc.description.version", "dc.description", "dc.format.extent", "dc.format.medium", "dc.format.mimetype", "dc.format",
        "dc.identifier.citation", "dc.identifier.govdoc", "dc.identifier.isbn", "dc.identifier.ismn", "dc.identifier.issn", "dc.identifier.other",
        "dc.identifier.sici", "dc.identifier.slug", "dc.identifier.uri", "dc.identifier", "dc.language.iso", "dc.language.rfc3066", "dc.language",
        "dc.publisher", "dc.relation.haspart", "dc.relation.hasversion", "dc.relation.isbasedon", "dc.relation.isformatof", "dc.relation.ispartof",
        "dc.relation.ispartofseries", "dc.relation.isreferencedby", "dc.relation.isreplacedby", "dc.relation.isversionof", "dc.relation.replaces",
        "dc.relation.requires", "dc.relation.uri", "dc.relation", "dc.rights.holder", "dc.rights.uri", "dc.rights", "dc.source.uri", "dc.source",
        "dc.subject.classification", "dc.subject.ddc", "dc.subject.lcc", "dc.subject.lcsh", "dc.subject.mesh", "dc.subject.other", "dc.subject",
        "dc.title.alternative", "dc.title", "dc.type"]

metadata_prism_list = [ # this is default prism format of dspace 
        "prism.contributor.advisor", "prism.contributor.author", "prism.contributor.editor", "prism.contributor.illustrator", "prism.contributor.other",
        "prism.contributor", "prism.coverage.spatial", "prism.coverage.temporal", "prism.creator", "prism.date.accessioned", "prism.date.available",
        "prism.date.copyright", "prism.date.created", "prism.date.issued", "prism.date.submitted", "prism.date.updated", "prism.date", "prism.description.abstract",
        "prism.description.provenance", "prism.description.sponsorship", "prism.description.statementofresponsibility", "prism.description.tableofcontents",
        "prism.description.uri", "prism.description.version", "prism.description", "prism.format.extent", "prism.format.medium", "prism.format.mimetype", "prism.format",
        "prism.identifier.citation", "prism.identifier.govdoc", "prism.identifier.isbn", "prism.identifier.ismn", "prism.identifier.issn", "prism.identifier.other",
        "prism.identifier.sici", "prism.identifier.slug", "prism.identifier.uri", "prism.identifier", "prism.language.iso", "prism.language.rfc3066", "prism.language",
        "prism.publisher", "prism.relation.haspart", "prism.relation.hasversion", "prism.relation.isbasedon", "prism.relation.isformatof", "prism.relation.ispartof",
        "prism.relation.ispartofseries", "prism.relation.isreferencedby", "prism.relation.isreplacedby", "prism.relation.isversionof", "prism.relation.replaces",
        "prism.relation.requires", "prism.relation.uri", "prism.relation", "prism.rights.holder", "prism.rights.uri", "prism.rights", "prism.source.uri", "prism.source",
        "prism.subject.classification", "prism.subject.ddc", "prism.subject.lcc", "prism.subject.lcsh", "prism.subject.mesh", "prism.subject.other", "prism.subject",
        "prism.title.alternative", "prism.title", "prism.type", "prism.volume", "prism.startingPage", "prism.endingPage", "prism.issueIdentifier", "prism.keyword",
        "prism.aggregation.type", "prism.doi", "prism.eid", "prism.pageRange", "prism.pmid", "prism.location" ]   
# new elements:  volume, startingPage, endingPage, issueIdentifier, prism.keyword, prism.aggregation.type, prism.doi, prism.eid,
# prism.pageRange, prism.pmid, prism.location

class pars_meta_data:
        """
        this class import data from csv or db and generate dic 
        """
        
        def __init__(self):
                # create dict from currently supported dc metadata fields in dspace
                self.metadata_dc_fields = OrderedDict.fromkeys(metadata_dc_list, None)

                # create dict from currently supported prism metadata fields in dspace
                self.metadata_prism_fields = OrderedDict.fromkeys(metadata_prism_list, None)

        def from_db (self, db_host, db_name, db_user, db_password):
                """ Connect to MySQL database """
                self.dbconn = None
                try:
                        self.dbconn = mysql.connector.connect(host = db_host,
                                                       database = db_name,
                                                       user = db_user, password = db_password)
                        if self.dbconn.is_connected():
                                print "Connection OK!\n"
                                return self.dbconn
                except Error as e:
                        print e
                        return None


	def make_dc(self):
		"""
		This method transforms the class attribute data into standards
		compliant XML according to the dspace xml format.
		
		This method takes one mandatory argument, one optional argument and
		returns a string. The mandatory argument is the location of the XML
		schema which should be a fully qualified URL. The option arguments
		is the root tag with which to enclose and encapsulate all of the
		DC elements. The default is "metadata" but it can be overridden if
		needed.
		"""
		#set XML declaration
		xmlOut = '<?xml version="1.0" encoding="utf-8" standalone="no"?>\n'
		
		#open encapsulating element tag and deal with namespace and schema declarations
		xmlOut += '<dublin_core schema="dc"> \n\n'
                
                for key, value in self.metadata_dc_fields.iteritems():
                        if value:
                                value = unicode(value)
                                fields = key.split('.')
                                if len(fields) == 2: # so key has no qualifier.
                                        xmlOut += '\t<dcvalue element="%s" qualifier="none">%s</dcvalue>\n' % (fields[1], xml.sax.saxutils.escape(value))

                                if len(fields) == 3: # so fields[2] is qualifier.
                                        xmlOut += '\t<dcvalue element="%s" qualifier="%s">%s</dcvalue>\n' % (fields[1], fields[2], xml.sax.saxutils.escape(value))
		xmlOut += '</dublin_core>\n'
		
		return xmlOut

	def make_prism(self):
		"""
		This method transforms the class attribute data into standards
		compliant XML according to the dspace xml format.
		
		This method takes one mandatory argument, one optional argument and
		returns a string. The mandatory argument is the location of the XML
		schema which should be a fully qualified URL. The option arguments
		is the root tag with which to enclose and encapsulate all of the
		DC elements. The default is "metadata" but it can be overridden if
		needed.
		"""
		#set XML declaration
		xmlOut = '<?xml version="1.0" encoding="utf-8" standalone="no"?>\n'
		
		#open encapsulating element tag and deal with namespace and schema declarations
		xmlOut += '<dublin_core schema="prism"> \n\n'
                
                for key, value in self.metadata_prism_fields.iteritems():
                        if value:
                                value = unicode(value)
                                fields = key.split('.')
                                if len(fields) == 2: # so key has no qualifier.
                                        xmlOut += '\t<dcvalue element="%s" qualifier="none">%s</dcvalue>\n' % (fields[1], xml.sax.saxutils.escape(value))

                                if len(fields) == 3: # so fields[2] is qualifier.
                                        xmlOut += '\t<dcvalue element="%s" qualifier="%s">%s</dcvalue>\n' % (fields[1], fields[2], xml.sax.saxutils.escape((value)))
		xmlOut += '</dublin_core>\n'
		
		return xmlOut

        def load_code_subject(self, csv_file):
                with open(csv_file) as f:
                        f_csv = csv.reader(f)
                        headers = next(f_csv)
                        return {int(rows[1]):rows[0].strip() for rows in f_csv}

        def load_issn_codes(self, issn_codes_file):
                with open(issn_codes_file) as f:
                        PISSN, EISSN = 1,2

                        f_csv = csv.reader(f)
                        headers = next(f_csv)
                        dic = {}

                        for row in f_csv: # strip() will delete any additional space from fields
                                if row[PISSN] != "":
                                        dic[row[PISSN].strip()] =  [tmp.strip() for tmp in row[4].split(';')[:-1]]
                                elif row[EISSN] != "":
                                        dic[row[EISSN].strip()] =  [tmp.strip() for tmp in row[4].split(';')[:-1]]
                        return dic

        def make_collection(self, code_subject):
                """
                refrence: https://wiki.duraspace.org/display/DSDOC4x/Importing+Community+and+Collection+Hierarchy
                """
                xmlOut = "<import_structure>\n"
                xmlOut += "\t<community>\n"
                xmlOut += "\t\t<name>eContents</name>\n"
                xmlOut += "\t\t<description>eContents</description>\n"
                for i in range(10,37):
                        xmlOut += "\t\t<community>\n"
                        sub_community_code = i*100

                        if sub_community_code in code_subject:
                                xmlOut += "\t\t\t<name>%s</name>\n" % code_subject[sub_community_code]

                        for k in range(sub_community_code, sub_community_code+100):
                                if k in code_subject:
                                        xmlOut += "\t\t\t\t<collection><name>%s</name></collection>\n" % code_subject[k]
                        xmlOut += "\t\t</community>\n"

                xmlOut += "\t</community>\n"
                xmlOut += "</import_structure>\n"

                return xmlOut
 
        def load_identifier(self, xml_file, code_subject):
                dic = {}
                tree = parse (xml_file)
                root  = tree.getroot()
                rev_code_subject = {v: k for k, v in code_subject.items()}

                for structure in root:
                   for comm in structure:
                      for sub_comm in comm:
                         try:
                                 id_key =  sub_comm.attrib.values()[0]
                         except:
                                 pass
                         else:
                                 for coll_name in sub_comm.iter('name'):
                                         code = rev_code_subject[coll_name.text]
                                         dic[code] = [id_key, coll_name.text]
                return dic
 
        def import_item(self, root, output):
                IDENTIFIER_BASE = '123456789/'
                identifier_codes = os.listdir(root)
                MY_MAPFILE_STR = '| grep "/storage" | grep "/contents" | cut -d ":" -f 2 | cut -d "/" -f 1-6 >> %smy-mapfile.txt' % LOG_DIR
                f = open(output, 'a')
                f.write("#!/bin/sh\n")

                for code in identifier_codes:
                        mapfile = MAPFILE_DIR_PATH+ '/'+ 'mapfile-'+ code+ '.txt'
                        if not os.path.exists(mapfile):
                                logging.info("%s does not exist so it would be created" % mapfile)
                                open(mapfile, "a")

                        path = os.path.join(root, code)
                        full_path= os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
                        collection_id = IDENTIFIER_BASE + code
                        import_str = "%s import --add --eperson=%s --collection=%s --source=%s -m %s --resume %s" %\
                        (DSPACE, E_PERSON, collection_id, full_path, mapfile, MY_MAPFILE_STR)
                        
                        f.write(import_str)
                        f.write("\n")
                        
                f.close()

class simple_archive_format:

        def __init__(self):
                a = None

        def create_item_dir (self, directory):
                if not os.path.exists(directory):
                        os.makedirs(directory)
                        return directory
                return ""
                        
        def create_dc_file (self, meta_data):
                """
                out_type is of type dublin core format
                """
                return meta_data.make_dc()
               
        def create_prism_file (self, meta_data):
                """
                out_type is of type prism format
                """
                return meta_data.make_prism()

        def create_contents_rec (self, file_path, asset_store_number=1, reg_import_type='register'):
                cont_out = ''
                if reg_import_type == 'register':
                        cont_out += '-r -s %d -f ' % asset_store_number
                        
                cont_out += file_path
                cont_out += "\tbundle:ORIGINAL primary:true\n"

                return cont_out

