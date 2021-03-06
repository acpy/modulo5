# coding: utf-8

'''
Open Library dumps: a tab separated file with the following columns:

type - type of record (/type/edition, /type/work etc.)
key - unique key of the record. (/books/OL1M etc.)
revision - revision number of the record
last_modified - last modified timestamp
JSON - the complete record in JSON format
'''

ED_FILE = 'ol_dump_editions_20120404.txt'
AU_FILE = 'ol_dump_authors_20120404.txt'
WK_FILE = 'ol_dump_works_20120404.txt'

ED_FILE_ORA = ED_FILE.replace('.txt', '_OREILLY.txt')

import json
from pprint import pprint

def has_isbn_prefix(rec, prefixes):
	# drop check digit
	isbns = {cod[:-1] for cod in rec.get('isbn_10', [])}
	# drop check digit and ISBN-13 prefix
	isbns |= {cod[3:-1] for cod in rec.get('isbn_13', [])}
	return any(i.startswith(prefix) for i in isbns 
	                                for prefix in prefixes) 
	
selected = 0

prefixes = ['156592', '0596', '1449']
with open(ED_FILE) as ed_ol, open(ED_FILE_ORA, 'wt') as ed_out:
	for count, line in enumerate(ed_ol, 1):
		rec_type, key, rev, modified, body = line.split('\t')
		rec = json.loads(body)
		if has_isbn_prefix(rec, prefixes): 
			ed_out.write(line)
			selected += 1
		if count % 1000 == 0: 
			print count, selected
