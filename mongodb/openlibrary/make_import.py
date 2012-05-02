# coding: utf-8

'''
Make MongoDB mongoimport compatible JSON

{ "_id" : { "$oid" : "4e5bb37258200ed9aabc5d65" }, 
  "name" : "Bob", "age" : 28, "address" : "123 fake street" }


Open Library dumps: a tab separated file with the following columns:

type - type of record (/type/edition, /type/work etc.)
key - unique key of the record. (/books/OL1M etc.)
revision - revision number of the record
last_modified - last modified timestamp
JSON - the complete record in JSON format
'''

IN_FILENAMES = [ 'ol_dump_editions_20120404.txt',
                #'ol_dump_authors_20120404.txt',
                #'ol_dump_works_20120404.txt',
               ]

import json
from pprint import pprint

selected = 0

for in_filename in IN_FILENAMES:
	print '*' * 60 + ' ' + in_filename
	out_filename = in_filename.replace('_dump', '')
	out_filename = out_filename.replace('.txt', '.mongoimport')
	with open(in_filename) as in_file, open(out_filename, 'wt') as out_file:
		for count, line in enumerate(in_file, 1):
			rec_type, key, rev, modified, body = line.split('\t')
			rec = json.loads(body)
			rec['_id'] = key
			del rec['key']
			out_file.write(json.dumps(rec, ensure_ascii=True)+'\n')
			if count % 1000 == 0:
				print count
	print count, 'records written to', out_filename
