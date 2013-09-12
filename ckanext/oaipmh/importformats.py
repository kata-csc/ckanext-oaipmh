# coding: utf-8

from oaipmh.common import Metadata
from importcore import generic_xml_metadata_reader, generic_rdf_metadata_reader
from lxml import etree

def copy_element(source, dest, md, callback = None):
	if source in md:
		md[dest] = md[dest + '.0'] = md[source]
		md[dest + '.count'] = 1
		if callback: callback(source, dest)
		return
	count = md.get(source + '.count', 0)
	md[dest + '.count'] = count
	for i in range(count):
		source_n = '%s.%d' % (source, i)
		dest_n = '%s.%d' % (dest, i)
		md[dest_n] = md[dest] = md[source_n]
		src_lang = source_n + '/language'
		dst_n_lang = dest_n + '/language'
		dst_lang = dest + '/language'
		if src_lang in md: md[dst_n_lang] = md[dst_lang] = md[src_lang]
		src_lang = source_n + '/@lang'
		if src_lang in md: md[dst_n_lang] = md[dst_lang] = md[src_lang]
		if callback: callback(source_n, dest_n)

def nrd_metadata_reader(xml):
	result = generic_rdf_metadata_reader(xml).getMap()
	copy_element(u'dataset/dct:title', u'title', result)
	copy_element(u'dataset/nrd:modified', u'modified', result)
	copy_element(u'dataset/nrd:rights', u'rights', result)
	try:
		rights = etree.XML(result[u'rights'])
		rightsclass = rights.attrib['RIGHTSCATEGORY'].lower()
		result[u'rightsclass'] = rightsclass
		if rightsclass == 'licensed':
			result[u'license'] = rights[0].text
		if rightsclass == 'contractual':
			result[u'accessURL'] = rights[0].text
	except: pass
	copy_element(u'dataset/nrd:language', u'language', result)
	def pick_person_attributes(source, dest):
		copy_element(source + '/foaf:name', dest + '/name', result)
		copy_element(source + '/foaf:mbox', dest + '/email', result)
		copy_element(source + '/foaf:phone', dest + '/phone', result)
	copy_element(u'dataset/nrd:owner', u'owner',
			result, pick_person_attributes)
	copy_element(u'dataset/nrd:creator', u'creator',
			result, pick_person_attributes)
	copy_element(u'dataset/nrd:distributor', u'distributor',
			result, pick_person_attributes)
	copy_element(u'dataset/nrd:contributor', u'contributor',
			result, pick_person_attributes)
	return Metadata(result)

def dc_metadata_reader(xml):
	result = generic_xml_metadata_reader(xml)
	return result

