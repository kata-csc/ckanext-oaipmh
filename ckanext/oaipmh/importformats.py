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

	def pick_person_attributes(source, dest):
		# TODO: here we could also fetch from ISNI/ORCID
		copy_element(source + '/foaf:name', dest + '/name', result)
		copy_element(source + '/foaf:mbox', dest + '/email', result)
		copy_element(source + '/foaf:phone', dest + '/phone', result)

	def copy_funding(source, dest):
		copy_element(source + '/rev:arpfo:funds.0/arpfo:grantNumber.0',
				dest + '/fundingNumber', result)
		copy_element(source + '/rev:arpfo:funds.0/rev:arpfo:provides.0',
				dest + '/funder', result,
				pick_person_attributes)

	mapping = [(u'dataset', u'versionidentifier', None),
		(u'dataset/nrd:continuityIdentifier', u'continuityidentifier',
			None),
		(u'dataset/rev:foaf:primaryTopic.0/nrd:metadataIdentifier',
			u'metadata/identifier', None),
		(u'dataset/rev:foaf:primaryTopic.0/nrd:metadataModified',
			u'metadata/modified', None),
		(u'dataset/dct:title', u'title', None),
		(u'dataset/nrd:modified', u'modified', None),
		(u'dataset/nrd:rights', u'rights', None),
		(u'dataset/nrd:language', u'language', None),
		(u'dataset/nrd:owner', u'owner', pick_person_attributes),
		(u'dataset/nrd:creator', u'creator', pick_person_attributes),
		(u'dataset/nrd:distributor', u'distributor',
			pick_person_attributes),
		(u'dataset/nrd:contributor', u'contributor',
			pick_person_attributes),
		(u'dataset/nrd:subject', u'subject', None), # fetch tags?
		(u'dataset/nrd:producerProject', u'project', copy_funding),
	]
	for source, dest, callback in mapping:
		copy_element(source, dest, result, callback)
	try:
		rights = etree.XML(result[u'rights'])
		rightsclass = rights.attrib['RIGHTSCATEGORY'].lower()
		result[u'rightsclass'] = rightsclass
		if rightsclass == 'licensed':
			result[u'license'] = rights[0].text
		if rightsclass == 'contractual':
			result[u'accessURL'] = rights[0].text
	except: pass
	return Metadata(result)

def dc_metadata_reader(xml):
	result = generic_xml_metadata_reader(xml)
	return result

