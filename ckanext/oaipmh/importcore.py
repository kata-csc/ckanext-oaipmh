#!/usr/bin/env python

# coding: utf-8

from oaipmh.metadata import MetadataRegistry
from oaipmh.common import Metadata
from lxml import etree
from rdflib import Graph, Namespace
from cStringIO import StringIO
import os

default_namespaces = [
	('dc', 'http://purl.org/dc/elements/1.1/'),
	('dct', 'http://purl.org/dc/terms/'),
	('xsd', 'http://www.w3.org/2001/XMLSchema#'),
	('rdf', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'),
	('rdfs', 'http://www.w3.org/2000/01/rdf-schema#'),
	('skos', 'http://www.w3.org/2004/02/skos/core#'),
	('owl', 'http://www.w3.org/2002/07/owl#'),
	('nrd', 'http://purl.org/net/nrd#'),
	('void', 'http://rdfs.org/ns/void#'),
	('foaf', 'http://xmlns.com/foaf/0.1/'),
	('dcat', 'http://www.w3.org/ns/dcat#'),
	('fp', 'http://downlode.org/Code/RDF/File_Properties/schema#'),
	('arpfo', 'http://vocab.ox.ac.uk/projectfunding#'),
	('org', 'http://www.w3.org/ns/org#'),
	('lvont', 'http://lexvo.org/ontology#'),
	('qb', 'http://purl.org/linked-data/cube#'),
	('prov', 'http://www.w3.org/ns/prov#'),
]

def namespaced_name(name, namespaces):
	for prefix, nsurl in namespaces + default_namespaces:
		if prefix is None: prefix = ''
		else: prefix += ':'
		if name.startswith(nsurl): return prefix + name[len(nsurl):]
		nsurl = '{%s}' % nsurl
		if name.startswith(nsurl): return prefix + name[len(nsurl):]
	return name

def namepath_for_element(prefix, name, indices, md):
	index = indices.get(name, 0)
	indices[name] = index + 1
	last_rel = prefix.split('/')[-1]
	if not is_reverse_relation(name, last_rel):
		md["%s/%s.count" % (prefix, name)] = index + 1
	return "%s/%s.%d" % (prefix, name, index)

def generic_xml_metadata_reader(xml_element):
	def flatten_with(prefix, element, result):
		if element.text: result[prefix] = element.text
		for attr in element.attrib:
			name = namespaced_name(attr, element.nsmap.items())
			result["%s/@%s" % (prefix, name)] = element.attrib[attr]
		indices = {}
		for child in element:
			name = namespaced_name(child.tag, child.nsmap.items())
			child_path = namepath_for_element(prefix, name,
					indices, result)
			flatten_with(child_path, child, result)
	result = {}
	flatten_with(namespaced_name(xml_element.tag,
		xml_element.nsmap.items()), xml_element, result)
	return Metadata(result)

def is_reverse_relation(rel1, rel2):
	try: rel1 = rel1[:rel1.rindex('.')]
	except ValueError: pass
	try: rel2 = rel2[:rel2.rindex('.')]
	except ValueError: pass
	return rel1 == 'rev:' + rel2 or rel2 == 'rev:' + rel1

def generic_rdf_metadata_reader(xml_element):
	g = Graph()
	e = etree.ElementTree(xml_element[0])
	ns = dict((prefix, Namespace(nsurl))
			for prefix, nsurl in default_namespaces)
	# this is kinda stupid, but by far the easiest way:
	# rdflib uses xml.sax so it doesn't understand etree,
	# so text is the only common language spoken by lxml and rdflib
	f = StringIO(etree.tostring(e, xml_declaration=True, encoding="utf-8"))
	g.parse(f, format='xml') # publicID could be the metadata source URL
	# end stupid

	visited = set()
	def flatten_with(prefix, node, result):
		path = prefix.split('/')
		if len(path) > 2 and is_reverse_relation(path[-1], path[-2]):
			return
		result[prefix] = unicode(node)
		if node in visited: return
		visited.add(node)
		if hasattr(node, 'language') and node.language:
			result[prefix + '/language'] = node.language
		indices = {}
		arcs = [(namespaced_name(str(p), list(g.namespaces())), o)
				for p, o in g.predicate_objects(node)] + \
			[('rev:' + namespaced_name(str(p),
				list(g.namespaces())), s)
				for s, p in g.subject_predicates(node)]
		for name, child in arcs:
			child_path = namepath_for_element(prefix, name,
					indices, result)
			flatten_with(child_path, child, result)

	datasets = list(g.subjects(ns['rdf']['type'], ns['nrd']['Dataset']))
	assert len(datasets) == 1
	root_node = datasets[0]
	result = {}
	flatten_with('dataset', root_node, result)
	return Metadata(result)

def dummy_metadata_reader(xml_element):
	return Metadata({'test': 'success'})

def create_metadata_registry():
	from importformats import nrd_metadata_reader, dc_metadata_reader
	registry = MetadataRegistry()
	registry.registerReader('oai_dc', dc_metadata_reader)
	registry.registerReader('nrd', nrd_metadata_reader)
	registry.registerReader('rdf', generic_rdf_metadata_reader)
	return registry

def test_fetch(url, record_id):
	from oaipmh.client import Client
	registry = create_metadata_registry()
	client = Client(url, registry)
	record = client.getRecord(identifier=record_id,
			metadataPrefix='nrd')
	return record

def test_list(url):
	from oaipmh.client import Client
	registry = create_metadata_registry()
	client = Client(url, registry)
	return (header.identifier() for header in
			client.listIdentifiers(metadataPrefix='oai_dc'))

if __name__ == '__main__':
	import sys
	if len(sys.argv) > 2:
		header, metadata, about = test_fetch(sys.argv[1], sys.argv[2])
		for item in metadata.getMap().items(): print item
	else:
		for item in test_list(sys.argv[1]): print item

