#!/usr/bin/env python

# coding: utf-8

from oaipmh.metadata import MetadataRegistry
from oaipmh.common import Metadata
from lxml import etree

def generic_xml_metadata_reader(xml_element):
	def flatten_with(prefix, element, result):
		if element.text: result[prefix] = element.text
		for attr in element.attrib:
			result["%s.@%s" % (prefix, attr)] = element.attrib[attr]
		indices = {}
		for child in element:
			index = indices.get(child.tag, 0)
			indices[child.tag] = index + 1
			child_path = "%s.%s.%d" % (prefix, child.tag, index)
			flatten_with(child_path, child, result)
	result = {}
	flatten_with(xml_element.tag, xml_element, result)
	return Metadata(result)

def dummy_metadata_reader(xml_element):
	return Metadata({'test': 'success'})

def create_metadata_registry():
	registry = MetadataRegistry()
	registry.registerReader('oai_dc', generic_xml_metadata_reader)
	#registry.registerReader('nrd', nrd_metadata_reader)
	return registry

def test_fetch(url, record_id):
	from oaipmh.client import Client
	registry = create_metadata_registry()
	client = Client(url, registry)
	record = client.getRecord(identifier=record_id,
			metadataPrefix='oai_dc')
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

