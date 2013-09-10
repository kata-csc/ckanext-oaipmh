#!/usr/bin/env python

# coding: utf-8

from oaipmh.metadata import MetadataRegistry
from oaipmh.common import Metadata
from lxml import etree

def dummy_metadata_reader(xml_element):
	return Metadata({'test': 'success', 'foo': 'bar'})

def create_metadata_registry():
	registry = MetadataRegistry()
	registry.registerReader('oai_dc', dummy_metadata_reader)
	#registry.registerReader('nrd', nrd_metadata_reader)
	return registry

def test_fetch(url):
	from oaipmh.client import Client
	registry = create_metadata_registry()
	client = Client(url, registry)
	record_ids = client.listIdentifiers(metadataPrefix='oai_dc')
	def first_from_generator(gen):
		for x in gen: return x
	record_id = first_from_generator(record_ids)
	record = client.getRecord(identifier=record_id.identifier(),
			metadataPrefix='oai_dc')
	return record

if __name__ == '__main__':
	import sys
	header, metadata, about = test_fetch(sys.argv[1])
	for item in metadata.getMap().items(): print item

