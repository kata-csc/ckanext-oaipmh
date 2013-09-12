# coding: utf-8

from oaipmh.common import Metadata
from importcore import generic_xml_metadata_reader, generic_rdf_metadata_reader

def nrd_metadata_reader(xml):
	result = generic_rdf_metadata_reader(xml)
	return result

def dc_metadata_reader(xml):
	result = generic_xml_metadata_reader(xml)
	return result

