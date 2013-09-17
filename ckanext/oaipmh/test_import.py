# coding: utf-8
# vi:et:ts=8:

from oaipmh.client import Client
import importformats
from importcore import generic_rdf_metadata_reader, generic_xml_metadata_reader

def test_fetch(url, record_id, fmt):
        registry = importformats.create_metadata_registry()
        client = Client(url, registry)
        record = client.getRecord(identifier=record_id, metadataPrefix=fmt)
        return record

def test_list(url):
        registry = importformats.create_metadata_registry()
        client = Client(url, registry)
        return (header.identifier() for header in
                        client.listIdentifiers(metadataPrefix='oai_dc'))

if __name__ == '__main__':
        import sys
        if len(sys.argv) > 3:
                header, metadata, about = test_fetch(sys.argv[1],
                                sys.argv[2], sys.argv[3])
                for item in metadata.getMap().items(): print item
        else:
                for item in test_list(sys.argv[1]): print item

