'''RDF reader and writer for OAI-PMH harvester and server interface
'''
from lxml import etree
from oaipmh.metadata import MetadataReader
from oaipmh.server import NS_DC

NSRDF = 'http://www.openarchives.org/OAI/2.0/rdf/'
NSOW = 'http://www.ontoweb.org/ontology/1#'
RDF_SCHEMA = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'

rdf_reader = MetadataReader(
    fields={'title': ('textList', 'rdf:RDF/ow:Publication/dc:title/text()'),
            'creator': ('textList', 'rdf:RDF/ow:Publication/dc:creator/text()'),
            'subject': ('textList', 'rdf:RDF/ow:Publication/dc:subject/text()'),
            'description': ('textList', 'rdf:RDF/ow:Publication/dc:description/text()'),
            'publisher': ('textList', 'rdf:RDF/ow:Publication/dc:publisher/text()'),
            'contributor': ('textList', 'rdf:RDF/ow:Publication/dc:contributor/text()'),
            'date': ('textList', 'rdf:RDF/ow:Publication/dc:date/text()'),
            'type': ('textList', 'rdf:RDF/ow:Publication/dc:type/text()'),
            'format': ('textList', 'rdf:RDF/ow:Publication/dc:format/text()'),
            'identifier': ('textList', 'rdf:RDF/ow:Publication/dc:identifier/text()'),
            'source': ('textList', 'rdf:RDF/ow:Publication/dc:source/text()'),
            'language': ('textList', 'rdf:RDF/ow:Publication/dc:language/text()'),
            'relation': ('textList', 'rdf:RDF/ow:Publication/dc:relation/text()'),
            'coverage': ('textList', 'rdf:RDF/ow:Publication/dc:coverage/text()'),
            'rights': ('textList', 'rdf:RDF/ow:Publication/dc:rights/text()')},
    namespaces={'rdf': NSRDF,
                'ow': NSOW,
                'dc': NS_DC})


def dcat2rdf_writer(element, metadata):
    ''' Parse metadata from ckanext-dcat to etree for pyoai (oaipmh) to consume
    A bit ugly implementation for a ready xml string is parsed to lxml.etree
    and back to a string again.

    :param element: An etree element append to
    :param metadata: Actually ready string of rdf xml
    '''
    e_dc = etree.fromstring(metadata)
    element.append(e_dc)


def nsrdf(name):
    return '{%s}%s' % (NSRDF, name)


def nsow(name):
    return '{%s}%s' % (NSOW, name)
