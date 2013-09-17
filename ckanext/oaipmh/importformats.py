# coding: utf-8
# vi:et:ts=8:

from importcore import generic_xml_metadata_reader, generic_rdf_metadata_reader

from oaipmh.common import Metadata
from oaipmh.metadata import MetadataRegistry
from lxml import etree

def copy_element(source, dest, md, callback = None):
        if source in md:
                md[dest] = md[source]
                copy_element(source + '/language', dest + '/language', md)
                copy_element(source + '/@lang', dest + '/language', md)
                copy_element(source + '/@xml:lang', dest + '/language', md)
                if callback: callback(source, dest)
                return
        count = md.get(source + '.count', 0)
        if not count: return
        md[dest + '.count'] = count
        for i in range(count):
                source_n = '%s.%d' % (source, i)
                dest_n = '%s.%d' % (dest, i)
                copy_element(source_n, dest_n, md, callback)

def nrd_metadata_reader(xml):
        result = generic_rdf_metadata_reader(xml).getMap()

        def person_attrs(source, dest):
                # TODO: here we could also fetch from ISNI/ORCID
                copy_element(source + '/foaf:name', dest + '/name', result)
                copy_element(source + '/foaf:mbox', dest + '/email', result)
                copy_element(source + '/foaf:phone', dest + '/phone', result)

        def document_attrs(source, dest):
                copy_element(source + '/dct:title', dest + '/title', result)
                copy_element(source + '/dct:identifier', dest, result)
                copy_element(source + '/dct:creator',
                                dest + '/creator.0/name', result)
                copy_element(source + '/nrd:creator', dest + '/creator',
                                result, person_attrs)
                copy_element(source + '/dct:description',
                                dest + '/description', result)

        def funding_attrs(source, dest):
                copy_element(source + '/rev:arpfo:funds.0/arpfo:grantNumber',
                                dest + '/fundingNumber', result)
                copy_element(source + '/rev:arpfo:funds.0/rev:arpfo:provides',
                                dest + '/funder', result,
                                person_attrs)

        def file_attrs(source, dest):
                copy_element(source + '/dcat:mediaType',
                                dest + '/mimetype', result)
                copy_element(source + '/fp:checksum.0/fp:checksumValue.0',
                                dest + '/checksum.0', result)
                copy_element(source + '/fp:checksum.0/fp:generator.0',
                                dest + '/checksum.0/algorithm', result)
                copy_element(source + '/dcat:byteSize', dest + '/size', result)

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
                (u'dataset/nrd:owner', u'owner', person_attrs),
                (u'dataset/nrd:creator', u'creator', person_attrs),
                (u'dataset/nrd:distributor', u'distributor', person_attrs),
                (u'dataset/nrd:contributor', u'contributor', person_attrs),
                (u'dataset/nrd:subject', u'subject', None), # fetch tags?
                (u'dataset/nrd:producerProject', u'project', funding_attrs),
                (u'dataset/dct:isPartOf', u'collection', document_attrs),
                (u'dataset/dct:requires', u'requires', None),
                (u'dataset/nrd:discipline', u'discipline', None),
                (u'dataset/nrd:temporal', u'temporalcoverage', None),
                (u'dataset/nrd:spatial', u'spatialcoverage', None), # names?
                (u'dataset/nrd:manifestation', u'resource', file_attrs),
                (u'dataset/nrd:observationMatrix', u'variables', None), # TODO
                (u'dataset/nrd:usedByPublication', u'publication',
                        document_attrs),
                (u'dataset/dct:description', u'description', None),
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
        result = generic_xml_metadata_reader(xml).getMap()
        mapping = [(u'dc:title', u'title.%d'),
                (u'dc:identifier', u'versionidentifier.%d'),
                (u'dc:creator', u'creator.%d/name.0'),
                (u'dc:language', u'language.%d/label.0'),
                (u'dc:description', u'description.%d'),
                (u'dc:subject', u'subject.%d'),
                (u'dc:publisher', u'distributor.%d/name.0'),
                (u'dc:format', u'resource.%d/format.0'),
                (u'dc:contributor', u'contributor.%d/name.0'),
                (u'dc:rights', u'license.%d/description.0'),
                (u'dc:source', u'continuityidentifier.%d'),
        ]
        for source, dest in mapping:
                count = result.get('metadata/oai_dc:dc.0/%s.count' % source, 0)
                result[dest[:dest.index('.%d')] + '.count'] = count
                for i in range(count):
                        source_n = 'metadata/oai_dc:dc.0/%s.%d' % (source, i)
                        copy_element(source_n, dest % i, result)
                        if dest.endswith('.0'):
                                result[dest[:-2] % i + '.count'] = 1
        return Metadata(result)

def create_metadata_registry():
        '''return new metadata registry with all common metadata readers

        :returns: metadata registry
        :rtype: oaipmh.metadata.MetadataRegistry instance
        '''
        registry = MetadataRegistry()
        registry.registerReader('oai_dc', dc_metadata_reader)
        registry.registerReader('nrd', nrd_metadata_reader)
        registry.registerReader('rdf', generic_rdf_metadata_reader)
        registry.registerReader('xml', generic_xml_metadata_reader)
        return registry

