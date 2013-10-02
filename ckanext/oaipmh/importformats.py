# coding: utf-8
# vi:et:ts=8:

import oaipmh.common
import oaipmh.metadata
import lxml.etree

import importcore

xml_reader = importcore.generic_xml_metadata_reader
rdf_reader = importcore.generic_rdf_metadata_reader


def copy_element(source, dest, md, callback=None):
        '''Copy element in metadata dictionary from one key to another

        This function changes the metadata dictionary, md, by copying the
        value corresponding to key source to the value corresponding to
        the key dest.  It also copies all elements if it is an indexed
        element, and language information that pertains to the copied
        element.  The parameter callback, if given, is called with any
        element names formed (indexed or no).

        :param source: key to be copied
        :type source: string
        :param dest: key to copy to
        :type dest: string
        :param md: a metadata dictionary to update
        :type md: hash from string to any value (inout)
        :param callback: optional callback function, called with source,
                dest and their indexed versions
        :type callback: function of (string, string) -> None
        '''
        # Check if key exists in dictionary
        if source in md:
                md[dest] = md[source]
                copy_element(source + '/language', dest + '/language', md)
                copy_element(source + '/@lang', dest + '/language', md)
                copy_element(source + '/@xml:lang', dest + '/language', md)
                copy_element(source + '/@rdf:resource', dest, md)  # overwrites any possible element text

                # Call possible callback function
                if callback:
                    callback(source, dest)
                return

        count = md.get(source + '.count', 0)
        if not count:
            return

        # Add {dest}.count field to md
        md[dest + '.count'] = count
        for i in range(count):
                source_n = '%s.%d' % (source, i)
                dest_n = '%s.%d' % (dest, i)
                copy_element(source_n, dest_n, md, callback)


def person_attrs(source, dest, result):
    '''Callback for copying person attributes'''
    # TODO: here we could also fetch from ISNI/ORCID
    copy_element(source + '/foaf:name', dest + '/name', result)
    copy_element(source + '/foaf:mbox', dest + '/email', result)
    copy_element(source + '/foaf:phone', dest + '/phone', result)


def nrd_metadata_reader(xml):
        '''Read metadata in NRD schema

        This function takes NRD metadata as an lxml.etree.Element object,
        and returns the same metadata as a dictionary, with central TTA
        elements picked to format-independent keys.

        :param xml: RDF metadata as XML-encoded NRD
        :type xml: lxml.etree.Element instance
        :returns: a metadata dictionary
        :rtype: a hash from string to any value
        '''
        result = rdf_reader(xml).getMap()

        def document_attrs(source, dest):
                '''Callback for copying document attributes'''
                copy_element(source + '/dct:title', dest + '/title', result)
                copy_element(source + '/dct:identifier', dest, result)
                copy_element(source + '/dct:creator',
                                dest + '/creator.0/name', result)
                copy_element(source + '/nrd:creator', dest + '/creator',
                                result, person_attrs)
                copy_element(source + '/dct:description',
                                dest + '/description', result)

        def funding_attrs(source, dest):
                '''Callback for copying project attributes'''
                copy_element(source + '/rev:arpfo:funds.0/arpfo:grantNumber',
                                dest + '/fundingNumber', result)
                copy_element(source + '/rev:arpfo:funds.0/rev:arpfo:provides',
                                dest + '/funder', result,
                                person_attrs)

        def file_attrs(source, dest):
                '''Callback for copying manifestation attributes'''
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
                rights = lxml.etree.XML(result[u'rights'])
                rightsclass = rights.attrib['RIGHTSCATEGORY'].lower()
                result[u'rightsclass'] = rightsclass
                if rightsclass == 'licensed':
                        result[u'license'] = rights[0].text
                if rightsclass == 'contractual':
                        result[u'accessURL'] = rights[0].text
        except:
            pass
        return oaipmh.common.Metadata(result)


def dc_metadata_reader(xml):
        '''Read metadata in oai_dc schema

        This function takes oai_dc metadata as an lxml.etree.Element
        object, and returns the same metadata as a dictionary, with
        central TTA elements picked to format-independent keys.

        :param xml: oai_dc metadata
        :type xml: lxml.etree.Element instance
        :returns: a metadata dictionary
        :rtype: a hash from string to any value
        '''

        def foaf_type_counter(x, y, ts):
            for t in ts:
                print(t)
                p = '{s}/foaf:{t}'.format(s=x, t=t)
                print(p)
                if p + '.count' in result:
                    print(p + '.count')
                    for n in range(result[p + '.count']):
                        r = p + '.0'
                        print(r)
                        person_attrs(r, y, result)

        def foaf_agent_derived_handler(x, y):
            print('src: {0}'.format(x))
            print('dst: {0}'.format(y))

            foaf_types = ('Agent', 'Organization', 'Group', 'Person', 'Project')
            foaf_type_counter(x, y, foaf_types)

        result = xml_reader(xml).getMap()
        mapping = [
            (u'dc:title', u'title.%d', None),
            (u'dc:identifier', u'versionidentifier.%d', None),
            (u'dc:creator', u'creator.%d/name.0', None),
            (u'dc:language', u'language.%d/label.0', None),
            (u'dc:description', u'description.%d', None),
            (u'dc:subject', u'subject.%d', None),
            (u'dc:publisher', u'distributor.%d', foaf_agent_derived_handler),
            (u'dc:format', u'resource.%d/format.0', None),
            (u'dc:contributor', u'contributor.%d', foaf_agent_derived_handler),
            (u'dc:rights', u'license.%d/description.0', None),
            (u'dc:source', u'continuityidentifier.%d', None),
        ]
        for source, dest, callback in mapping:
                count = result.get('metadata/oai_dc:dc.0/%s.count' % source, 0)
                result[dest[:dest.index('.%d')] + '.count'] = count
                for i in range(count):
                        source_n = 'metadata/oai_dc:dc.0/%s.%d' % (source, i)
                        copy_element(source_n, dest % i, result, callback)
                        if dest.endswith('.0'):
                                result[dest[:-2] % i + '.count'] = 1
        return oaipmh.common.Metadata(result)


def create_metadata_registry():
        '''Return new metadata registry with all common metadata readers

        The readers currently implemented are for metadataPrefixes
        oai_dc, nrd, rdf and xml.

        :returns: metadata registry instance
        :rtype: oaipmh.metadata.MetadataRegistry
        '''
        registry = oaipmh.metadata.MetadataRegistry()
        registry.registerReader('oai_dc', dc_metadata_reader)
        registry.registerReader('nrd', nrd_metadata_reader)
        registry.registerReader('rdf', rdf_reader)
        registry.registerReader('xml', xml_reader)
        return registry
