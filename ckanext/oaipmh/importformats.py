# coding: utf-8
# vi:et:ts=8:

import logging
import itertools
import re

import oaipmh.common
import oaipmh.metadata
import lxml.etree
import bs4
import pointfree

import importcore

xml_reader = importcore.generic_xml_metadata_reader
rdf_reader = importcore.generic_rdf_metadata_reader
log = logging.getLogger(__name__)


def ExceptReturn(exception, returns):
    def decorator(f):
        def call(*args, **kwargs):
            try:
                log.debug('call()')
                return f(*args, **kwargs)
            except exception as e:
                log.error('Exception occurred: %s' % e)
                return returns
        log.debug('decorator()')
        return call
    log.debug('ExceptReturn()')
    return decorator


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
                    callback(source, dest, md)
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

        def document_attrs(source, dest, result):
                '''Callback for copying document attributes'''
                copy_element(source + '/dct:title', dest + '/title', result)
                copy_element(source + '/dct:identifier', dest, result)
                copy_element(source + '/dct:creator',
                                dest + '/creator.0/name', result)
                copy_element(source + '/nrd:creator', dest + '/creator',
                                result, person_attrs)
                copy_element(source + '/dct:description',
                                dest + '/description', result)

        def funding_attrs(source, dest, result):
                '''Callback for copying project attributes'''
                copy_element(source + '/rev:arpfo:funds.0/arpfo:grantNumber',
                                dest + '/fundingNumber', result)
                copy_element(source + '/rev:arpfo:funds.0/rev:arpfo:provides',
                                dest + '/funder', result,
                                person_attrs)

        def file_attrs(source, dest, result):
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


@ExceptReturn(exception=Exception, returns=None)
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

        def foaf_handler(x, y, result):
            '''Handler for foaf elements

            :param x: Source element
            :param y: Destination element
            :param result: Metadata dictionary
            :return: Nothing
            :type x: str
            :type y: str
            '''
            foaf_types = ('Agent', 'Organization', 'Group', 'Person', 'Project')
            for t in foaf_types:
                p = '{s}/foaf:{t}'.format(s=x, t=t)
                if p + '.count' in result:
                    for n in range(result[p + '.count']):
                        r = p + '.0'
                        person_attrs(r, y, result)

        def publisher_handler(x, y, result):
            '''Handler for publisher elements

            :param x: Source element
            :param y: Destination element
            :param result: Metadata dictionary
            :return: Nothing
            :type x: str
            :type y: str
            '''
            foaf_handler(x, y, result)
            # # Clean out 'empty' entry. Should only occur in cases where sub-entries exist
            # if y in result and not result[y]:
            #     del result[y]

        def contributor_handler(x, y, result):
            '''Handler for contributor elements

            :param x: Source element
            :param y: Destination element
            :param result: Metadata dictionary
            :return: Nothing
            :type x: str
            :type y: str
            '''
            foaf_handler(x, y, result)

        @pointfree.partial
        def filter_tag_name_namespace(name, namespace, tag):
            '''
            Boolean filter function, for BeautifulSoup find functions, that checks tag's name and namespace
            '''
            return tag.name == name and tag.namespace == namespace

        def fst(x):
            '''
            Return the first element in an Object without failing if not iterable
            '''
            try:
                iter(x)
                return x[0]
            except TypeError:
                return None

        ns = {
            'dct': 'http://purl.org/dc/terms/',
            'dc': 'http://purl.org/dc/elements/1.1/',
        }

        flatten = itertools.chain.from_iterable

        # Populate a BeautifulSoup object
        bs = bs4.BeautifulSoup(lxml.etree.tostring(xml), 'xml')
        dc = bs.metadata.dc

        project_funder, project_funding, project_name, project_homepage = zip(*[
            tuple(a.Project.comment.string.split(u' rahoituspäätös ')) + (a.Project.find('name').string,) + (a.Project.get('about'),)
            for a in dc(filter_tag_name_namespace(name='contributor', namespace=ns['dct']))])

        maintainer, maintainer_email, contact_phone, contact_URL = zip(*list(flatten(
            [[(b.find('name').string, b.mbox.get('resource'), b.phone.get('resource'), b.get('about'))
              for b in a(recursive=False)]
                for a in dc(filter_tag_name_namespace(name='publisher', namespace=ns['dct']), recursive=False)])))

        access_application_url, access_request_url = NotImplemented, NotImplemented

        # Create a unified internal harvester format dict
        unified = dict(
            # ?=dc('source', recursive=False),
            # ?=dc('relation', recursive=False),
            # ?=dc('type', recursive=False),

            algorithm=NotImplemented,

            availability=NotImplemented,

            checksum=dc.hasFormat.File.checksum.Checksum.checksumValue.string,

            # Todo! Using only the first entry, for now
            contact_URL=fst(contact_URL),
            contact_phone=fst(contact_phone),

            direct_download_URL=dc.hasFormat.File.about,

            # discipline=NotImplemented,

            # evdescr=NotImplemented,
            # evtype=NotImplemented,
            # evwhen=NotImplemented,
            # evwho=NotImplemented,

            # geographic_coverage=NotImplemented,

            langtitle=[dict(lang=a.get('xml:lang', ''), value=a.string) for a in dc('title', recursive=False)],

            # DONE!
            language=','.join(sorted([a.string for a in dc('language', recursive=False)])),

            # license_id='notspecified',

            maintainer=dc(
                filter_tag_name_namespace('publisher', ns['dct']), recursive=False) or dc(
                    filter_tag_name_namespace('publisher', ns['dc']), recursive=False),
            maintainer_email=fst(maintainer_email),

            mimetype=dc('hasFormat', recursive=False) or dc('format', recursive=False),

            name=dc('identifier', recursive=False),

            # TEST!
            notes='\r\n\r\n'.join(sorted([a.string for a in dc(
                filter_tag_name_namespace('description', ns['dc']),
                recursive=False)])),

            orgauth=NotImplemented,
            # author=[dict(value=a.string) for a in dc('creator', recursive=False)],
            # author=dc('contributor', recursive=False) if "Person",
            # organization=dc('contributor', recursive=False) if "Organization",

            owner=[a.get('resource') for a in dc('rightsHolder', recursive=False)],

            project_funder=list(project_funder),
            project_funding=list(project_funding),
            project_homepage=list(project_homepage),
            project_name=list(project_name),

            # TEST!
            tag_string=','.join(sorted([a.string for a in dc('subject', recursive=False)])),

            # temporal_coverage_begin=NotImplemented,
            # temporal_coverage_end=NotImplemented,

            # Todo! This should be more exactly picked
            version=dc.modified.string or dc.date.string,
            # version=dc(
            #     partial(filter_tag_name_namespace, 'modified', ns['dct']), recursive=False)[0].string or dc(
            #         partial(filter_tag_name_namespace, 'date', ns['dc']), recursive=False)[0].string,

            version_PID=dc('description', 'Identifier.version:'),
        )
        if not unified['language']:
            unified['langdis'] = 'True'

        result = xml_reader(xml).getMap()
        result['unified'] = unified

        # for source, dest, callback in mapping:
        #     # if callback:
        #     #     copy_element(source, dest, result, callback)
        #     # else:
        #         count = result.get('metadata/oai_dc:dc.0/%s.count' % source, 0)
        #         result[dest[:dest.index('.%d')] + '.count'] = count
        #         for i in range(count):
        #                 source_n = 'metadata/oai_dc:dc.0/%s.%d' % (source, i)
        #                 copy_element(source_n, dest % i, result, callback)
        #                 if dest.endswith('.0'):
        #                         result[dest[:-2] % i + '.count'] = 1

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
