# coding: utf-8
# vi:et:ts=8:

import logging
import itertools as it
import re
import urllib

import oaipmh.common as oc
import oaipmh.metadata as om
import lxml.etree
import bs4
import pointfree as pf
import functionally as fun
from functionally import first

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
        return oc.Metadata(result)


# @ExceptReturn(exception=Exception, returns=None)
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

        @pf.partial
        def filter_tag_name_namespace(name, namespace, tag):
            '''
            Boolean filter function, for BeautifulSoup find functions, that checks tag's name and namespace
            '''
            return tag.name == name and tag.namespace == namespace

        def get_version_pid(tag_tree):
            '''
            Generate results for version_PID
            :param tag_tree: Metadata Tag in BeautifulSoup tree
            :type tag_tree: bs4.Tag
            '''
            # IDA
            for a in tag_tree('description', recursive=False):
                s = re.search('Identifier.version: (.+)', a.string)
                if s and s.group(1):
                    yield s.group(1)

        def get_project_stuff(tag_tree):
            for a in tag_tree(filter_tag_name_namespace(name='contributor', namespace=ns['dct']), recursive=False):
                if a.Project:
                    p = a.Project.comment.string.split(u' rahoituspäätös ') if a.Project.comment else ('', '')
                    n = a.Project.find('name').string if a.Project.find('name') else ''
                    m = a.Project.get('about', '')
                    yield tuple(p) + (n,) + (m,)

        def get_maintainer_stuff(tag_tree):
            for a in tag_tree(filter_tag_name_namespace(name='publisher', namespace=ns['dct']), recursive=False):
                for b in a(recursive=False):
                    n = b.find('name').string if b.find('name') else ''
                    m = b.mbox.get('resource', '') if b.mbox else ''
                    p = b.phone.get('resource', '') if b.phone else ''
                    h = b.get('about', '')
                    yield (n, m, p, h)

        def get_data_pids(tag_tree):
            '''
            Returns an iterator over data PIDs from metadata
            '''
            def pids(t):
                '''
                Get data 'PIDs' from OAI-DC and IDA
                '''
                for p in t('identifier', recursive=False):
                    yield p.string

            pids1, pids2 = it.tee(pids(tag_tree), 2)
            pred = lambda x: re.search('urn', x, flags=re.I)
            return it.chain(it.ifilter(pred, pids1), it.ifilterfalse(pred, pids2))

        def get_checksum(tag_tree):
            try:
                return tag_tree.hasFormat.File.checksum.Checksum.checksumValue.string
            except Exception as e:
                log.info('Checksum missing from dataset!')

        def get_download(tag_tree):
            # @ExceptReturn(exception=Exception, returns=None)
            def ida():
                try:
                    yield tag_tree.hasFormat.File.get('about')
                except Exception as e:
                    pass

            # @ExceptReturn(Exception, None)
            def helda():
                for pid in get_data_pids(tag_tree):
                    if pid.startswith('http'):
                        yield pid

            return it.chain(ida(), helda())

        def get_org_auth(tag_tree):
            '''
            Returns an iterator over organization-author dicts from metadata
            '''
            def oai_dc():
                '''
                Get 'author' and 'organization' information from OAI-DC
                '''
                for c in tag_tree(filter_tag_name_namespace(name='creator', namespace=ns['dc']), recursive=False):
                    yield {'org': '', 'value': c.string}
                for c in tag_tree(filter_tag_name_namespace(name='contributor', namespace=ns['dc']), recursive=False):
                    yield {'org': '', 'value': c.string}

            def ida():
                '''
                Get 'author' and 'organization' information from IDA
                '''
                for c in tag_tree(filter_tag_name_namespace(name='contributor', namespace=ns['dct']), recursive=False):
                    # Todo! Simplify this!
                    if c.Person and c.Organization:
                        yield {'org': c.Organization.find('name').string, 'value': c.Person.find('name').string}
                    elif c.Person:
                        yield {'org': '', 'value': c.Person.find('name').string}
                    elif c.Organization:
                        yield {'org': c.Organization.find('name').string, 'value': ''}

            return ida() if first(ida()) else oai_dc()

        def get_algorithm(tag_tree):
            # @ExceptReturn(exception=Exception, passes=True)
            def ida():
                try:
                    yield tag_tree.hasFormat.File.checksum.Checksum.generator.Algorithm.get('about').split('/')[-1]
                except Exception as e:
                    pass
            return ida()

        def get_rights(tag_tree):
            def ida():
                try:
                    decl = tag_tree.find(filter_tag_name_namespace(name='rights', namespace=ns['dct'])).RightsDeclaration.string
                    cat = tag_tree.find(filter_tag_name_namespace(name='rights', namespace=ns['dct'])).RightsDeclaration.get('RIGHTSCATEGORY')
                    avail = lid = lurl = aaurl = None
                    if cat == 'COPYRIGHTED':
                        avail = 'contact_owner'
                        lid = 'notspecified'
                    elif cat == 'LICENSED':
                        avail = 'direct_download'
                        lid = 'notspecified'
                        lurl = decl
                    elif cat == 'CONTRACTUAL':
                        avail = 'access_application'
                        lid = 'notspecified'
                        aaurl = decl
                    elif cat == 'PUBLIC DOMAIN':
                        avail = 'direct_download'
                        lid = 'other-pd'
                    elif cat == 'OTHER':
                        avail = 'direct_download'
                        lid = 'other-open'
                        lurl = decl
                    else:
                        raise ValueError('Unfamiliar rights encountered from IDA!')
                    return avail, lid, lurl, aaurl
                except AttributeError as e:
                    log.info('IDA rights not detected. Probably not harvesting IDA. {e}'.format(e=e))
                    pass

            # TODO! Does license text belong in url?
            def oai_dc():
                try:
                    return '', '', tag_tree.find(filter_tag_name_namespace(name='rights', namespace=ns['dc'])).string, ''
                except AttributeError as e:
                    log.info('OAI_DC rights not detected. Probably just missing. {e}'.format(e=e))
                    pass

            return ida() or oai_dc()

        ns = {
            'dct': 'http://purl.org/dc/terms/',
            'dc': 'http://purl.org/dc/elements/1.1/',
        }

        # Populate a BeautifulSoup object
        bs = bs4.BeautifulSoup(lxml.etree.tostring(xml), 'xml')
        dc = bs.metadata.dc

        project_funder, project_funding, project_name, project_homepage = zip(*get_project_stuff(dc)) or ('', '', '', '')

        # Todo! This needs to be improved to use also simple-dc
        # dc(filter_tag_name_namespace('publisher', ns['dc']), recursive=False)
        maintainer, maintainer_email, contact_phone, contact_URL = zip(*get_maintainer_stuff(dc)) or ('', '', '', '')

        availability, license_id, license_url, access_application_url = get_rights(dc) or ('', '', '', '')

        # Create a unified internal harvester format dict
        unified = dict(
            # ?=dc('source', recursive=False),
            # ?=dc('relation', recursive=False),
            # ?=dc('type', recursive=False),

            access_application_URL=access_application_url or '',

            algorithm=first(get_algorithm(dc)) or '',

            availability=availability or '',

            checksum=get_checksum(dc) or '',

            # Todo! Using only the first entry, for now
            contact_URL=first(contact_URL) or '',
            contact_phone=first(contact_phone) or '',

            direct_download_URL=first(get_download(dc)) or '',

            # Todo! Implement
            discipline='',

            # Todo! Should be possible to implement with QDC, but not with OAI_DC
            evdescr=[],
            evtype=[],
            evwhen=[],
            evwho=[],

            # Todo! Implement
            geographic_coverage='',

            langtitle=[dict(lang=a.get('xml:lang', ''), value=a.string) for a in dc('title', recursive=False)],

            # DONE!
            language=','.join(sorted([a.string for a in dc('language', recursive=False)])),

            license_URL=license_url or '',
            license_id=license_id or 'notspecified',

            # Todo! Using only the first entry, for now
            maintainer=first(maintainer) or '',
            maintainer_email=first(maintainer_email) or '',

            # Todo! IDA currently doesn't produce this, maybe in future
            # dc('hasFormat', recursive=False)
            mimetype=first([a.string for a in dc('format', text=re.compile('/'), recursive=False)]) or '',

            name=first(it.imap(urllib.quote_plus, get_data_pids(dc))) or '',
            # name=first(it.imap(pf.partial(urllib.quote_plus, safe=':'), get_data_pids(dc))) or '',

            # TEST!
            notes='\r\n\r\n'.join(sorted([a.string for a in dc(
                filter_tag_name_namespace('description', ns['dc']),
                recursive=False)])) or '',

            orgauth=list(get_org_auth(dc)),

            # Todo! Using only the first entry, for now
            owner=first([a.get('resource') for a in dc('rightsHolder', recursive=False)]) or '',

            # Todo! Using only the first entry, for now
            project_funder=first(project_funder) or '',
            project_funding=first(project_funding) or '',
            project_homepage=first(project_homepage) or '',
            project_name=first(project_name) or '',

            # TEST!
            tag_string=','.join(sorted([a.string for a in dc('subject', recursive=False)])) or '',

            # Todo! Implement if possible
            temporal_coverage_begin='',
            temporal_coverage_end='',

            # Todo! This should be more exactly picked
            version=(dc.modified or dc.date).string if (dc.modified or dc.date) else '',
            # version=dc(
            #     partial(filter_tag_name_namespace, 'modified', ns['dct']), recursive=False)[0].string or dc(
            #         partial(filter_tag_name_namespace, 'date', ns['dc']), recursive=False)[0].string,

            version_PID=first(get_version_pid(dc)) or '',
        )
        if not unified['language']:
            unified['langdis'] = 'True'
        if not unified['project_name']:
            unified['projdis'] = 'True'

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

        return oc.Metadata(result)


def create_metadata_registry():
        '''Return new metadata registry with all common metadata readers

        The readers currently implemented are for metadataPrefixes
        oai_dc, nrd, rdf and xml.

        :returns: metadata registry instance
        :rtype: oaipmh.metadata.MetadataRegistry
        '''
        registry = om.MetadataRegistry()
        registry.registerReader('oai_dc', dc_metadata_reader)
        registry.registerReader('nrd', nrd_metadata_reader)
        registry.registerReader('rdf', rdf_reader)
        registry.registerReader('xml', xml_reader)
        return registry
