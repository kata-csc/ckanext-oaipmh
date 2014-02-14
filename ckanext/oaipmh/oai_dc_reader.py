# coding=utf-8
import logging
import re
import urllib
from itertools import tee, chain

import bs4
import lxml.etree
import pointfree as pf
from fn.uniform import zip, filter, map, filterfalse
from functionally import first

from oaipmh import common as oc
from ckanext.oaipmh import importcore

xml_reader = importcore.generic_xml_metadata_reader
log = logging.getLogger(__name__)

NS = {
    'dct': 'http://purl.org/dc/terms/',
    'dc': 'http://purl.org/dc/elements/1.1/',
}


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

        # TODO: Return Nones rather than empty strings

        # Populate a BeautifulSoup object
        bs = bs4.BeautifulSoup(lxml.etree.tostring(xml), 'xml')
        dc = bs.metadata.dc

        project_funder, project_funding, project_name, project_homepage = _get_project_stuff(dc) or ('', '', '', '')

        # Todo! This needs to be improved to use also simple-dc
        # dc(filter_tag_name_namespace('publisher', ns['dc']), recursive=False)
        maintainer, maintainer_email, contact_phone, contact_URL = _get_maintainer_stuff(dc) or ('', '', '', '')

        availability, license_id, license_url, access_application_url = _get_rights(dc) or ('', '', '', '')

        # Create a unified internal harvester format dict
        unified = dict(
            # ?=dc('source', recursive=False),
            # ?=dc('relation', recursive=False),
            # ?=dc('type', recursive=False),

            access_application_URL=access_application_url or '',

            # Todo! Implement
            access_request_URL='',

            algorithm=first(_get_algorithm(dc)) or '',

            # TODO: Handle availabilities better
            availability=availability or 'through_provider' if first(_get_download(dc)) else '',

            checksum=_get_checksum(dc) or '',

            # Todo! Using only the first entry, for now
            contact_URL=first(contact_URL) or '',
            contact_phone=first(contact_phone) or '',

            direct_download_URL=first(_get_download(dc)) or '',

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

            language=','.join(sorted([a.string for a in dc('language', recursive=False)])),

            license_URL=license_url or '',
            license_id=license_id or 'notspecified',

            # Todo! Using only the first entry, for now
            maintainer=first(maintainer) or '',
            maintainer_email=first(maintainer_email) or '',

            # Todo! IDA currently doesn't produce this, maybe in future
            # dc('hasFormat', recursive=False)
            mimetype=first([a.string for a in dc('format', text=re.compile('/'), recursive=False)]) or '',

            name=first(map(urllib.quote_plus, _get_data_pids(dc))) or '',
            # name=first(map(pf.partial(urllib.quote_plus, safe=':'), get_data_pids(dc))) or '',

            notes='\r\n\r\n'.join(sorted([a.string for a in dc(
                _filter_tag_name_namespace('description', NS['dc']),
                recursive=False)])) or '',

            orgauth=list(_get_org_auth(dc)),

            # Todo! Using only the first entry, for now
            owner=first([a.get('resource') for a in dc('rightsHolder', recursive=False)]) or '',

            # Todo! Using only the first entry, for now
            project_funder=first(project_funder) or '',
            project_funding=first(project_funding) or '',
            project_homepage=first(project_homepage) or '',
            project_name=first(project_name) or '',

            tag_string=','.join(sorted([a.string for a in dc('subject', recursive=False)])) or '',

            # Todo! Implement if possible
            temporal_coverage_begin='',
            temporal_coverage_end='',

            through_provider_URL=first(_get_download(dc)) or '',

            type='dataset',

            # Todo! This should be more exactly picked
            version=(dc.modified or dc.date).string if (dc.modified or dc.date) else '',
            # version=dc(
            #     partial(filter_tag_name_namespace, 'modified', ns['dct']), recursive=False)[0].string or dc(
            #         partial(filter_tag_name_namespace, 'date', ns['dc']), recursive=False)[0].string,

            version_PID=first(_get_version_pid(dc)) or '',
        )
        if not unified['language']:
            unified['langdis'] = 'True'
        if not unified['project_name']:
            unified['projdis'] = 'True'

        result = xml_reader(xml).getMap()
        result['unified'] = unified

        return oc.Metadata(result)


@pf.partial
def _filter_tag_name_namespace(name, namespace, tag):
    '''
    Boolean filter function, for BeautifulSoup find functions, that checks tag's name and namespace
    '''
    return tag.name == name and tag.namespace == namespace


def _get_version_pid(tag_tree):
    '''
    Generate results for version_PID

    :param tag_tree: Metadata (dc) Tag in BeautifulSoup tree
    :type tag_tree: bs4.Tag
    '''
    # IDA
    for a in tag_tree('description', recursive=False):
        s = re.search('Identifier.version: (.+)', a.string)
        if s and s.group(1):
            yield s.group(1)


def _get_project_stuff(tag_tree):
    '''
    Get project_funder, project_funding, project_name, project_homepage

    :param tag_tree: metadata (dc) element in BeautifulSoup tree
    '''
    def ida():
        for a in tag_tree(_filter_tag_name_namespace(name='contributor', namespace=NS['dct']), recursive=False):
            if a.Project:
                p = a.Project.comment.string.split(u' rahoituspäätös ') if a.Project.comment else ('', '')
                n = a.Project.find('name').string if a.Project.find('name') else ''
                m = a.Project.get('about', '')
                yield tuple(p) + (n,) + (m,)
    return zip(*ida()) if first(ida()) else None


def _get_maintainer_stuff(tag_tree):
    def ida():
        for a in tag_tree(_filter_tag_name_namespace(name='publisher', namespace=NS['dct']), recursive=False):
            for b in a(recursive=False):
                n = b.find('name').string if b.find('name') else ''
                m = b.mbox.get('resource', '') if b.mbox else ''
                p = b.phone.get('resource', '') if b.phone else ''
                h = b.get('about', '')
                yield (n, m, p, h)
    return zip(*ida()) if first(ida()) else None


def _get_data_pids(tag_tree):
    '''
    Returns an iterator over data PIDs from metadata
    '''
    def pids(t):
        '''
        Get data 'PIDs' from OAI-DC and IDA
        '''
        for p in t('identifier', recursive=False):
            yield p.string

    pids1, pids2 = tee(pids(tag_tree), 2)
    pred = lambda x: re.search('urn', x, flags=re.I)
    return chain(filter(pred, pids1), filterfalse(pred, pids2))


def _get_checksum(tag_tree):
    '''
    Get checksum of data file
    '''
    try:
        return tag_tree.hasFormat.File.checksum.Checksum.checksumValue.string
    except Exception as e:
        log.info('Checksum missing from dataset!')


def _get_download(tag_tree):
    # @ExceptReturn(exception=Exception, returns=None)
    def ida():
        try:
            yield tag_tree.hasFormat.File.get('about')
        except Exception as e:
            pass

    # @ExceptReturn(Exception, None)
    def helda():
        for pid in _get_data_pids(tag_tree):
            if pid.startswith('http'):
                yield pid

    return chain(ida(), helda())


def _get_org_auth(tag_tree):
    '''
    Returns an iterator over organization-author dicts from metadata
    '''
    def oai_dc():
        '''
        Get 'author' and 'organization' information from OAI-DC
        '''
        for c in tag_tree(_filter_tag_name_namespace(name='creator', namespace=NS['dc']), recursive=False):
            yield {'org': '', 'value': c.string}
        for c in tag_tree(_filter_tag_name_namespace(name='contributor', namespace=NS['dc']), recursive=False):
            yield {'org': '', 'value': c.string}

    def ida():
        '''
        Get 'author' and 'organization' information from IDA
        '''
        for c in tag_tree(_filter_tag_name_namespace(name='contributor', namespace=NS['dct']), recursive=False):
            # Todo! Simplify this!
            if c.Person and c.Organization:
                yield {'org': c.Organization.find('name').string, 'value': c.Person.find('name').string}
            elif c.Person:
                yield {'org': '', 'value': c.Person.find('name').string}
            elif c.Organization:
                yield {'org': c.Organization.find('name').string, 'value': ''}

    return ida() if first(ida()) else oai_dc()


def _get_algorithm(tag_tree):
    # @ExceptReturn(exception=Exception, passes=True)
    def ida():
        try:
            yield tag_tree.hasFormat.File.checksum.Checksum.generator.Algorithm.get('about').split('/')[-1]
        except Exception as e:
            pass
    return ida()


def _get_rights(tag_tree):
    '''
    Returns a quadruple of rights information (availability, license-id, license-url, access-application-url)
    '''
    def ida():
        '''
        Get rights information from IDA
        '''
        try:
            decl = tag_tree.find(_filter_tag_name_namespace(name='rights', namespace=NS['dct'])).RightsDeclaration.string
            cat = tag_tree.find(_filter_tag_name_namespace(name='rights', namespace=NS['dct'])).RightsDeclaration.get('RIGHTSCATEGORY')
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

    def oai_dc():
        '''
        Get rights information from OAI-DC
        '''
        try:
            return '', '', tag_tree.find(_filter_tag_name_namespace(
                name='rights', namespace=NS['dc'])).string, ''
        except AttributeError as e:
            log.info('OAI_DC rights not detected. Probably just missing. {e}'.format(e=e))
            pass

    return ida() or oai_dc()
