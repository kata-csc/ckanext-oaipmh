'''RDF reader and writer for OAI-PMH harvester and server interface
'''
from iso639 import languages
from lxml import etree
from lxml.etree import SubElement
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


NS_XSI = 'http://www.w3.org/2001/XMLSchema-instance'
NS_OAIDC = 'http://www.openarchives.org/OAI/2.0/oai_dc/'
NS_DC = "http://purl.org/dc/elements/1.1/"
NS_DATACITE = 'http://datacite.org/schema/kernel-3'

event_to_dt = {'collection': 'Collected',
               'creation': 'Created',
               'extended': 'Updated',
               'changed': 'Updated',
               'published': 'Issued',
               'sent': 'Submitted',
               'received': 'Accepted',
               'modified': 'Updated'}

def convert_language(lang):
    '''
    Convert alpha2 language (eg. 'en') to terminology language (eg. 'eng')
    '''
    try:
        lang_object = languages.get(part2t=lang)
        return lang_object.part1
    except KeyError as ke:
        # TODO: Parse ISO 639-2 B/T ?
        # log.debug('Invalid language: {ke}'.format(ke=ke))
        return ''


def append_agent(e_agent_parent, role, key, value, roletype=None):
    for agent in value:
        e_agent = SubElement(e_agent_parent, nsdatacite(role))
        if roletype:
            e_agent.set(role + 'Type', roletype)
        agentname = role + 'Name'
        e_agent_name = SubElement(e_agent, nsdatacite(agentname))
        e_agent_name.text = agent['name']
        org = agent.get('organisation')
        if org:
            e_affiliation = SubElement(e_agent, nsdatacite('affiliation'))
            e_affiliation.text = org


def datacite_writer(element, metadata):
    '''Transform oaipmh.common.Metadata metadata dictionaries to lxml.etree.Element XML documents.
    '''
    e_dc = SubElement(element, nsdatacite('datacite'),
                      # nsmap={'datacite': NS_DATACITE, 'xsi': NS_XSI})
                      nsmap = {'oai_dc': NS_OAIDC, 'dc': NS_DC, 'datacite': NS_DATACITE, 'xsi': NS_XSI})
    e_dc.set('{%s}schemaLocation' % NS_XSI,
             '%s http://www.openarchives.org/OAI/2.0/oai_dc.xsd' % NS_DC)
    map = metadata.getMap()
    for k, v in map.iteritems():
        if v:
            if '/@' in k:
                continue
            if k == 'creators':
                e_agent_parent = SubElement(e_dc, nsdatacite('creators'))
                append_agent(e_agent_parent, 'creator', k, v)
                continue
            if k == 'titles':
                primary_lang = 'eng'
                e_titles = SubElement(e_dc, nsdatacite(k))
                e_title_primary = SubElement(e_titles, nsdatacite('title'))
                title_langs = v[0].keys()
                if primary_lang in title_langs:
                    lang = convert_language(primary_lang)
                    e_title_primary.set('lang', lang)
                    e_title_primary.text = v[0][primary_lang]
                    for l in title_langs:
                        if l != primary_lang:
                            e_title_translated = SubElement(e_titles, nsdatacite('title'))
                            e_title_translated.set('lang', convert_language(l))
                            e_title_translated.set('titleType', 'TranslatedTitle')
                            e_title_translated.text = v[0][l]
                else:
                    e_title_primary.set('lang', convert_language(title_langs[0]))
                    e_title_primary.text = v[0][title_langs[0]]
                    for l in title_langs[1:]:
                        e_title_translated = SubElement(e_titles, nsdatacite('title'))
                        e_title_translated.set('lang', convert_language(l))
                        e_title_translated.set('titleType', 'TranslatedTitle')
                        e_title_translated.text = v[0][l]
                continue
            if k == 'subjects':
                e_subjects = SubElement(e_dc, nsdatacite(k))
                for subject in v:
                    e_subject = SubElement(e_subjects, nsdatacite('subject'))
                    e_subject.text = subject
                continue
            if k == 'contributors':
                e_agent_parent = e_dc.find(".//{*}" + 'contributors')
                if not e_agent_parent:
                    e_agent_parent = SubElement(e_dc, nsdatacite('contributors'))
                append_agent(e_agent_parent, 'contributor', k, v, 'Other')
                continue
            if k == 'funders':
                if v[0].get('organisation') or v[0].get('name'):
                    e_agent_parent = e_dc.find(".//{*}" + 'contributors')
                    if not e_agent_parent:
                        e_agent_parent = SubElement(e_dc, nsdatacite('contributors'))
                    for agent in v:
                        e_agent = SubElement(e_agent_parent, nsdatacite('contributor'))
                        e_agent.set('contributorType', 'Funder')
                        e_agent_name = SubElement(e_agent, nsdatacite('contributorName'))
                        e_agent_name.text = agent.get('organisation') or agent.get('name')
                continue
            if k == 'dates':
                e_dates = SubElement(e_dc, nsdatacite(k))
                for event in v:
                    e_date = SubElement(e_dates, nsdatacite('date'))
                    e_date.text = event['when']
                    e_date.set('dateType', event_to_dt[event['type']])
                continue
            e = SubElement(e_dc, nsdatacite(k))
            e.text = v[0] if isinstance(v, list) else v
    for k, v in map.iteritems():
        if '/@' in k:
            element, attr = k.split('/@')
            print(e_dc.tag)
            e = e_dc.find(".//{*}" + element, )
            if e is not None:
                e.set(attr, v[0] if isinstance(v, list) else v)

def nsoaidc(name):
    return '{%s}%s' % (NS_OAIDC, name)
def nsdc(name):
    return '{%s}%s' % (NS_DC, name)
def nsdatacite(name):
    return '{%s}%s' % (NS_DATACITE, name)

def nsrdf(name):
    return '{%s}%s' % (NSRDF, name)


def nsow(name):
    return '{%s}%s' % (NSOW, name)
