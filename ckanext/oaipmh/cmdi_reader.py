from urlparse import urlparse
from ckanext.kata.utils import datapid_to_name
from ckanext.oaipmh.importcore import generic_xml_metadata_reader
import oaipmh.common
from functionally import first


namespaces = {'oai': "http://www.openarchives.org/OAI/2.0/", 'cmd': "http://www.clarin.eu/cmd/"}
provider = "http://metalb.csc.fi/cgi-bin/que" # TODO: Ask about correct value!


def _text_xpath(root, query):
    return [unicode(text).strip() for text in root.xpath(query, namespaces=namespaces)]


def _to_name(identifier):
    parsed = urlparse(identifier)
    if parsed.scheme and parsed.netloc:
        identifier = parsed.path.strip('/')
    return datapid_to_name(identifier)


def cmdi_reader(xml):
    unified = {}
    result = generic_xml_metadata_reader(xml).getMap()

    metadata_identifiers = _text_xpath(xml, '//oai:record/oai:header/oai:identifier/text()')
    cmd = xml.xpath('//oai:record/oai:metadata/cmd:CMD', namespaces=namespaces)[0]
    data_identifiers = _text_xpath(cmd, "//cmd:identificationInfo/cmd:identifier/text()")
    version = first(_text_xpath(cmd, "//cmd:Header/cmd:MdCreationDate/text()")) or ""
    description = first(_text_xpath(cmd, "//cmd:identificationInfo/cmd:description/text()"))

    titles = [{'lang': title.get('{http://www.w3.org/XML/1998/namespace}lang', ''), 'value': title.text.strip()} for title in xml.xpath('//cmd:identificationInfo/cmd:resourceName', namespaces=namespaces)]
    primary_pid = None

    pids = [dict(id=pid, provider=provider, type='data') for pid in data_identifiers]
    for pid in pids:
        if 'urn' in pid.get('id', ""):
            pid['primary'] = "true"
            primary_pid = pid['id']

    pids += [dict(id=pid, provider=provider, type='metadata') for pid in metadata_identifiers]

    result['unified'] = {'name': _to_name(primary_pid or first(data_identifiers)),
                         'pids': pids,
                         'version': version,
                         'tag_string': 'cmdi', # TODO: Ask about value!
                         'notes': description,
                         'langtitle': titles,
                         'type': 'dataset',

    }
    return oaipmh.common.Metadata(result)
