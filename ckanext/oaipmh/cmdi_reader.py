from lxml import etree
from urlparse import urlparse
from ckanext.kata.utils import datapid_to_name
from ckanext.oaipmh.importcore import generic_xml_metadata_reader
import oaipmh.common
from functionally import first


namespaces = {'oai': "http://www.openarchives.org/OAI/2.0/", 'cmd': "http://www.clarin.eu/cmd/"}
provider = "http://metalb.csc.fi/cgi-bin/que" # TODO: Ask about correct value!

class CmdiReaderException(Exception):
    pass


def _text_xpath(root, query):
    return [unicode(text).strip() for text in root.xpath(query, namespaces=namespaces)]


def _to_name(identifier):
    parsed = urlparse(identifier)
    if parsed.scheme and parsed.netloc:
        identifier = parsed.path.strip('/')
    return datapid_to_name(identifier)


def _strip_first(elements):
    """ Strip and return first element.
    :param elements: list of xml elements
    :return: first element or none
    """
    return (first(elements) or "").strip()


def _get_organizations(root, xpath):
    return [{'role':  _strip_first(organization.xpath("cmd:role/text()", namespaces=namespaces)),
             'name': _strip_first(organization.xpath("cmd:organizationInfo/cmd:organizationName/text()", namespaces=namespaces)),
             'short_name': _strip_first(organization.xpath("cmd:organizationInfo/cmd:organizationShortName/text()", namespaces=namespaces)),
             'email': _strip_first(organization.xpath("cmd:organizationInfo/cmd:communicationInfo/cmd:email/text()", namespaces=namespaces)),
             'url': _strip_first(organization.xpath("cmd:organizationInfo/cmd:communicationInfo/cmd:email/text()", namespaces=namespaces))}

            for organization in root.xpath(xpath, namespaces=namespaces)]


def _get_persons(root, xpath):
    return [{'role':  _strip_first(person.xpath("cmd:role/text()", namespaces=namespaces)),
             'surname': _strip_first(person.xpath("cmd:personInfo/cmd:surname/text()",  namespaces=namespaces)),
             'given_name': _strip_first(person.xpath("cmd:personInfo/cmd:givenName/text()", namespaces=namespaces)),
             'email': _strip_first(person.xpath("cmd:personInfo/cmd:communicationInfo/cmd:email/text()", namespaces=namespaces)),
             'organization': first(_get_organizations(person, "cmd:personInfo/cmd:affiliation"))}
            for person in root.xpath(xpath, namespaces=namespaces)]


def _get_person_name(person):
    return u"%s %s" % (person['given_name'], person['surname'])


def _persons_as_contact(persons):
    return [{'name': _get_person_name(person),
             'url': (person.get('organization', None) or {}).get('url', ""),
             'email': person['email'],
             'phone': ""}
            for person in persons]


def _persons_as_agent(persons, agent_role):
    return [{'name': _get_person_name(person),
             'role': agent_role}
            for person in persons]


def cmdi_reader(xml):
    unified = {}
    result = generic_xml_metadata_reader(xml).getMap()

    metadata_identifiers = _text_xpath(xml, '//oai:record/oai:header/oai:identifier/text()')
    cmd = first(xml.xpath('//oai:record/oai:metadata/cmd:CMD', namespaces=namespaces))
    if cmd is None:
        raise CmdiReaderException("Unexpected XML format: No CMD -element found")

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

    resource_info = cmd.xpath("//cmd:Components/cmd:resourceInfo", namespaces=namespaces)[0]
    if resource_info is None:
        raise CmdiReaderException("Unexpected XML format: No resourceInfo -element found")

    # TODO: Check agent mapping.
    #print "###", \
    _get_persons(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:licensorPerson")
    #print "###", \
    _get_persons(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:distributionRightsHolderPerson")
    #print "###", \
    _get_persons(resource_info, "//cmd:distributionInfo/cmd:iprHolderPerson")
    #print "###", \
    _get_persons(resource_info, "//cmd:contactPerson")
    #print "###", \
    _get_persons(resource_info, "//cmd:metadataInfo/cmd:metadataCreator")

    #print "###", \
    _get_organizations(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:licensorOrganization")
    #print "###", \
    _get_organizations(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:distributionRightsHolderOrganization")
    #print "###", \
    _get_organizations(resource_info, "//cmd:distributionInfo/cmd:iprHolderOrganization")

    contacts = _persons_as_contact(_get_persons(resource_info, "//cmd:contactPerson"))

    direct_download_url = _strip_first(_text_xpath(cmd, "//cmd:identificationInfo/cmd:url/text()"))

    availability = _strip_first(_text_xpath(resource_info, "//cmd:distributionInfo/availability/text()")) or 'through_provider' if direct_download_url else ""

    agents = []
    agents.extend(_persons_as_agent( _get_persons(resource_info, "//cmd:distributionInfo/cmd:iprHolderPerson"), 'owner'))
#    agents.extend(_persons_as_agent( _get_persons(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:licensorPerson"), 'owner'))

    result['unified'] = {'name': _to_name(primary_pid or first(data_identifiers)),
                         'pids': pids,
                         'version': version,
                         'tag_string': 'cmdi', # TODO: Ask about value!
                         'notes': description,
                         'langtitle': titles,
                         'type': 'dataset',
                         'contact': contacts,
                         'agent': agents,
                         'availability': availability,
                         'direct_download_URL': direct_download_url,
    }
    return oaipmh.common.Metadata(result)
