from lxml import etree
from urlparse import urlparse
from ckanext.kata.utils import datapid_to_name
from ckanext.oaipmh.importcore import generic_xml_metadata_reader
import oaipmh.common
from functionally import first


namespaces = {'oai': "http://www.openarchives.org/OAI/2.0/", 'cmd': "http://www.clarin.eu/cmd/"}
provider = "http://metalb.csc.fi/cgi-bin/que"  # TODO: Ask about correct value!


class CmdiReaderException(Exception):
    """ Reader exception is thrown on unexpected data or error. """
    pass


def _text_xpath(root, query):
    """ Select list of texts.

    :param root:
    :param query:
    :return:
    """
    return [unicode(text).strip() for text in root.xpath(query, namespaces=namespaces)]


def _to_name(identifier):
    """ Convert identifier to CKAN package name.

    :param identifier: identifier string
    :return: CKAN package name
    """
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
    """ Extract organization dictionaries from XML using given Xpath.

    :param root: parent element (lxml) where selection is done.
    :param xpath: xpath selector used to get data
    :return: list of organization dictionaries
    """
    return [{'role': _strip_first(organization.xpath("cmd:role/text()", namespaces=namespaces)),
             'name': _strip_first(organization.xpath("cmd:organizationInfo/cmd:organizationName/text()", namespaces=namespaces)),
             'short_name': _strip_first(organization.xpath("cmd:organizationInfo/cmd:organizationShortName/text()", namespaces=namespaces)),
             'email': _strip_first(organization.xpath("cmd:organizationInfo/cmd:communicationInfo/cmd:email/text()", namespaces=namespaces)),
             'url': _strip_first(organization.xpath("cmd:organizationInfo/cmd:communicationInfo/cmd:email/text()", namespaces=namespaces))}

            for organization in root.xpath(xpath, namespaces=namespaces)]


def _get_persons(root, xpath):
    """ Extract person dictionary from XML using given Xpath.

    :param root: parent element (lxml) where selection is done
    :param xpath: xpath selector used to get data
    :return: list of person dictionaries
    """
    return [{'role': _strip_first(person.xpath("cmd:role/text()", namespaces=namespaces)),
             'surname': _strip_first(person.xpath("cmd:personInfo/cmd:surname/text()", namespaces=namespaces)),
             'given_name': _strip_first(person.xpath("cmd:personInfo/cmd:givenName/text()", namespaces=namespaces)),
             'email': _strip_first(person.xpath("cmd:personInfo/cmd:communicationInfo/cmd:email/text()", namespaces=namespaces)),
             'organization': first(_get_organizations(person, "cmd:personInfo/cmd:affiliation"))}
            for person in root.xpath(xpath, namespaces=namespaces)]


def _get_person_name(person):
    """ Generate name from person dictionary.

    :param person: person dictionary
    :return: name of the person
    """
    return u"%s %s" % (person['given_name'], person['surname'])


def _persons_as_contact(persons):
    """ Convert person dictionaries to contact dictionaries.

    :param persons: list of person dictionaries
    :return: list of contact dictionaries
    """
    return [{'name': _get_person_name(person),
             'url': (person.get('organization', None) or {}).get('url', ""),
             'email': person['email'],
             'phone': ""}
            for person in persons]


def _organization_as_agent(organizations, agent_role):
    """ Convert organization dictionaries to agent dictionaries.

    :param organizations: list of organization dictionaries
    :param agent_role: name of the role
    :return: list of agent dictionaries
    """
    return [{'name': "",
             'organisation': organization.get('name', ""),
             'role': agent_role}
            for organization in organizations]



def _persons_as_agent(persons, agent_role):
    """ Convert person dictionaries to agent dictionaries.

    :param persons: list of person dictionaries
    :param agent_role: name of the role
    :return: list of agent dictionaries
    """
    return [{'name': _get_person_name(person),
             'organisation': (person.get('organization', None) or {}).get('name', ""),
             'role': agent_role}
            for person in persons]


def cmdi_reader(xml):
    """ Extract package data from given XML.
    :param xml: xml element (lxml)
    :return: oaipmh.common.Metadata object generated from xml
    """

    result = generic_xml_metadata_reader(xml).getMap()

    metadata_identifiers = _text_xpath(xml, '//oai:record/oai:header/oai:identifier/text()')
    cmd = first(xml.xpath('//oai:record/oai:metadata/cmd:CMD', namespaces=namespaces))
    if cmd is None:
        raise CmdiReaderException("Unexpected XML format: No CMD -element found")

    resource_info = cmd.xpath("//cmd:Components/cmd:resourceInfo", namespaces=namespaces)[0]
    if resource_info is None:
        raise CmdiReaderException("Unexpected XML format: No resourceInfo -element found")

    languages = _text_xpath(cmd, "//cmd:corpusInfo/cmd:corpusMediaType/cmd:corpusTextInfo/cmd:languageInfo/cmd:languageId/text()")

    data_identifiers = _text_xpath(cmd, "//cmd:identificationInfo/cmd:identifier/text()")
    description = first(_text_xpath(cmd, "//cmd:identificationInfo/cmd:description/text()"))

    titles = [{'lang': title.get('{http://www.w3.org/XML/1998/namespace}lang', ''), 'value': title.text.strip()} for title in xml.xpath('//cmd:identificationInfo/cmd:resourceName', namespaces=namespaces)]
    primary_pid = None

    pids = [dict(id=pid, provider=provider, type='data') for pid in data_identifiers]
    for pid in pids:
        if 'urn' in pid.get('id', ""):
            pid['primary'] = "true"
            primary_pid = pid['id']

    pids += [dict(id=pid, provider=provider, type='metadata') for pid in metadata_identifiers]

    version = first(_text_xpath(resource_info, "//cmd:metadataInfo/cmd:metadataLastDateUpdated/text()")) or ""

    # TODO: Check agent mapping.
    #print "###", _get_persons(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:licensorPerson")
    #print "###", _get_persons(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:distributionRightsHolderPerson")
    #print "###", _get_persons(resource_info, "//cmd:distributionInfo/cmd:iprHolderPerson")
    #print "###", _get_persons(resource_info, "//cmd:contactPerson")
    #print "###", _get_persons(resource_info, "//cmd:metadataInfo/cmd:metadataCreator")

    #print "###", _get_organizations(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:licensorOrganization")
    #print "###", _get_organizations(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:distributionRightsHolderOrganization")
    #print "###", _get_organizations(resource_info, "//cmd:distributionInfo/cmd:iprHolderOrganization")

    contacts = _persons_as_contact(_get_persons(resource_info, "//cmd:contactPerson"))

    agents = []
    agents.extend(_persons_as_agent( _get_persons(resource_info, "//cmd:distributionInfo/cmd:iprHolderPerson"), 'author'))
    agents.extend(_persons_as_agent(_get_persons(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:distributionRightsHolderPerson"), 'owner'))

    agents.extend(_organization_as_agent(_get_organizations(resource_info, "//cmd:distributionInfo/cmd:iprHolderOrganization"), 'author'))
    agents.extend(_organization_as_agent(_get_organizations(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:distributionRightsHolderOrganization"), 'owner'))

    result['unified'] = {'name': _to_name(primary_pid or first(data_identifiers)),
                         'language': ",".join(languages),
                         'pids': pids,
                         'version': version,
                         'tag_string': 'cmdi', # TODO: Ask about value!
                         'notes': description,
                         'langtitle': titles,
                         'type': 'dataset',
                         'contact': contacts,
                         'agent': agents,
                         'availability': 'contact_owner'}

    # TODO: Ask about distributionAccessMedium
    # _strip_first(_text_xpath(resource_info, "//cmd:distributionInfo/availability/text()"))
    # url = _strip_first(_text_xpath(resource_info, "//cmd:identificationInfo/cmd:url/text()"))
    download_location = first(_text_xpath(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:downloadLocation/text()"))

    if download_location:
        result['unified']['through_provider_URL'] = download_location
        result['unified']['availability'] = 'through_provider'

    return oaipmh.common.Metadata(result)
