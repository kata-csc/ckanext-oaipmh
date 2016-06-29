from lxml import etree
from urlparse import urlparse
from ckanext.kata.utils import datapid_to_name
from ckanext.kata.utils import generate_pid
from ckanext.oaipmh.importcore import generic_xml_metadata_reader
import oaipmh.common
from functionally import first
from pylons import config
import json
import utils


class CmdiReaderException(Exception):
    """ Reader exception is thrown on unexpected data or error. """
    pass


class CmdiReader(object):
    """ Reader for CMDI XML data """

    namespaces = {'oai': "http://www.openarchives.org/OAI/2.0/", 'cmd': "http://www.clarin.eu/cmd/"}

    def __init__(self, provider=None):
        """ Generate new reader instance.
        :param provider: URL used for pids.
        """
        super(CmdiReader, self).__init__()
        self.provider = provider or config.get('ckan.site_url')

    def __call__(self, xml):
        """ Call :meth:`CmdiReader.read`. """
        return self.read(xml)

    @classmethod
    def _text_xpath(cls, root, query):
        """ Select list of texts and strip results. Use text() suffix in Xpath `query`.

        :param root: parent element (lxml) where selection is made.
        :param query: Xpath query used to get data
        :return: list of strings
        """
        return [unicode(text).strip() for text in root.xpath(query, namespaces=cls.namespaces)]

    @staticmethod
    def _to_identifier(identifier):
        """ Convert url identifier to identifier.

        :param identifier: identifier string
        :return: CKAN package name
        """
        parsed = urlparse(identifier)
        if parsed.scheme and parsed.netloc:
            identifier = parsed.path.strip('/')
        return identifier

    @classmethod
    def _to_name(cls, identifier):
        """ Convert identifier to CKAN package name.

        :param identifier: identifier string
        :return: CKAN package name
        """
        return datapid_to_name(cls._to_identifier(identifier))

    @staticmethod
    def _strip_first(elements):
        """ Strip and return first element.

        :param elements: list of xml elements
        :return: first element or none
        """
        return (first(elements) or "").strip()

    @classmethod
    def _get_organizations(cls, root, xpath):
        """ Extract organization dictionaries from XML using given Xpath.

        :param root: parent element (lxml) where selection is done.
        :param xpath: xpath selector used to get data
        :return: list of organization dictionaries
        """
        return [{'role': cls._strip_first(organization.xpath("cmd:role/text()", namespaces=cls.namespaces)),
                 'name': ", ".join(cls._text_xpath(organization, "cmd:organizationInfo/cmd:organizationName/text()")),
                 'short_name': cls._strip_first(organization.xpath("cmd:organizationInfo/cmd:organizationShortName/text()", namespaces=cls.namespaces)),
                 'email': cls._strip_first(organization.xpath("cmd:organizationInfo/cmd:communicationInfo/cmd:email/text()", namespaces=cls.namespaces)),
                 'url': cls._strip_first(organization.xpath("cmd:organizationInfo/cmd:communicationInfo/cmd:email/text()", namespaces=cls.namespaces))}

                for organization in root.xpath(xpath, namespaces=cls.namespaces)]

    @classmethod
    def _get_persons(cls, root, xpath):
        """ Extract person dictionary from XML using given Xpath.

        :param root: parent element (lxml) where selection is done
        :param xpath: xpath selector used to get data
        :return: list of person dictionaries
        """
        return [{'role': cls._strip_first(person.xpath("cmd:role/text()", namespaces=cls.namespaces)),
                 'surname': cls._strip_first(person.xpath("cmd:personInfo/cmd:surname/text()", namespaces=cls.namespaces)),
                 'given_name': cls._strip_first(person.xpath("cmd:personInfo/cmd:givenName/text()", namespaces=cls.namespaces)),
                 'email': cls._strip_first(person.xpath("cmd:personInfo/cmd:communicationInfo/cmd:email/text()", namespaces=cls.namespaces)),
                 'organization': first(cls._get_organizations(person, "cmd:personInfo/cmd:affiliation"))}
                for person in root.xpath(xpath, namespaces=cls.namespaces)]

    @staticmethod
    def _get_person_name(person):
        """ Generate name from person dictionary.

        :param person: person dictionary
        :return: name of the person
        """
        return u"%s %s" % (person['given_name'], person['surname'])

    @classmethod
    def _persons_as_contact(cls, persons):
        """ Convert person dictionaries to contact dictionaries.

        :param persons: list of person dictionaries
        :return: list of contact dictionaries
        """
        return [{'name': cls._get_person_name(person),
                 'url': (person.get('organization', None) or {}).get('url', ""),
                 'email': person['email'],
                 'phone': ""}
                for person in persons]

    @staticmethod
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

    @classmethod
    def _persons_as_agent(cls, persons, agent_role):
        """ Convert person dictionaries to agent dictionaries.

        :param persons: list of person dictionaries
        :param agent_role: name of the role
        :return: list of agent dictionaries
        """
        return [{'name': cls._get_person_name(person),
                 'organisation': (person.get('organization', None) or {}).get('name', ""),
                 'role': agent_role}
                for person in persons]

    def read(self, xml):
        """ Extract package data from given XML.
        :param xml: xml element (lxml)
        :return: oaipmh.common.Metadata object generated from xml
        """
        result = generic_xml_metadata_reader(xml).getMap()
        result['unified'] = self.read_data(xml)
        return oaipmh.common.Metadata(xml, result)

    def read_data(self, xml):
        """ Extract package data from given XML.
        :param xml: xml element (lxml)
        :return: dictionary
        """
        cmd = first(xml.xpath('//oai:record/oai:metadata/cmd:CMD', namespaces=self.namespaces))
        if cmd is None:
            raise CmdiReaderException("Unexpected XML format: No CMD -element found")

        resource_info = cmd.xpath("//cmd:Components/cmd:resourceInfo", namespaces=self.namespaces)[0]
        if resource_info is None:
            raise CmdiReaderException("Unexpected XML format: No resourceInfo -element found")

        metadata_identifiers = self._text_xpath(cmd, "//cmd:identificationInfo/cmd:identifier/text()")
        data_identifiers = self._text_xpath(cmd, "//cmd:identificationInfo/cmd:url/text()")

        languages = self._text_xpath(cmd, "//cmd:corpusInfo/cmd:corpusMediaType/cmd:corpusTextInfo/cmd:languageInfo/cmd:languageId/text()")

        # convert the descriptions to a JSON string of type {"fin":"kuvaus", "eng","desc"}
        desc_json = {}
        for desc in xml.xpath("//cmd:identificationInfo/cmd:description", namespaces=self.namespaces):
            lang = utils.convert_language(desc.get('{http://www.w3.org/XML/1998/namespace}lang', 'undefined').strip())
            desc_json[lang] = unicode(desc.text).strip()

        description = json.dumps(desc_json)

        # convert the titles to a JSON string of type {"fin":"otsikko", "eng","title"}
        transl_json = {}
        for title in xml.xpath('//cmd:identificationInfo/cmd:resourceName', namespaces=self.namespaces):
            lang = utils.convert_language(title.get('{http://www.w3.org/XML/1998/namespace}lang', 'undefined').strip())
            transl_json[lang] = title.text.strip()

        title = json.dumps(transl_json)

        version = first(self._text_xpath(resource_info, "//cmd:metadataInfo/cmd:metadataLastDateUpdated/text()")) or ""
        coverage = first(self._text_xpath(resource_info, "//cmd:corpusInfo/cmd:corpusMediaType/cmd:corpusTextInfo/cmd:timeCoverageInfo/cmd:timeCoverage/text()")) or ""
        license_identifier = first(self._text_xpath(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:licence/text()")) or 'notspecified'

        provider = self.provider

        pids = []
        for pid in [dict(id=pid, provider=provider, type='metadata') for pid in metadata_identifiers]:
            if 'urn' in pid.get('id', ""):
                pids.append(dict(id=pid['id'], provider=provider, type='metadata', primary=True))
            else:
                pids.append(pid)

        pids += [dict(id=pid, provider=provider, type='data', primary=data_identifiers.index(pid) == 0) for pid in data_identifiers]

        temporal_coverage_begin = ""
        temporal_coverage_end = ""

        if coverage:
            split = [item.strip() for item in coverage.split("-")]
            if len(split) == 2:
                temporal_coverage_begin = split[0]
                temporal_coverage_end = split[1]

        # TODO: Check agent mapping.
        #print "###", _get_persons(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:licensorPerson")
        #print "###", _get_persons(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:distributionRightsHolderPerson")
        #print "###", _get_persons(resource_info, "//cmd:distributionInfo/cmd:iprHolderPerson")
        #print "###", _get_persons(resource_info, "//cmd:contactPerson")
        #print "###", _get_persons(resource_info, "//cmd:metadataInfo/cmd:metadataCreator")

        #print "###", _get_organizations(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:licensorOrganization")
        #print "###", _get_organizations(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:distributionRightsHolderOrganization")
        #print "###", _get_organizations(resource_info, "//cmd:distributionInfo/cmd:iprHolderOrganization")

        contacts = self._persons_as_contact(self._get_persons(resource_info, "//cmd:contactPerson"))

        agents = []
        agents.extend(self._persons_as_agent(self._get_persons(resource_info, "//cmd:distributionInfo/cmd:iprHolderPerson"), 'author'))
        agents.extend(self._persons_as_agent(self._get_persons(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:distributionRightsHolderPerson"), 'owner'))

        agents.extend(self._organization_as_agent(self._get_organizations(resource_info, "//cmd:distributionInfo/cmd:iprHolderOrganization"), 'author'))
        agents.extend(self._organization_as_agent(self._get_organizations(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:distributionRightsHolderOrganization"), 'owner'))

        package_id = generate_pid()

        result = {'name': datapid_to_name(package_id),
                  'language': ",".join(languages),
                  'pids': pids,
                  'version': version,
                  'notes': description,
                  #'langtitle': titles,
                  'title': title,
                  'type': 'dataset',
                  'contact': contacts,
                  'agent': agents,
                  'availability': 'contact_owner',
                  'temporal_coverage_begin': temporal_coverage_begin,
                  'temporal_coverage_end': temporal_coverage_end,
                  'license_id': license_identifier}

        if not languages:
            result['langdis'] = u'True'

        if package_id:
            result['id'] = package_id

        # TODO: Ask about distributionAccessMedium
        # _strip_first(_text_xpath(resource_info, "//cmd:distributionInfo/availability/text()"))
        # url = _strip_first(_text_xpath(resource_info, "//cmd:identificationInfo/cmd:url/text()"))
        download_location = first(self._text_xpath(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:downloadLocation/text()"))

        if download_location:
            result['through_provider_URL'] = download_location
            result['availability'] = 'through_provider'

        return result
