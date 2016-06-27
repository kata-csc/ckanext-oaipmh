import logging
from lxml import etree
from urlparse import urlparse
from ckanext.kata.utils import datapid_to_name
from ckanext.oaipmh.importcore import generic_xml_metadata_reader
import oaipmh.common
from functionally import first
from pylons import config
import json
import utils

log = logging.getLogger(__name__)

class RdfReaderException(Exception):
    """ Reader exception is thrown on unexpected data or error. """
    pass


class RdfReader(object):
    """ Reader for RDF XML data """

    namespaces = {'adms': "http://www.w3.org/ns/adms#",
                  'dcat': "http://www.w3.org/ns/dcat#",
                  'dct': "http://purl.org/dc/terms/",
                  'foaf': "http://xmlns.com/foaf/0.1/",
                  'frapo': "http://purl.org/cerif/frapo/",
                  'org': "http://www.w3.org/ns/org#",
                  'rdf': "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                  'rdfs': "http://www.w3.org/2000/01/rdf-schema#",
                  'spdx': "http://spdx.org/rdf/terms#",
                  'time': "http://www.w3.org/2006/time#",
                  'xsd': "http://www.w3.org/2001/XMLSchema#",
                  'cmd': "http://www.clarin.eu/cmd/"    ## TODO: JPL: Remove this
                  }

    def __init__(self, provider=None):
        """ Generate new reader instance.
        :param provider: URL used for pids.
        """
        super(RdfReader, self).__init__()
        self.provider = provider or config.get('ckan.site_url')

    def __call__(self, xml):
        """ Call :meth:`RdfReader.read`. """
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
        return [{'role': 'affilation',
                 'name': cls._strip_first(organization.xpath("foaf:organization/foaf:name/text()", namespaces=cls.namespaces)),
                 # 'short_name': cls._strip_first(organization.xpath("cmd:organizationInfo/cmd:organizationShortName/text()", namespaces=cls.namespaces)),
                 # 'email': cls._strip_first(organization.xpath("cmd:organizationInfo/cmd:communicationInfo/cmd:email/text()", namespaces=cls.namespaces)),
                 # 'url': cls._strip_first(organization.xpath("cmd:organizationInfo/cmd:communicationInfo/cmd:email/text()", namespaces=cls.namespaces))
                }
                for organization in root.xpath(xpath, namespaces=cls.namespaces)]

    @classmethod
    def _get_persons(cls, root, xpath):
        """ Extract person dictionary from XML using given Xpath.

        :param root: parent element (lxml.etree) where selection is done
        :param xpath: xpath selector used to get data
        :return: list of person dictionaries
        """
        persons = []
        for person in root.xpath(xpath, namespaces=cls.namespaces):
            names = cls._strip_first(person.xpath("foaf:Agent/foaf:name/text()", namespaces=cls.namespaces)).split(',')
            identifier = cls._strip_first(person.xpath("foaf:Agent/foaf:account/text()", namespaces=cls.namespaces))
            # TODO JPL: Why there is "mailto:" prefix in email in read.rdf?
            email_el = person.find(".//foaf:mbox[@rdf:resource]", namespaces=cls.namespaces)
            email = email_el.attrib.values()[0].split(':') if email_el is not None else ''
            phone_el = person.find(".//foaf:phone[@rdf:resource]", namespaces=cls.namespaces)
            phone = phone_el.attrib.values()[0].split(':') if phone_el is not None else ''
            homepage_el = person.find(".//foaf:homepage[@rdf:resource]", namespaces=cls.namespaces)
            url = homepage_el.attrib.values()[0] if homepage_el is not None else ''
            persons.extend(
                [{'role': 'creator',
                  'surname': names[0] if names else '',
                  'given_name': names[1] if len(names) > 1 else '',
                  'id': identifier if identifier else '',
                  'email': email[1] if len(email) > 1 else '',
                  'phone': phone[1] if len(phone) > 1 else '',
                  'organization': first(cls._get_organizations(person, "foaf:Agent/org:memberOf")),
                  'URL': url if url else ''}])
        log.debug("JPL DEBUG: _get_persons(): {msg}".format(msg=persons))
        return persons

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
                 'URL': person['URL'],
                 'email': person['email'],
                 'phone': person['phone']}
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
                 'id': person['id'],
                 'organisation': (person.get('organization', None) or {}).get('name', ""),
                 'role': agent_role}
                for person in persons]

    @classmethod
    def _funders_as_agent(cls, funders, agent_role):
        """ Convert funder dictionaries to agent dictionaries.

        :param funders: list of funder dictionaries
        :param agent_role: name of the role
        :return: list of agent dictionaries
        """
        return [{'name': funder['name'],
                 'URL': funder['homepage'],
                 'organisation': (funder.get('organization', None) or {}).get('name', ""),
                 'fundingid': funder['fundingid'],
                 'role': agent_role}
                for funder in funders]

    @classmethod
    def _get_project(cls, root, xpath):
        """ Extract project dictionary from XML using given Xpath.

        :param root: parent element (lxml.etree) where selection is done
        :param xpath: xpath selector used to get data
        :return:

        >>> _get_project(rdf, "//dcat:Dataset/frapo:isOutputOf")
        [{u'URL': u'http://lehtilehti.org', u'fundingid': u'12345', u'name': u'\
Testproject', u'organisation': u'Tekes', u'role': u'funder'}, {u'URL': u'', u'f\
undingid': u'12345', u'name': u'', u'organisation': u'THL', u'role': u'funder'}]
        """
        funders = []
        for project in root.xpath(xpath, namespaces=cls.namespaces):
            homepage_el = project.find(".//foaf:homepage[@rdf:resource]", namespaces=cls.namespaces)
            url = homepage_el.attrib.values()[0] if homepage_el is not None else ''
            funders.extend([{'name': cls._strip_first(project.xpath("foaf:Project/foaf:name/text()", namespaces=cls.namespaces)),
                            'homepage': url if url else '',
                            'fundingid': cls._strip_first(project.xpath("foaf:Project/rdfs:comment/text()", namespaces=cls.namespaces)),
                            'organization': first(cls._get_organizations(project, "foaf:Project/org:memberOf"))}])
        log.debug("JPL DEBUG: _get_projects(): {msg}".format(msg=funders))
        return funders

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
        :param xml: xml element (lxml.etree)
        :return: dictionary
        """
        rdf = first(xml.xpath('//rdf:RDF', namespaces=self.namespaces))
        if rdf is None:
            raise RdfReaderException("Unexpected XML format: No RDF -element found")

        catalog_record = rdf.xpath("//dcat:CatalogRecord", namespaces=self.namespaces)[0]
        if catalog_record is None:
            raise RdfReaderException("Unexpected XML format: No CatalogRecord element found")

        metadata_identifiers = self._text_xpath(catalog_record, "//dct:identifier/text()")
        # TODO JPL: Not working if multiple data_identifiers
        data_identifiers = self._text_xpath(rdf, "//dcat:Dataset/adms:identifier/text()")

        languages = self._text_xpath(rdf, "//dcat:Dataset/dct:language/text()")

        # convert the descriptions to a JSON string of type {"fin":"kuvaus", "eng","desc"}
        desc_json = {}
        for desc in xml.xpath("//dcat:Dataset/dct:description", namespaces=self.namespaces):
            lang = utils.convert_language(desc.get('{http://www.w3.org/XML/1998/namespace}lang', 'undefined').strip())
            desc_json[lang] = unicode(desc.text).strip()

        description = json.dumps(desc_json)

        # convert the titles to a JSON string of type {"fin":"otsikko", "eng","title"}
        transl_json = {}
        for title in xml.xpath('//dcat:Dataset/dct:title', namespaces=self.namespaces):
            lang = utils.convert_language(title.get('{http://www.w3.org/XML/1998/namespace}lang', 'undefined').strip())
            transl_json[lang] = title.text.strip()

        title = json.dumps(transl_json)

        # version = first(self._text_xpath(resource_info, "//cmd:metadataInfo/cmd:metadataLastDateUpdated/text()")) or ""
        # coverage = first(self._text_xpath(resource_info, "//cmd:corpusInfo/cmd:corpusMediaType/cmd:corpusTextInfo/cmd:timeCoverageInfo/cmd:timeCoverage/text()")) or ""
        # license_identifier = first(self._text_xpath(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:licence/text()")) or 'notspecified'

        primary_pid = None
        provider = self.provider

        pids = []
        for pid in [dict(id=pid, provider=provider, type='metadata') for pid in metadata_identifiers]:
            if 'urn' in pid.get('id', ""):
                primary_pid = pid['id']
            else:
                pids.append(pid)

        pids += [dict(id=pid, provider=provider, type='data', primary=data_identifiers.index(pid) == 0) for pid in data_identifiers]

        temporal_coverage_begin = ""
        temporal_coverage_end = ""

        # if coverage:
        #     split = [item.strip() for item in coverage.split("-")]
        #     if len(split) == 2:
        #         temporal_coverage_begin = split[0]
        #         temporal_coverage_end = split[1]

        # TODO: Check agent mapping.
        #print "###", _get_persons(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:licensorPerson")
        #print "###", _get_persons(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:distributionRightsHolderPerson")
        #print "###", _get_persons(resource_info, "//cmd:distributionInfo/cmd:iprHolderPerson")
        #print "###", _get_persons(resource_info, "//cmd:contactPerson")
        #print "###", _get_persons(resource_info, "//cmd:metadataInfo/cmd:metadataCreator")

        #print "###", _get_organizations(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:licensorOrganization")
        #print "###", _get_organizations(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:distributionRightsHolderOrganization")
        #print "###", _get_organizations(resource_info, "//cmd:distributionInfo/cmd:iprHolderOrganization")

        contacts = self._persons_as_contact(self._get_persons(rdf, "//dcat:Dataset/dct:publisher"))

        agents = []
        agents.extend(self._persons_as_agent(self._get_persons(rdf, "//dcat:Dataset/dct:creator"), 'author'))
        agents.extend(self._persons_as_agent(self._get_persons(rdf, "//dcat:Dataset/dct:contributor"), 'contributor'))
        # TODO: When owner (or any other agent) is organization should xpath include foaf:organization in read.rdf to make explicit that it is organization?
        # TODO: Owner org is not exported to rdf. Should it be?
        agents.extend(self._persons_as_agent(self._get_persons(rdf, "//dcat:Dataset/dct:rightsHolder"), 'owner'))
        agents.extend(self._funders_as_agent(self._get_project(rdf, "//dcat:Dataset/frapo:isOutputOf"), 'funder'))

        tags = self._text_xpath(rdf, "//dcat:Dataset/dcat:keyword/text()")

        discipline_tags = self._text_xpath(rdf, "//dcat:Dataset/dct:subject/text()")

        distribution = first(rdf.xpath('//dcat:Dataset/dcat:distribution/dcat:Distribution', namespaces=self.namespaces))
        availability = first(distribution.xpath(".//dct:title/text()", namespaces=self.namespaces))

        result = {'name': self._to_name(primary_pid or first(metadata_identifiers)),
                  'language': ",".join(languages),
                  'pids': pids,
                  'version': '',   ##version,
                  'notes': description,
                  #'langtitle': titles,
                  'title': title,
                  'type': 'dataset',
                  'contact': contacts,
                  'agent': agents,
                  'availability': availability,
                  'temporal_coverage_begin': temporal_coverage_begin,
                  'temporal_coverage_end': temporal_coverage_end,
                  'license_id': '',    ##license_identifier
                  'tag_string': ','.join(tags) or '',
                  'discipline': ','.join(discipline_tags) or ''}

        if not languages:
            result['langdis'] = u'True'

        if primary_pid:
            result['id'] = primary_pid

        access_URL_el = None
        if availability == 'direct_download':
            access_URL_el = first(distribution.xpath("//dcat:downloadURL", namespaces=self.namespaces))
        elif availability in ['access_application', 'access_request', 'through_provider']:
            access_URL_el = first(distribution.xpath("//dcat:accessURL", namespaces=self.namespaces))
        access_URL = access_URL_el.attrib.values()[0] if access_URL_el is not None else ''
        if availability in ['direct_download', 'access_request',
                            'access_application', 'through_provider']:
            result['{av}_URL'.format(av=availability)] = access_URL

        # TODO: Ask about distributionAccessMedium
        # _strip_first(_text_xpath(resource_info, "//cmd:distributionInfo/availability/text()"))
        # url = _strip_first(_text_xpath(resource_info, "//cmd:identificationInfo/cmd:url/text()"))
        # download_location = first(self._text_xpath(resource_info, "//cmd:distributionInfo/cmd:licenceInfo/cmd:downloadLocation/text()"))

        return result
