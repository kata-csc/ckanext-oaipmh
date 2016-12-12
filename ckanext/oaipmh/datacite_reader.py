# coding: utf-8

import datetime
import oaipmh.common

from ckanext.oaipmh.importcore import generic_xml_metadata_reader
from lxml import etree
from pylons import config

# for debug
import logging
log = logging.getLogger(__name__)


class DataCiteReader(object):
    """ Reader for DataCite XML data """


    def __init__(self, provider=None):
        """ Generate new reader instance.
        :param provider: URL used for pids.
        """
        super(DataCiteReader, self).__init__()
        self.provider = provider or config.get('ckan.site_url')

    def __call__(self, xml):
        """ Call :meth:`DataCiteReader.read`. """
        return self.read(xml)


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

        # MAP DATACITE MANDATORY FIELD

        # Identifier to primary pid and data pid
        identifier = xml.find('.//{http://datacite.org/schema/kernel-3}identifier')
        primary_pid = identifier.text
        pids = [{
            'id': primary_pid, 
            'type': 'data', 
            'primary': 'True', 
            'provider': identifier.get('identifierType')}]

        log.debug(pids)

        # Creator name to agent
        # TODO: map nameIdentifier to agent.id and nameIdentifierScheme and schemeURI 
        # to extras
        agents = []
        for creator in xml.findall('.//{http://datacite.org/schema/kernel-3}creator'):
            creatorName = creator.find('.//{http://datacite.org/schema/kernel-3}creatorName').text
            creatorAffiliation = creator.find('.//{http://datacite.org/schema/kernel-3}affiliation').text
            agents.append({
                'role': u'author', 
                'name': creatorName, 
                'organisation': creatorAffiliation
                })

        # Primary title to name and title
        # TODO: if titleType is present, check to find out if title is actually primary
        # TODO: map non-primary titles to extras
        name = xml.find('.//{http://datacite.org/schema/kernel-3}title').text
        langtitle = [{'lang': 'en', 'value': name}] # Assuming we always harvest English

        # Publisher to contact
        publisher = xml.find('.//{http://datacite.org/schema/kernel-3}publisher').text
        contacts = [{'name': publisher}]

        # Publication year to event
        publication_year = xml.find('.//{http://datacite.org/schema/kernel-3}publicationYear').text
        events = [{'type': u'published', 'when': publication_year, 'who': publisher, 'descr': u'Dataset was published'}]


        # MAP DATACITE RECOMMENDED FIELDS

        # Subject to tags
        # TODO: map subjectsScheme and schemeURI to extras

        # Contributor to agent
        # TODO: map nameIdentifier to agent.id, nameIdentifierScheme, schemeURI and 
        # contributorType to extras
        for contributor in xml.findall('.//{http://datacite.org/schema/kernel-3}contributor'):
            contributorName = contributor.find('.//{http://datacite.org/schema/kernel-3}contributorName').text
            contributorAffiliation = contributor.find('.//{http://datacite.org/schema/kernel-3}affiliation').text
            agents.append({
                'role': u'contributor', 
                'name': contributorName, 
                'organisation': contributorAffiliation
                })

        # Date to event
        for date in xml.findall('.//{http://datacite.org/schema/kernel-3}date'):
            events.append({
              'type': date.get('dateType'),
              'when': date.text,
              'who': u'unknown',
              'descr': date.get('dateType'),
              })

        # ResourceType to extra
        # TODO: map resourceType and resourceTypeGeneral to extras

        # RelatedIdentifier to showcase
        # TODO: map RelatedIdentifier to showcase title, relatedIdentifierType, relationType, 
        # relatedMetadataScheme, schemeURI and schemeType to showcase description

        # Description to langnotes
        description = ''
        for element in xml.findall('.//{http://datacite.org/schema/kernel-3}description'):
            description += element.get('descriptionType') + ': ' + element.text + ' '
        langnotes = [{
          'lang': 'en', # Assuming we always harvest English
          'value': description,
          }]

        # GeoLocation to geograhic_coverage
        # TODO: map geoLocationPoint and geoLocationBox to extras, geoLocationPlace to 
        # geographic_coverage


        # MAP DATACITE OPTIONAL FIELDS

        # Language to language
        # TODO: map language to language

        # AlternateIdentifier to pids
        # TODO: map AlternateIdentifier to pids.id, alternateIdentifierType to pids.provider

        # Size to extra
        # TODO: map size to extra

        # Format to resources
        # TODO: map format to resources.format

        # Version to extra
        # DataCite version is a string such as 'v3.2.1' and can't be used as Etsin version
        # TODO: map version to extra

        # Rights to license
        license_URL = ''
        for right in xml.findall('.//{http://datacite.org/schema/kernel-3}rights'):
            license_URL += right.text + ' ' + right.get('rightsURI') + ' '


        result = {
                  'agent': agents,
                  'contact': contacts,
                  'event': events,
                  'id': primary_pid,
                  'langnotes': langnotes,
                  'langtitle': langtitle,
                  'license_URL': license_URL,
                  'name': name,
                  'pids': pids,
                  'type': 'dataset',
                  'version': datetime.datetime.now().strftime("%Y-%m-%d")
                  }


        return result
