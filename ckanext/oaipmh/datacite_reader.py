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

        # Identifier to primary pid
        primary_pid = xml.find('.//{http://datacite.org/schema/kernel-3}identifier').text
        pids = [{'id': primary_pid, 'type': 'data', 'primary': 'True'}]

        # Creator name to agent
        # TODO: Creator element has additional information that should be incorporated
        agents = []
        for element in xml.findall('.//{http://datacite.org/schema/kernel-3}creatorName'):
            agents.append({'role': u'author', 'name': element.text})

        # Primary title to name
        # TODO: Dataset may have multiple titles; the first one may not always be primary
        name = xml.find('.//{http://datacite.org/schema/kernel-3}title').text

        # Publisher to contact
        publisher = xml.find('.//{http://datacite.org/schema/kernel-3}publisher').text
        contacts = [{'name': publisher}]

        # Publication year to event
        publication_year = xml.find('.//{http://datacite.org/schema/kernel-3}publicationYear').text
        events = [{'type': u'published', 'when': publication_year, 'who': publisher, 'descr': u'Dataset was published'}]

        result = {
                  'agent': agents,
                  'contact': contacts,
                  'event': events,
                  'id': primary_pid,
                  'name': name,
                  'pids': pids,
                  'type': 'dataset',
                  'version': datetime.datetime.now().strftime("%Y-%m-%d")
                  }


        return result
