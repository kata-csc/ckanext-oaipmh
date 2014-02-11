# coding: utf-8
#
# pylint: disable=no-self-use, missing-docstring, too-many-public-methods, invalid-name

"""
Unit tests for OAI-PMH harvester.
"""
import bs4
from lxml import etree

from unittest import TestCase
import lxml

import ckan
from ckanext.oaipmh.harvester import OAIPMHHarvester
import ckanext.harvest.model as harvest_model
import ckanext.kata.model as kata_model
from ckanext.oaipmh.oai_dc_reader import dc_metadata_reader, _filter_tag_name_namespace, NS

FIXTURE_HELDA = "ckanext-oaipmh/ckanext/oaipmh/test_fixtures/helda_oai_dc.xml"

class TestOAIPMHHarvester(TestCase):

    @classmethod
    def setup_class(cls):
        '''
        Setup database and variables
        '''
        harvest_model.setup()
        kata_model.setup()
        cls.harvester = OAIPMHHarvester()

    @classmethod
    def teardown_class(cls):
        ckan.model.repo.rebuild_db()

    def test_harvester_info(self):
        info = self.harvester.info()
        assert isinstance(info, dict)

    def test_gather_stage(self):
        # gather_stage should throw some exception with parameter None
        self.assertRaises(Exception, self.harvester.gather_stage, (None))

    def test_fetch_stage(self):
        # should throw some exception with parameter None
        self.assertRaises(Exception, self.harvester.fetch_stage, (None))

    def test_import_stage(self):
        # should return false
        assert not self.harvester.import_stage(None)


# class TestImportCore(TestCase):
#
#     def test_harvester_info(self):
#         parse_xml = generic_xml_metadata_reader
#         assert isinstance(info, dict)

class TestOaiDCReader(TestCase):

    def test_dc_metadata_reader(self):
        '''
        Test reading a whole file
        '''

        xml_file = open(FIXTURE_HELDA, 'r')
        metadata = dc_metadata_reader(etree.fromstring(xml_file.read()))

        assert metadata

        assert 'unified' in metadata.getMap()
        assert 'availability' in metadata.getMap()['unified']

    def test_filter_tag_name_namespace(self):

        xml_file = open(FIXTURE_HELDA, 'r')
        bs = bs4.BeautifulSoup(xml_file.read(), 'xml')
        dc = bs.metadata.dc

        output = _filter_tag_name_namespace('creator', NS['dc'])

        creators = [creator for creator in dc(output, recursive=False)]

        assert len(creators) == 3