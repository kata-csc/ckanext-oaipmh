# coding: utf-8
#
# pylint: disable=no-self-use, missing-docstring, too-many-public-methods, invalid-name

"""
Test classes for OAI-PMH harvester.
"""
import ckan
import mock

import oaipmh.client

from unittest import TestCase
from ckanext.oaipmh import importformats
from ckanext.oaipmh.harvester import OAIPMHHarvester
import ckanext.harvest.model as harvest_model
import ckanext.kata.model as kata_model


class TestOAIPMHHarvester(TestCase):

    @classmethod
    def setup_class(cls):
        '''
        Setup database and variables
        '''
        harvest_model.setup()
        kata_model.setup()
        cls.harvester = OAIPMHHarvester()

    def test_get_field_titles(self):
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

    @classmethod
    def teardown_class(cls):
        ckan.model.repo.rebuild_db()


class TestImportFormats(TestCase):

    TEST_ID = "urn:nbn:fi:csc-ida2013032600070s"

    @classmethod
    def setup_class(cls):
        '''
        Setup database and variables
        '''
        harvest_model.setup()
        kata_model.setup()
        cls.harvester = OAIPMHHarvester()

    # def test_list(self):
    #     registry = importformats.create_metadata_registry()
    #     client = oaipmh.client.Client("http://someurl.com", registry)
    #     identifiers = (header.identifier() for header in client.listIdentifiers(metadataPrefix='oai_dc'))
    #
    #     assert identifiers

    # TODO: Use mock to read from xml-file instead of URL.

    # def test_fetch(self):
    #     registry = importformats.create_metadata_registry()
    #     client = oaipmh.client.Client("http://csc.fi", registry)
    #     record = client.getRecord(identifier=self.TEST_ID, metadataPrefix='oai_dc')
    #
    #     assert record, record

    @classmethod
    def teardown_class(cls):
        ckan.model.repo.rebuild_db()

