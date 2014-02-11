# coding: utf-8
#
# pylint: disable=no-self-use, missing-docstring, too-many-public-methods, invalid-name

"""
Functional tests for OAI-PMH harvester.
"""

from unittest import TestCase

import oaipmh.client
import pointfree as pf
import ckan

from ckanext.harvest import model as harvest_model
from ckanext.oaipmh import importformats
from ckanext.oaipmh.harvester import OAIPMHHarvester
import ckanext.kata.model as kata_model

FIXTURE_LISTIDENTIFIERS = "file:ckanext-oaipmh/ckanext/oaipmh/test_fixtures/listidentifiers.xml"
FIXTURE_DATASET = "file:ckanext-oaipmh/ckanext/oaipmh/test_fixtures/oai-pmh.xml"

class TestReadingFixtures(TestCase):

    TEST_ID = "urn:nbn:fi:csc-ida2013032600070s"

    @classmethod
    def setup_class(cls):
        '''
        Setup database and variables
        '''
        harvest_model.setup()
        kata_model.setup()
        cls.harvester = OAIPMHHarvester()

    def test_list(self):
        '''
        Parse ListIdentifiers result
        '''

        registry = importformats.create_metadata_registry()
        client = oaipmh.client.Client(FIXTURE_LISTIDENTIFIERS, registry)
        identifiers = (header.identifier() for header in client.listIdentifiers(metadataPrefix='oai_dc'))

        assert 'oai:arXiv.org:hep-th/9801001' in identifiers
        assert 'oai:arXiv.org:hep-th/9801002' in identifiers
        assert 'oai:arXiv.org:hep-th/9801005' in identifiers
        assert 'oai:arXiv.org:hep-th/9801010' in identifiers

    def test_fetch(self):
        '''
        Parse example dataset
        '''
        registry = importformats.create_metadata_registry()
        client = oaipmh.client.Client(FIXTURE_DATASET, registry)
        record = client.getRecord(identifier=self.TEST_ID, metadataPrefix='oai_dc')

        assert record

    def test_fetch_fail(self):
        '''
        Try to parse ListIdentifiers result as a dataset (basically testing PyOAI)
        '''
        def getrecord():
            client.getRecord(identifier=self.TEST_ID, metadataPrefix='oai_dc')

        registry = importformats.create_metadata_registry()
        client = oaipmh.client.Client(FIXTURE_LISTIDENTIFIERS, registry)
        self.assertRaises(Exception, getrecord)

    @classmethod
    def teardown_class(cls):
        ckan.model.repo.rebuild_db()