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

from ckan.tests import WsgiAppCase
from ckan.lib.helpers import url_for

import lxml.etree
from ckan.logic import get_action
from ckan import model
from ckanext.kata.tests.test_fixtures.unflattened import TEST_DATADICT
from copy import deepcopy


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

    @classmethod
    def teardown_class(cls):
        ckan.model.repo.rebuild_db()

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


class TestOaipmhServer(WsgiAppCase, TestCase):
    """ Test OAI-PMH server """

    _namespaces = {'o': 'http://www.openarchives.org/OAI/2.0/',
                   'oai_dc': "http://www.openarchives.org/OAI/2.0/oai_dc/",
                   'dc': "http://purl.org/dc/elements/1.1/"}

    @classmethod
    def setup_class(cls):
        '''
        Setup database
        '''
        harvest_model.setup()
        kata_model.setup()

    @classmethod
    def teardown(cls):
        ckan.model.repo.rebuild_db()

    def _get_results(self, xml, xpath):
        return xml.xpath(xpath, namespaces=self._namespaces)

    def _get_single_result(self, xml, xpath):
        results = self._get_results(xml, xpath)
        self.assertEquals(len(results), 1)
        return results[0]

    def test_coverage(self):
        model.User(name="test_coverage", sysadmin=True).save()
        organization = get_action('organization_create')({'user': 'test_coverage'}, {'name': 'test-organization-coverage', 'title': "Test organization"})
        package_1_data = deepcopy(TEST_DATADICT)
        package_1_data['owner_org'] = organization['name']
        package_1_data['private'] = False
        package_1_data['name'] = 'test-package-coverage'

        get_action('package_create')({'user': 'test_coverage'}, package_1_data)
        url = url_for('/oai')
        result = self.app.get(url, {'verb': 'GetRecord', 'identifier': 'test-package-coverage', 'metadataPrefix': 'oai_dc' })

        root = lxml.etree.fromstring(result.body)
        expected = ['Keilaniemi (populated place)', 'Espoo (city)', '2003-07-10T06:36:27Z/2010-04-15T03:24:47Z']

        found = 0
        for coverage in self._get_results(root, "//dc:coverage"):
            self.assertTrue(coverage.text in expected)
            found += 1
        self.assertEquals(3, found, "Unexpected coverage results")

        get_action('organization_delete')({'user': 'test_coverage'}, {'id': organization['id']})


    def test_records(self):
        """ Test record fetching via http-request to prevent accidental changes to interface """
        model.User(name="test", sysadmin=True).save()
        organization = get_action('organization_create')({'user': 'test'}, {'name': 'test-organization', 'title': "Test organization"})
        package_1_data = deepcopy(TEST_DATADICT)
        package_1_data['owner_org'] = organization['name']
        package_1_data['private'] = False
        package_2_data = deepcopy(package_1_data)

        package_1_data['name'] = 'test-package1'
        package_2_data['name'] = 'test-package2'

        packages = [get_action('package_create')({'user': 'test'}, package_1_data),
                    get_action('package_create')({'user': 'test'}, package_2_data)]

        package_identifiers = [package['id'] for package in packages]
        package_names = [package['name'] for package in packages]

        url = url_for('/oai')
        result = self.app.get(url, {'verb': 'ListSets'})

        root = lxml.etree.fromstring(result.body)
        request_set = self._get_single_result(root, "//o:set")
        set_name = request_set.xpath("string(o:setName)", namespaces=self._namespaces)
        set_spec = request_set.xpath("string(o:setSpec)", namespaces=self._namespaces)
        self.assertEquals(organization['id'], set_spec)
        self.assertEquals(organization['name'], set_name)

        result = self.app.get(url, {'verb': 'ListIdentifiers', 'set': set_spec, 'metadataPrefix': 'oai_dc' })

        root = lxml.etree.fromstring(result.body)
        fail = True

        for header in root.xpath("//o:header", namespaces=self._namespaces):
            fail = False
            set_spec = header.xpath("string(o:setSpec)", namespaces=self._namespaces)
            identifier = header.xpath("string(o:identifier)", namespaces=self._namespaces)
            self.assertTrue(set_spec in package_names)
            self.assertTrue(identifier in package_identifiers)

            result = self.app.get(url, {'verb': 'GetRecord', 'identifier': identifier, 'metadataPrefix': 'oai_dc' })

            root = lxml.etree.fromstring(result.body)

            fail_record = True
            for record_result in root.xpath("//o:record", namespaces=self._namespaces):
                fail_record = False
                header = self._get_single_result(record_result, 'o:header')
                metadata = self._get_single_result(record_result, 'o:metadata')

                self.assertTrue(header.xpath("string(o:identifier)", namespaces=self._namespaces) in package_identifiers)
                self.assertTrue(header.xpath("string(o:setSpec)", namespaces=self._namespaces) in package_names)

            self.assertFalse(fail_record, "No records received")

        self.assertFalse(fail, "No headers (packages) received")
