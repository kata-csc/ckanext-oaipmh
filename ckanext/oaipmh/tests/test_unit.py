# coding: utf-8
#
# pylint: disable=no-self-use, missing-docstring, too-many-public-methods, invalid-name
"""
Unit tests for OAI-PMH harvester.
"""
from unittest import TestCase

import bs4
from lxml import etree

import ckan
from ckanext.oaipmh.harvester import OAIPMHHarvester
import ckanext.harvest.model as harvest_model
import ckanext.kata.model as kata_model
from ckanext.oaipmh.importformats import create_metadata_registry
import ckanext.oaipmh.oai_dc_reader as dcr


FIXTURE_HELDA = "ckanext-oaipmh/ckanext/oaipmh/test_fixtures/helda_oai_dc.xml"
FIXTURE_IDA = "ckanext-oaipmh/ckanext/oaipmh/test_fixtures/oai-pmh.xml"

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
        self.assertRaises(Exception, self.harvester.fetch_stage, (None))

    def test_validate_config_valid(self):
        config = '{"from": "2014-03-03", "limit": 5}'

        config = self.harvester.validate_config(config)

        assert isinstance(config, basestring)
        assert 'limit' in config

    def test_validate_config_invalid(self):
        config = '{"from": 100, "limit": 5}'

        # 'from' is not a string so should throw an error
        self.assertRaises(TypeError, self.harvester.validate_config, (config))


# class TestImportCore(TestCase):
#
#     def test_harvester_info(self):
#         parse_xml = generic_xml_metadata_reader
#         assert isinstance(info, dict)

class TestOAIDCReaderHelda(TestCase):
    '''
    Tests for reading OAI_DC metadata generated from Helda
    '''

    @classmethod
    def setup_class(cls):
        '''
        Setup variables
        '''
        cls.xml_file = open(FIXTURE_HELDA, 'r')
        cls.xml = cls.xml_file.read()

        cls.bs = bs4.BeautifulSoup(cls.xml, 'xml')
        cls.dc = cls.bs.metadata.dc

    @classmethod
    def teardown_class(cls):
        pass

    def test_dc_metadata_reader(self):
        '''
        Test reading a whole file
        '''

        metadata = dcr.dc_metadata_reader(etree.fromstring(self.xml))

        assert metadata

        assert 'unified' in metadata.getMap()
        assert 'availability' in metadata.getMap()['unified']

    def test_filter_tag_name_namespace(self):
        output = dcr._filter_tag_name_namespace('creator', dcr.NS['dc'])
        creators = [creator for creator in self.dc(output, recursive=False)]

        assert len(creators) == 3

    def test_get_data_pids(self):
        expected_pids = set([u'http://link.aip.org/link/?jcp/123/064507', u'http://hdl.handle.net/10138/1074'])
        output = dcr._get_data_pids(self.dc)

        assert set(output) == expected_pids

    def test_get_download(self):
        output = dcr._get_download(self.dc)

        # We should get atleast some download link from the pids:
        assert len(list(output)) > 0

    def test_get_org_auth(self):
        output = dcr._get_org_auth(self.dc)

        org_auth = []
        auths = []
        for row in output:
            org_auth.append(row)
            auths.append(row.get('value'))

        assert org_auth
        assert auths

        assert u"Khriachtchev, Leonid" in auths
        assert u"Räsänen, Markku" in auths

    def test_get_rights(self):
        output = dcr._get_rights(self.dc)

        values = list(output)
        assert len(values) == 4

        license_url = values[2]

        assert license_url
        assert license_url.startswith('Copyright')

    def test_dc_metadata_reader_fields(self):
        '''
        Test reading a whole file and check that fields are what they are supposed to be
        '''
        EXPECTED_FIELDS = {'access_application_URL': '',
                           'access_request_URL': '',
                           'algorithm': '',
                           'availability': 'through_provider',
                           'checksum': '',
                           'contact': [{'URL': '', 'phone': '', 'email': '', 'name': ''}],
                           'direct_download_URL': u'http://link.aip.org/link/?jcp/123/064507',
                           'discipline': '',
                           'geographic_coverage': '',
                           'langtitle': [{'lang': '',
                                          'value': u'Neutralization of solvated protons and formation of noble-gas hydride molecules: matrix-isolation indications of tunneling mechanisms?'}],
                           'language': u'en',
                           'license_URL': u'Copyright 2005 American Institute of Physics. This article may be downloaded for personal use only. Any other use requires prior permission of the author and the American Institute of Physics.',
                           'license_id': 'notspecified',
                           'mimetype': '',
                           'name': u'http%3A%2F%2Flink.aip.org%2Flink%2F%3Fjcp%2F123%2F064507',
                           'notes': '',
                           'pids': [{'id': u'http://hdl.handle.net/10138/1074',
                                     'provider': u'http://helda.helsinki.fi/oai/request',
                                     'type': 'data'},
                                    ],
                           'tag_string': '',
                           'temporal_coverage_begin': '',
                           'temporal_coverage_end': '',
                           'through_provider_URL': u'http://link.aip.org/link/?jcp/123/064507',
                           'type': 'dataset',
                           'version': u'2005-08-08'}

        metadata = dcr.dc_metadata_reader(etree.fromstring(self.xml))
        assert metadata

        data_dict = metadata['unified']

        for (key, value) in EXPECTED_FIELDS.items():
            assert key in data_dict, "Key not found: %r" % key

            output_value = data_dict.get(key)

            # Note. Possibility for random fail, because data order is not promised by python
            assert unicode(output_value) == unicode(value), "Values for key %r not matching: %r versus %r" % (
                key, value, output_value)

        fail_agent = 1
        fail_author = 3
        for agent in data_dict.get('agent', []):
            if agent['role'] == 'funder':
                for key, value in ('URL', ''), ('id', ''), ('fundingid', ''), ('name', ''):
                    self.assertTrue(key in agent, "Expected to find key %s" % key)
                    self.assertEquals(agent[key], value)
                fail_agent -= 1
            elif agent['role'] == 'author':
                self.assertTrue(agent['name'] in (u'Khriachtchev, Leonid', u'Lignell, Antti', u'R\xe4s\xe4nen, Markku'))
                fail_author -= 1

        self.assertEqual(fail_agent, 0, "Invalid agent data")
        self.assertEqual(fail_author, 0, "Invalid author data")

    # TODO: Implement this in harvester first
    # def test_get_provider(self):
    #     output = dcr._get_provider(self.dc)
    #
    #     assert output == u'http://helda.helsinki.fi/oai/request', output


class TestOAIDCReaderIda(TestCase):

    @classmethod
    def setup_class(cls):
        '''
        Setup variables
        '''
        cls.xml_file = open(FIXTURE_IDA, 'r')
        cls.xml = cls.xml_file.read()

        cls.bs = bs4.BeautifulSoup(cls.xml, 'xml')
        cls.dc = cls.bs.metadata.dc

    @classmethod
    def teardown_class(cls):
        pass

    def test_dc_metadata_reader(self):
        '''
        Test reading a whole file
        '''

        metadata = dcr.dc_metadata_reader(etree.fromstring(self.xml))

        assert metadata

        assert 'unified' in metadata.getMap()
        assert 'availability' in metadata.getMap()['unified']

    def test_get_version_pid(self):
        pid = dcr._get_version_pid(self.dc)

        assert pid
        assert 'ida' in next(pid)

    def test_get_checksum(self):
        hash = dcr._get_checksum(self.dc)

        assert hash == u'7932df5999a30bb70871359f700dbe23'

    def test_get_download(self):
        output = dcr._get_download(self.dc)

        # We should get atleast some download link:
        assert len(list(output)) > 0

    def test_get_provider(self):
        output = dcr._get_provider(self.dc)

        assert output == 'ida', output


class TestImportFormats(TestCase):
    def test_create_metadata_registry(self):
        reg = create_metadata_registry()

        assert reg
        assert reg.hasReader('oai_dc')
