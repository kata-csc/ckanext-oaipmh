# coding: utf-8
#
# pylint: disable=no-self-use, missing-docstring, too-many-public-methods, invalid-name
"""
Unit tests for OAI-PMH harvester.
"""
import copy
from unittest import TestCase

import testfixtures
import bs4
from lxml import etree
from pylons import config

import ckan
from ckanext.harvest.commands import harvester
from ckanext.harvest.model import HarvestJob, HarvestSource, HarvestObject
from ckanext.oaipmh.cmdi import CMDIHarvester
from ckanext.oaipmh.cmdi_reader import CmdiReader
from ckanext.oaipmh.harvester import OAIPMHHarvester
import ckanext.harvest.model as harvest_model
import ckanext.kata.model as kata_model
from ckanext.oaipmh.ida import IdaHarvester
from ckanext.oaipmh.importformats import create_metadata_registry
import ckanext.oaipmh.oai_dc_reader as dcr
from ckanext.oaipmh.oai_dc_reader import dc_metadata_reader
import os
from ckan import model
from ckan.logic import get_action
import json


FIXTURE_HELDA = "helda_oai_dc.xml"
FIXTURE_IDA = "oai-pmh.xml"


def _get_fixture(filename):
    return os.path.join(os.path.dirname(__file__), "..", "test_fixtures", filename)


def _get_record(filename):
    tree = etree.parse(_get_fixture(filename))
    return tree.xpath('/oai:OAI-PMH/*/oai:record', namespaces={'oai': 'http://www.openarchives.org/OAI/2.0/'})[0]


def _get_single_package():
    packages = model.Session.query(model.Package).filter_by(state=model.State.ACTIVE)
    assert len(list(packages)) == 1
    return packages[0]


class _FakeHarvestSource():
    def __init__(self, config, url):
        self.config = json.dumps(config)
        self.url = url


class _FakeHarvestJob():
    def __init__(self, source):
        self.source = source


class _FakeHarvestObject():
    def __init__(self, content, identification, config, source_url=None):
        self.content = content
        self.id = identification
        self.guid = self.id
        self.source = _FakeHarvestSource(config, source_url)
        self.harvest_source_id = None
        self.job = _FakeHarvestJob(self.source)
        self.report_status = None

    def add(self):
        pass

    def save(self):
        pass


class _FakeIdentifier():
    def __init__(self, identifier):
        self._identifier = identifier

    def identifier(self):
        return self._identifier


class _FakeClient():
    def listIdentifiers(self, metadataPrefix):
        return [_FakeIdentifier('oai:kielipankki.fi:sha3a880')]

class TestOAIPMHHarvester(TestCase):

    @classmethod
    def setup_class(cls):
        '''
        Setup database and variables
        '''
        harvest_model.setup()
        kata_model.setup()
        cls.harvester = OAIPMHHarvester()

    def tearDown(self):
        ckan.model.repo.rebuild_db()

    def test_harvester_info(self):
        info = self.harvester.info()
        assert isinstance(info, dict)

    def test_gather_stage(self):
        # gather_stage should throw some exception with parameter None
        self.assertRaises(Exception, self.harvester.gather_stage, (None))

    def test_fetch_stage(self):
        url = "file://%s" % _get_fixture('ida.xml')
        harvest_object = _FakeHarvestObject(None, "test_fetch_id", {'type': 'ida'}, url)
        self.harvester.fetch_stage(harvest_object)

    def test_fetch_stage_invalid(self):
        url = "file://%s" % _get_fixture('ida_invalid.xml')
        harvest_object = _FakeHarvestObject(None, "test_fetch_id", {'type': 'ida'}, url)
        self.assertRaises(Exception, self.harvester.fetch_stage, harvest_object)

    def test_import_stage(self):
        assert not self.harvester.import_stage(None)

    def _run_import(self, xml, ida, config=None):
        if not model.User.get('harvest'):
            model.User(name='harvest', sysadmin=True).save()
        if not model.Group.get('test'):
            get_action('organization_create')({'user': 'harvest'}, {'name': 'test'})

        record = _get_record(xml)
        harvest_type = 'ida' if ida else 'default'
        if config is None:
            config = {'type': harvest_type}

        metadata = dc_metadata_reader(harvest_type)(record)
        metadata['unified']['owner_org'] = "test"
        harvest_object = _FakeHarvestObject(json.dumps(metadata.getMap()), "test_id", config)

        self.harvester.import_stage(harvest_object)

    def test_import_stage_data(self):
        for xml_path, ida in ('ida.xml', True), ('helda.xml', False):
            self._run_import(xml_path, ida)
            package = _get_single_package()
            package_dict = get_action('package_show')({'model': model, 'session': model.Session, 'user': 'harvest'}, {'id': package.id})
            if ida:
                self.assertTrue('direct_download' not in package.notes)
                self.assertEquals(package.extras.get('availability', None), 'direct_download')
                pid_ids = [pid.get('id') for pid in package_dict.get('pids', [])]
                self.assertTrue(u'test-version' in pid_ids)
                self.assertTrue(u'urn:nbn:fi:csc-ida2014010800372s' in pid_ids)
                self.assertEquals('application/test', package_dict['mimetype'])

                self.assertEquals(package.extras.get('availability', None), 'direct_download')
                expected = (u'contact_0_email', u'test1@example.fi'), (u'contact_0_name', u'Test Person1'), (u'contact_0_phone', u'0501231234'), \
                           (u'contact_1_email', u'test2@example.fi'), (u'contact_1_name', u'Test Person2'), (u'contact_1_phone', u'0501231234'),

                for key, value in expected:
                    self.assertEquals(package.extras.get(key), value)

            package.delete()
            model.repo.commit()

    def test_import_stage_tags(self):
        self._run_import('oai-pmh.xml', True)
        package = _get_single_package()
        tags = [tag.name for tag in package.get_tags()]
        self.assertTrue('televisiokasvatus' in tags)

    def test_import_stage_project(self):
        self._run_import('ida3.xml', True)
        package = _get_single_package()

        expected = [('agent_1_organisation', 'Paras yliopisto')]

        for key, value in expected:
            self.assertEquals(package.extras.get(key), value)

    def test_import_stage_recreate(self):
        """ Manual test for recreating harvested dataset multiple times """

        for xml, recreate in ('ida.xml', False), ('ida2.xml', False), ('ida2.xml', True):
            configuration = {'type': 'ida'}
            if recreate:
                configuration['recreate'] = True

            self._run_import(xml, True, configuration)

            package = _get_single_package()
            if recreate:
                self.assertEquals(package.extras.get('availability', None), 'MODIFIED')
            else:
                self.assertEquals(package.extras.get('availability', None), 'direct_download')

        package.delete()
        model.repo.commit()

        for xml, expected, recreate in ('helda.xml', 'ORIGINAL', True), ('helda2.xml', 'MODIFIED', True), ('helda.xml', 'MODIFIED', False):
            configuration = {'type': 'default'}
            if not recreate:
                configuration['recreate'] = False

            self._run_import(xml, False, configuration)
            package = _get_single_package()
            self.assertEquals(package.extras.get('agent_1_name', None), expected)

    def test_validate_config_valid(self):
        config = '{"from": "2014-03-03", "limit": 5}'

        config = self.harvester.validate_config(config)

        assert isinstance(config, basestring)
        assert 'limit' in config

    def test_validate_config_invalid(self):
        config = '{"from": 100, "limit": 5}'

        # 'from' is not a string so should throw an error
        self.assertRaises(TypeError, self.harvester.validate_config, (config))

    def test_fetch_xml(self):
        package = self.harvester.fetch_xml("file://%s" % _get_fixture('helda.xml'), {})
        self.assertEquals(package.get('name', None), u'http---hdl-handle-net-10138-8487')

    def test_parse_xml(self):
        with open(_get_fixture('helda.xml'), 'r') as source:
            package = self.harvester.parse_xml(source.read(), {})
            self.assertEquals(package.get('name', None), u'http---hdl-handle-net-10138-8487')


class TestIdaHarvester(TestCase):
    @classmethod
    def setup_class(cls):
        ''' Setup database and variables '''
        harvest_model.setup()
        kata_model.setup()
        cls.harvester = IdaHarvester()

    def tearDown(self):
        """ rebuild database """
        ckan.model.repo.rebuild_db()

    def test_fetch_xml(self):
        package = self.harvester.fetch_xml("file://%s" % _get_fixture('ida.xml'), {})
        self.assertEquals(package.get('name', None), u'urn-nbn-fi-csc-ida2014010800372s')

    def test_parse_xml(self):
        with open(_get_fixture('ida.xml'), 'r') as source:
            package = self.harvester.parse_xml(source.read(), {})
            self.assertEquals(package.get('name', None), u'urn-nbn-fi-csc-ida2014010800372s')


class TestCMDIHarvester(TestCase):
    @classmethod
    def setup_class(cls):
        ''' Setup database and variables '''
        harvest_model.setup()
        kata_model.setup()
        cls.harvester = CMDIHarvester()

    def tearDown(self):
        """ rebuild database """
        ckan.model.repo.rebuild_db()

    def _run_import(self, xml, job):
        if not model.User.get('harvest'):
            model.User(name='harvest', sysadmin=True).save()
        if not model.Group.get('test'):
            get_action('organization_create')({'user': 'harvest'}, {'name': 'test'})

        record = _get_record(xml)

        metadata = CmdiReader()(record)
        metadata['unified']['owner_org'] = "test"

        harvest_object = HarvestObject()
        harvest_object.content = json.dumps(metadata.getMap())
        harvest_object.id = xml
        harvest_object.guid = xml
        harvest_object.source = job.source
        harvest_object.harvest_source_id = None
        harvest_object.job = job
        harvest_object.save()

        self.harvester.import_stage(harvest_object)
        return harvest_object

    def test_reader(self):
        record = _get_record("cmdi_1.xml")
        metadata = CmdiReader("http://localhost/test")(record)
        content= metadata.getMap()
        package = content['unified']
        self.assertEquals(package.get('name', None), 'urn-nbn-fi-lb-20140730180')
        self.assertEquals(package.get('notes', None), '{"eng": "Test description"}')
        self.assertEquals(package.get('version', None), '2012-09-07')
        self.assertEquals(package.get('title', []), '{"eng": "Longi Corpus"}')

    def test_gather(self):
        source = HarvestSource(url="http://localhost/test_cmdi", type="cmdi")
        source.save()
        job = HarvestJob(source=source)
        job.save()
        self.harvester.client = _FakeClient()
        self.harvester.gather_stage(job)

    def test_import(self):
        source = HarvestSource(url="http://localhost/test_cmdi", type="cmdi")
        source.save()
        job = HarvestJob(source=source)
        job.save()

        harvest_object = self._run_import("cmdi_1.xml", job)

        self.assertEquals(len(harvest_object.errors), 0, u"\n".join(unicode(error.message) for error in (harvest_object.errors or [])))

        package = get_action('package_show')({'user': 'harvest'}, {'id': 'urn-nbn-fi-lb-20140730180'})

        self.assertEquals(package.get('id', None), 'http://urn.fi/urn:nbn:fi:lb-20140730180')
        self.assertEquals(package.get('name', None), 'urn-nbn-fi-lb-20140730180')
        self.assertEquals(package.get('notes', None), u'{"eng": "Test description"}')
        self.assertEquals(package.get('version', None), '2012-09-07')
        self.assertEquals(package.get('title', []), '{"eng": "Longi Corpus"}')
        self.assertEquals(package.get('license_id', None), 'underNegotiation')

        provider = config['ckan.site_url']
        expected_pid = {u'id': u'http://islrn.org/resources/248-895-085-557-0',
                        u'provider': provider,
                        u'type': u'metadata'}

        self.assertTrue(expected_pid in package.get('pids'))

        model.Session.flush()

        harvest_object = self._run_import("cmdi_2.xml", job)

        self.assertEquals(len(harvest_object.errors), 0, u"\n".join(unicode(error.message) for error in (harvest_object.errors or [])))

        package = get_action('package_show')({'user': 'harvest'}, {'id': 'urn-nbn-fi-lb-20140730186'})

        self.assertEquals(package['temporal_coverage_begin'], '1880')
        self.assertEquals(package['temporal_coverage_end'], '1939')
        self.assertEquals(package.get('license_id', None), 'other')
        # Delete package
        harvest_object = HarvestObject()
        harvest_object.content = None
        harvest_object.id = "test-cmdi-delete"
        harvest_object.guid = "test-cmdi-delete"
        harvest_object.source = job.source
        harvest_object.harvest_source_id = None
        harvest_object.job = job
        harvest_object.package_id = package.get('id')
        harvest_object.report_status = "deleted"
        harvest_object.save()

        self.harvester.import_stage(harvest_object)

        model.Session.flush()
        self.assertEquals(model.Package.get(package['id']).state, 'deleted')

    def test_fetch_xml(self):
        package = self.harvester.fetch_xml("file://%s" % _get_fixture('cmdi_1.xml'), {})
        self.assertEquals(package.get('notes', None), '{"eng": "Test description"}')
        self.assertEquals(package.get('version', None), '2012-09-07')

    def test_parse_xml(self):
        with open(_get_fixture('cmdi_1.xml'), 'r') as source:
            package = self.harvester.parse_xml(source.read(), {})
            self.assertEquals(package.get('notes', None), '{"eng": "Test description"}')
            self.assertEquals(package.get('version', None), '2012-09-07')


class TestOAIDCReaderHelda(TestCase):
    '''
    Tests for reading OAI_DC metadata generated from Helda
    '''

    @classmethod
    def setup_class(cls):
        '''
        Setup variables
        '''
        cls.xml_file = open(_get_fixture(FIXTURE_HELDA), 'r')
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

        metadata = dcr.dc_metadata_reader('default')(etree.fromstring(self.xml))

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
                           'contact': [],
                           'direct_download_URL': u'http://link.aip.org/link/?jcp/123/064507',
                           'discipline': '',
                           'geographic_coverage': '',
                           #'langtitle': [{'lang': '',
                           #               'value': u'Neutralization of solvated protons and formation of noble-gas hydride molecules: matrix-isolation indications of tunneling mechanisms?'}],
                           'title': '{"zxx": "Neutralization of solvated protons and formation of noble-gas hydride molecules: matrix-isolation indications of tunneling mechanisms?"}',
                           'language': u'en',
                           'license_URL': u'Copyright 2005 American Institute of Physics. This article may be downloaded for personal use only. Any other use requires prior permission of the author and the American Institute of Physics.',
                           'license_id': 'notspecified',
                           'mimetype': '',
                           'name': 'http---link-aip-org-link--jcp-123-064507',
                           'notes': '{"zxx": ""}',
                           'pids': [{'type': 'data',
                                     'id': u'http://link.aip.org/link/?jcp/123/064507',
                                     'provider': u'http://helda.helsinki.fi/oai/request'},
                                    {'id': u'http://hdl.handle.net/10138/1074',
                                     'provider': u'http://helda.helsinki.fi/oai/request',
                                     'type': 'data'},
                                    ],
                           'tag_string': '',
                           'temporal_coverage_begin': '',
                           'temporal_coverage_end': '',
                           'through_provider_URL': u'http://link.aip.org/link/?jcp/123/064507',
                           'type': 'dataset',
                           'version': u'2005-08-08',
                           'uploader': u''}

        metadata = dcr.dc_metadata_reader('default')(etree.fromstring(self.xml))
        assert metadata

        data_dict = metadata['unified']

        temp = copy.copy(data_dict)

        temp.pop('agent')   # TODO: Compare also agents directly

        testfixtures.compare(temp, EXPECTED_FIELDS)

        # for (key, value) in EXPECTED_FIELDS.items():
        #     assert key in data_dict, "Key not found: %r" % key
        #
        #     output_value = data_dict.get(key)
        #
        #     # Note. Possibility for random fail, because data order is not promised by python
        #     # TODO: testfixtures.compare() could be used here to prevent random failing
        #     assert unicode(output_value) == unicode(value), "Values for key %r not matching: %r versus %r" % (
        #         key, value, output_value)

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
        cls.xml_file = open(_get_fixture(FIXTURE_IDA), 'r')
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

        metadata = dcr.dc_metadata_reader('default')(etree.fromstring(self.xml))

        assert metadata

        assert 'unified' in metadata.getMap()
        assert 'availability' in metadata.getMap()['unified']

    def test_get_version_pid(self):
        tests = ((dcr.IdaDcMetadataReader, 'ida.xml', True), (dcr.DefaultDcMetadataReader, 'helda.xml', False),
                 (dcr.IdaDcMetadataReader, 'oai-pmh.xml', True))
        for reader_class, xml, ida in tests:
            reader = reader_class(_get_record(xml))
            # Testing private method. This can be removed when manual tests start to work.
            pid = reader._get_version_pids()  # pylint: disable=W0212
            if ida:
                self.assertTrue(bool(pid))
            else:
                self.assertFalse(bool(pid))

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
