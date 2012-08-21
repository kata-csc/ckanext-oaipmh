# coding: utf-8
import logging
import os
import unittest
import mock
import urllib2
from StringIO import StringIO
import json
import testdata

from ckan.model import Session, Package, User
import ckan.model as model
from ckan.tests import CreateTestData
from ckan.lib.helpers import url_for
from ckan.logic.auth.get import package_show
from ckan.tests.functional.base import FunctionalTestCase

from lxml import etree

from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader

from pylons import config

from ckanext.oaipmh.harvester import OAIPMHHarvester
from ckanext.harvest.model import HarvestJob, HarvestSource, HarvestObject,\
                                  setup

def fileInTestDir(name):
    _testdir = os.path.split(__file__)[0]
    return os.path.join(_testdir, name)

log = logging.getLogger(__name__)

oaischema = etree.XMLSchema(etree.parse(fileInTestDir('OAI-PMH.xsd')))

class TestOAIPMH(FunctionalTestCase, unittest.TestCase):

    base_url = url_for(controller='ckanext.oaipmh.controller:OAIPMHController', 
                       action='index')
    _first = True
    _second = False
    @classmethod
    def setup_class(cls):
        """
        Remove any initial sessions.
        """
        Session.remove()
        CreateTestData.create()
        setup()
        cls._first = True
        cls._second = False

    @classmethod
    def teardown_class(cls):
        """
        Tear down, remove the session.
        """
        CreateTestData.delete()
        Session.remove()

    def _oai_get_method_and_validate(self, url):
        offset =  self.base_url + url
        res = self.app.get(offset)
        self.assert_(oaischema.validate(etree.fromstring(res.body)))
        return res.body

    def test_list_sets(self):
        self._oai_get_method_and_validate('?verb=ListSets')

    def test_list_records(self):
        self._oai_get_method_and_validate('?verb=ListRecords&metadataPrefix=oai_dc&set=roger')

    def test_list_records_null(self):
        self._oai_get_method_and_validate('?verb=ListRecords&metadataPrefix=oai_dc&set=foo')

    def test_list_identifiers(self):
        self._oai_get_method_and_validate('?verb=ListIdentifiers&metadataPrefix=oai_dc&set=annakarenina')

    def test_list_identifiers_null(self):
        self._oai_get_method_and_validate('?verb=ListIdentifiers&metadataPrefix=oai_dc&set=foo')

    def test_list_metadata(self):
        self._oai_get_method_and_validate('?verb=ListMetadataFormats')

    def test_identify(self):
        self._oai_get_method_and_validate('?verb=Identify')

    def test_get_record(self):
        metadata_reg = MetadataRegistry()
        metadata_reg.registerReader('oai_dc', oai_dc_reader)
        client = Client(config.get('ckan.site_url')+self.base_url, metadata_reg)
        res = self._oai_get_method_and_validate('?verb=ListIdentifiers&metadataPrefix=oai_dc&set=roger')
        urllib2.urlopen = mock.Mock(return_value=StringIO(res))
        ids = client.listIdentifiers(metadataPrefix='oai_dc')
        for id in ids:
            offset =  self.base_url + '?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc' % id.identifier()
            res = self.app.get(offset)
            self.assert_(oaischema.validate(etree.fromstring(res.body)))

    def _create_harvester_info(self):
        rev = model.repo.new_revision()
        harv = OAIPMHHarvester()
        harvest_job = HarvestJob()
        harvest_job.source = HarvestSource()
        harvest_job.source.title = "Test"
        harvest_job.source.url = "http://helda.helsinki.fi/oai/request"
        harvest_job.source.config = '{"query":""}'
        harvest_job.source.type = "OAI-PMH"
        Session.add(harvest_job)
        return harvest_job, harv

    def _alternate_returns(self, foo):
        if self._first:
            res = self._oai_get_method_and_validate('?verb=Identify')
            self._second = True
            self._first = False
            ret = StringIO(res)
            return ret
        else:
            res = testdata.listsets
            self._second = False
            self._first = True
            ret = StringIO(res)
            return ret

    def _create_harvester(self):
        urllib2.urlopen = mock.Mock(side_effect=self._alternate_returns)
        harvest_job, harv = self._create_harvester_info()
        harvest_obj_list = harv.gather_stage(harvest_job)
        harvest_object = HarvestObject.get(harvest_obj_list[0])
        urllib2.urlopen = mock.Mock(return_value=StringIO(testdata.listrecords))
        harv.fetch_stage(harvest_object)
        return harvest_object, harv

    def test_harvester_import(self, mocked=True):
        harvest_object, harv = self._create_harvester()
        real_content = json.loads(harvest_object.content)
        self.assert_(real_content)
        self.assert_(harv.import_stage(harvest_object))

        the_package = Session.query(Package).filter(Package.title == u"Perunan typpilannoitus luonnonmukaisessa viljelyssä")
        the_package = the_package[0]
        self.assert_(the_package.url == "http://helda.helsinki.fi/oai/request?verb=getRecord&identifier=oai:helda.helsinki.fi:1975/7634&metadataPrefix=oai_dc")
        self.assert_(len(the_package.get_tags()) == 7)
        self.assert_(len(the_package.get_groups()) == 2)
        self.assert_(the_package.author == "Tall, Anna")
        pkg = Session.query(Package).all()[0]
        # Test user access
        user = User.get('tester')
        context = {'user': user.name, 'model': model}
        data_dict = {'id': pkg.id}
        auth_dict = package_show(context,data_dict)
        self.assert_(auth_dict['success'])

