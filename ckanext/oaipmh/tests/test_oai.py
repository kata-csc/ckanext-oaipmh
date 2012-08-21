# coding: utf-8
import logging
import os
import unittest
import mock
import urllib2
from StringIO import StringIO
import json
from datetime import datetime, timedelta

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
        package_dicts = [{'name':u'abraham', 'title':u'Abraham'},
                {'name':u'homer', 'title':u'Homer', 'tags':['foo', 'bar', 'baz']},
                {'name':u'homer_derived', 'title':u'Homer Derived'},
                {'name':u'beer', 'title':u'Beer'},
                {'name':u'bart', 'title':u'Bart'},
                {'name':u'lisa', 'title':u'Lisa'},
                {'name':u'marge', 'title':u'Marge'},
                {'name':u'marge1', 'title':u'Marge'},
                {'name':u'marge11', 'title':u'Marge'},
                {'name':u'marge121', 'title':u'Marge'},
                {'name':u'marge311', 'title':u'Marge'},
                {'name':u'marge24', 'title':u'Marge'},
                {'name':u'marget1', 'title':u'Marge'},
                {'name':u'marge31', 'title':u'Marge'},
                {'name':u'marge1121', 'title':u'Marge'},
                {'name':u'marge1t', 'title':u'Marge'},
                {'name':u'marge1b', 'title':u'Marge'},
                {'name':u'marge1a', 'title':u'Marge'},
                ]
        
        CreateTestData.create_arbitrary(package_dicts)
        package_dicts = [u'abraham',
                u'homer',
                u'homer_derived',
                u'beer',
                u'bart',
                u'lisa',
                u'marge',
                u'marge1',
                u'marge11',
                u'marge121',
                u'marge311',
                u'marge24',
                u'marget1',
                u'marge31',
                u'marge1121',
                u'marge1t',
                u'marge1b',
                u'marge1a',
                ]
        group_dicts = [{'name':'roger', 'title':'roger', 'description':'','packages': package_dicts}]
        CreateTestData.create_groups(group_dicts)
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
        body = self._oai_get_method_and_validate('?verb=ListSets')
        self.assert_('roger' in body)

    def test_list_records(self):
        # Good set
        body = self._oai_get_method_and_validate('?verb=ListRecords&metadataPrefix=oai_dc&set=roger')
        self.assert_("homer" in body)
        # Bad set
        body = self._oai_get_method_and_validate('?verb=ListRecords&metadataPrefix=oai_dc&set=foo')
        self.assert_("noRecordsMatch" in body)
        
        # A good from
        body = self._oai_get_method_and_validate('?verb=ListRecords&metadataPrefix=oai_dc&from=1998-01-15')
        self.assert_("homer" in body)
        # A bad from, in the future
        dates = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        body = self._oai_get_method_and_validate('?verb=ListRecords&metadataPrefix=oai_dc&from=%s' % dates)
        self.assert_("noRecordsMatch" in body)
        
        # A good until, in the future
        body = self._oai_get_method_and_validate('?verb=ListRecords&metadataPrefix=oai_dc&until=%s' % dates)
        self.assert_("homer" in body)
        # A bad until, in the past
        body = self._oai_get_method_and_validate('?verb=ListRecords&metadataPrefix=oai_dc&until=1998-01-15')
        self.assert_("noRecordsMatch")
        
        # A bad between, in the past
        body = self._oai_get_method_and_validate('?verb=ListRecords&metadataPrefix=oai_dc&from=1998-01-15&until=2000-01-15')
        self.assert_("noRecordsMatch" in body)
        # A good between
        dates = ((datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"), 
                 (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"))
        body = self._oai_get_method_and_validate('?verb=ListRecords&metadataPrefix=oai_dc&from=%s&until=%s' % dates)
        self.assert_("homer" in body)
        # A bad between, in the future
        dates = ((datetime.now() + timedelta(days=3)).strftime("%Y-%m-%d"), 
                 (datetime.now() + timedelta(days=4)).strftime("%Y-%m-%d"))
        body = self._oai_get_method_and_validate('?verb=ListRecords&metadataPrefix=oai_dc&from=%s&until=%s' % dates)
        self.assert_("noRecordsMatch" in body)




    def test_list_identifiers(self):
        body = self._oai_get_method_and_validate('?verb=ListIdentifiers&metadataPrefix=oai_dc&set=roger')
        body = self._oai_get_method_and_validate('?verb=ListIdentifiers&metadataPrefix=oai_dc')
        self.assert_("homer" in body)
        body = self._oai_get_method_and_validate('?verb=ListIdentifiers&metadataPrefix=oai_dc&set=foo')
        self.assert_("noRecordsMatch" in body)
        body = self._oai_get_method_and_validate('?verb=ListIdentifiers&metadataPrefix=oai_dc&set=roger&from=1992-01-15')
        self.assert_("homer" in body)
        body = self._oai_get_method_and_validate('?verb=ListIdentifiers&metadataPrefix=oai_dc&until=1992-01-15')
        self.assert_("noRecordsMatch")
        body = self._oai_get_method_and_validate('?verb=ListIdentifiers&metadataPrefix=oai_dc&from=1998-01-15&until=2000-01-15')
        self.assert_("noRecordsMatch")

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
        offset =  self.base_url + '?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc' % ids.next().identifier()
        res = self.app.get(offset)
        self.assert_(oaischema.validate(etree.fromstring(res.body)))
        self.assert_("abraham" in res.body)

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
            res = self._oai_get_method_and_validate('?verb=ListSets')
            self._second = False
            self._first = True
            ret = StringIO(res)
            return ret

    def _create_harvester(self):
        urllib2.urlopen = mock.Mock(side_effect=self._alternate_returns)
        harvest_job, harv = self._create_harvester_info()
        harvest_obj_list = harv.gather_stage(harvest_job)
        harvest_object = HarvestObject.get(harvest_obj_list[0])
        res = self._oai_get_method_and_validate('?verb=ListRecords&metadataPrefix=oai_dc&set=roger')
        # remove the resumption token
        res = res.replace('<resumptionToken>cursor%3D10%26set%3Droger%26metadataPrefix%3Doai_dc</resumptionToken>','')
        urllib2.urlopen = mock.Mock(return_value=StringIO(res))
        harv.fetch_stage(harvest_object)
        return harvest_object, harv

    def test_harvester_import(self, mocked=True):
        harvest_object, harv = self._create_harvester()
        real_content = json.loads(harvest_object.content)
        self.assert_(real_content)
        self.assert_(harv.import_stage(harvest_object))

        the_package = Session.query(Package).filter(Package.title == u"homer")
        the_package = the_package[0]
        self.assert_(the_package)
        self.assert_(len(the_package.get_tags()) == 4)
        self.assert_(len(the_package.get_groups()) == 3)
        self.assert_(the_package.url == "http://helda.helsinki.fi/oai/request?verb=getRecord&identifier=%s&metadataPrefix=oai_dc" % the_package.id)

