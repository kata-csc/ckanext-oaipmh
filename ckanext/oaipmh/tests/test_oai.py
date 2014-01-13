# coding: utf-8
#
# pylint: disable=no-self-use, missing-docstring, too-many-public-methods, invalid-name

"""
Test classes for OAI-PMH harvester.
"""

import ckan

from unittest import TestCase
from ckanext.oaipmh.harvester import OAIPMHHarvester
import ckanext.harvest.model as harvest_model
from ckanext.kata import model as kata_model


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

    # TODO: Sensible tests.

    @classmethod
    def teardown_class(cls):
        ckan.model.repo.rebuild_db()

