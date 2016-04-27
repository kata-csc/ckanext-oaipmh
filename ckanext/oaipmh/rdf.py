import httplib
import json
import logging
import urllib2
from lxml import etree
import oaipmh
from ckanext.kata.utils import get_package_id_by_pid
from ckanext.oaipmh import importformats
from ckanext.oaipmh.cmdi_reader import CmdiReader
from ckanext.oaipmh.harvester import OAIPMHHarvester

log = logging.getLogger(__name__)


class RDFHarvester(OAIPMHHarvester):
    md_format = 'etsin_rdf'    ## TODO: is this ok?
    client = None  # used for testing

    def info(self):
        ''' See ;meth:`ckanext.harvest.harvesters.base.HarvesterBase.info`. '''

        return {
            'name': 'rdf',
            'title': 'RDF',
            'description': 'Harvests RDF dataset'
        }

    def get_schema(self, config, pkg):
        from ckanext.kata.plugin import KataPlugin
        return KataPlugin.create_package_schema_etsin_rdf()

    def on_deleted(self, harvest_object, header):
        """ See :meth:`OAIPMHHarvester.on_deleted`
            Mark package for deletion.
        """
        package_id = get_package_id_by_pid(header.identifier(), 'metadata')
        if package_id:
            harvest_object.package_id = package_id
        harvest_object.content = None
        harvest_object.report_status = "deleted"
        harvest_object.save()
        return True

    def gather_stage(self, harvest_job):
        """ See :meth:`OAIPMHHarvester.gather_stage`  """
        config = self._get_configuration(harvest_job)
        if not config.get('type'):
            config['type'] = 'rdf'
            harvest_job.source.config = json.dumps(config)
            harvest_job.source.save()
        registry = self.metadata_registry(config, harvest_job)
        client = self.client or oaipmh.client.Client(harvest_job.source.url, registry)
        return self.populate_harvest_job(harvest_job, None, config, client)

    def parse_xml(self, f, context, orig_url=None, strict=True):
        return RdfReader().read_data(etree.fromstring(f))
