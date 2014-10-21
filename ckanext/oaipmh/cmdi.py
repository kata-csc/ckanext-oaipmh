import json
import oaipmh
from ckanext.oaipmh import importformats
from ckanext.oaipmh.harvester import OAIPMHHarvester


class CMDIHarvester(OAIPMHHarvester):
    md_format = 'cmdi0571'
    client = None  # used for testing

    def info(self):
        ''' See ;meth:`ckanext.harvest.harvesters.base.HarvesterBase.info`. '''

        return {
            'name': 'cmdi',
            'title': 'CMDI',
            'description': 'Harvests CMDI dataset'
        }

    def gather_stage(self, harvest_job):
        config = self._get_configuration(harvest_job)
        if not config.get('type'):
            config['type'] = 'cmdi'
            harvest_job.source.config = json.dumps(config)
            harvest_job.source.save()
        registry = importformats.create_metadata_registry(config['type'])
        client = self.client or oaipmh.client.Client(harvest_job.source.url, registry)
        return self.populate_harvest_job(harvest_job, None, config, client)
