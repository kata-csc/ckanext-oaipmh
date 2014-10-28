import json
from ckanext.oaipmh.harvester import OAIPMHHarvester


class IdaHarvester(OAIPMHHarvester):
    '''
    OAI-PMH Harvester
    '''
    md_format = "oai_dc"

    def info(self):
        ''' See :meth:`ckanext.harvest.harvesters.base.HarvesterBase.info`. '''
        return {
            'name': 'ida',
            'title': 'OAI-PMH IDA',
            'description': 'Harvests OAI-PMH IDA providers'
        }

    def gather_stage(self, harvest_job):
        """ See :meth:`OAIPMHHarvester.gather_stage`  """
        config = self._get_configuration(harvest_job)
        if not config.get('type'):
            config['type'] = 'cmdi'
            harvest_job.source.config = json.dumps(config)
            harvest_job.source.save()
