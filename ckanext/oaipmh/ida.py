import json
from lxml import etree
from ckanext.oaipmh.harvester import OAIPMHHarvester
from ckanext.oaipmh.oai_dc_reader import dc_metadata_reader


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
            config['type'] = 'ida'
            harvest_job.source.config = json.dumps(config)
            harvest_job.source.save()
        return super(IdaHarvester, self).gather_stage(harvest_job)

    def parse_xml(self, f, context, orig_url=None, strict=True):
        metadata = dc_metadata_reader('ida')(etree.fromstring(f))
        return metadata['unified']
