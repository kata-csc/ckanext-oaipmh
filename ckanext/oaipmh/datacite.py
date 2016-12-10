import json
import logging
import oaipmh

from ckan import model
from ckanext.oaipmh.harvester import OAIPMHHarvester

log = logging.getLogger(__name__)

class DataCiteHarvester(OAIPMHHarvester):
    md_format = 'oai_datacite3' # 'cmdi0571'
    client = None  # used for testing

    def info(self):
        ''' See ;meth:`ckanext.harvest.harvesters.base.HarvesterBase.info`. '''

        return {
            'name': 'datacite',
            'title': 'OAI-PMH DataCite',
            'description': 'Harvests DataCite v.3.0 datasets'
        }


    def get_schema(self):
        from ckanext.kata.plugin import KataPlugin
        return KataPlugin.create_package_schema_oai_datacite()


    def import_stage(self, harvest_object):
        '''
        The import stage will receive a HarvestObject object and will be
        responsible for:
        - performing any necessary action with the fetched object (e.g create a CKAN package).
        Note: if this stage creates or updates a package, a reference
        to the package should be added to the HarvestObject.
        - creating the HarvestObject
        - Package relation (if necessary)
        - creating and storing any suitable HarvestObjectErrors that may occur.
        - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.report_status == "deleted":
            if harvest_object.package_id:
                get_action('package_delete')({'model': model, 'session': model.Session, 'user': 'harvest'}, {'id': harvest_object.package_id})
                return True
            return True

        if not harvest_object.content:
            self._save_object_error('Import: Empty content for object {id}'.format(
                id=harvest_object.id), harvest_object)

            return False

        content = json.loads(harvest_object.content)


        package_dict = content.pop('unified')

        try:
            package = model.Package.get(harvest_object.harvest_source_id)
            if package and package.owner_org:
                package_dict['owner_org'] = package.owner_org

            schema = self.get_schema()
            result = self._create_or_update_package(package_dict,
                                                    harvest_object,
                                                    schema=schema,
                                                    )

        except Exception as e:
            import traceback
            traceback.print_exc()
            self._save_object_error('Import: Could not create {id}. {e}'.format(
                id=harvest_object.id, e=e), harvest_object)
            return False

        return result
