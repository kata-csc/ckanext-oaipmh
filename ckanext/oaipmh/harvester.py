'''
Harvester for OAI-PMH interfaces.
'''
#pylint: disable-msg=E1101,E0611,F0401
import logging
import json
import unicodedata
import string

from ckan.model import Session, Package, Resource, Group, Member
from ckan.plugins.core import SingletonPlugin, implements
from ckan import model

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject, HarvestJob
from ckan.model.authz import setup_default_user_roles
import oaipmh
from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
from oaipmh.error import NoRecordsMatchError, BadVerbError

log = logging.getLogger(__name__)


class OAIPMHHarvester(SingletonPlugin):
    '''
    OAI-PMH Harvester for ckanext-harvester.
    '''
    implements(IHarvester)

    config = None

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
        else:
            self.config = {}

    def info(self):
        '''
        Harvesting implementations must provide this method, which will return a
        dictionary containing different descriptors of the harvester. The
        returned dictionary should contain:

        * name: machine-readable name. This will be the value stored in the
          database, and the one used by ckanext-harvest to call the appropiate
          harvester.
        * title: human-readable name. This will appear in the form's select box
          in the WUI.
        * description: a small description of what the harvester does. This will
          appear on the form as a guidance to the user.
        * form_config_interface [optional]: Harvesters willing to store configuration
          values in the database must provide this key. The only supported value is
          'Text'. This will enable the configuration text box in the form. See also
          the ``validate_config`` method.

        A complete example may be::

            {
                'name': 'csw',
                'title': 'CSW Server',
                'description': 'A server that implements OGC\'s Catalog Service
                                for the Web (CSW) standard'
            }

        returns: A dictionary with the harvester descriptors
        '''
        return {
                'name': 'OAI-PMH',
                'title': 'OAI-PMH',
                'description': 'A server which has a OAI-PMH interface available.'
                }

    def gather_stage(self, harvest_job):
        '''
        The gather stage will recieve a HarvestJob object and will be
        responsible for:
            - gathering all the necessary objects to fetch on a later.
              stage (e.g. for a CSW server, perform a GetRecords request)
            - creating the necessary HarvestObjects in the database, specifying
              the guid and a reference to its source and job.
            - creating and storing any suitable HarvestGatherErrors that may
              occur.
            - returning a list with all the ids of the created HarvestObjects.

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        '''
        self._set_config(harvest_job.source.config)
        sets = []
        harvest_objs = []
        registry = MetadataRegistry()
        registry.registerReader('oai_dc', oai_dc_reader)
        client = oaipmh.client.Client(harvest_job.source.url, registry)
        identifier = client.identify()
        domain = identifier.repositoryName()
        group = Group.by_name(domain)
        if not group:
            group = Group(name=domain, description=domain)
        query = self.config['query'] if 'query' in self.config else ''
        for set in client.listSets():
            identifier, name, _ = set
            if 'query' in self.config:
                if query in name:
                    sets.append((identifier, name))
            else:
                sets.append((identifier, name))
        ids = []
        for set_id, set_name in sets:
            harvest_obj = HarvestObject(job=harvest_job)
            harvest_obj.content = json.dumps(
                                             {
                                              'set': set_id, \
                                              'set_name': set_name, \
                                              'domain': domain
                                              }
                                             )
            harvest_obj.save()
            harvest_objs.append(harvest_obj.id)
        model.repo.commit()
        return harvest_objs

    def fetch_stage(self, harvest_object):
        '''
        The fetch stage will receive a HarvestObject object and will be
        responsible for:
            - getting the contents of the remote object (e.g. for a CSW server,
              perform a GetRecordById request).
            - saving the content in the provided HarvestObject.
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''
        sets = json.loads(harvest_object.content)
        registry = MetadataRegistry()
        registry.registerReader('oai_dc', oai_dc_reader)
        client = oaipmh.client.Client(harvest_object.job.source.url, registry)
        records = []
        recs = []
        try:
            recs = client.listRecords(metadataPrefix='oai_dc', set=sets['set'])
        except:
            pass
        for rec in recs:
            header, metadata, _ = rec
            if metadata:
                records.append((header.identifier(), metadata.getMap(), None))
        if len(records):
            sets['records'] = records
            harvest_object.content = json.dumps(sets)
        else:
            return False
        return True

    def import_stage(self, harvest_object):
        '''
        The import stage will receive a HarvestObject object and will be
        responsible for:
            - performing any necessary action with the fetched object (e.g
              create a CKAN package).
              Note: if this stage creates or updates a package, a reference
              to the package must be added to the HarvestObject.
              Additionally, the HarvestObject must be flagged as current.
            - creating the HarvestObject - Package relation (if necessary)
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''
        model.repo.new_revision()
        master_data = json.loads(harvest_object.content)
        domain = master_data['domain']
        group = Group.get(domain)
        if not group:
            group = Group(name=domain, description=domain)
        if 'records' in master_data:
            records = master_data['records']
            set_name = master_data['set_name']
            for rec in records:
                identifier, metadata, _ = rec
                if metadata:
                    name = metadata['title'][0] if len(metadata['title']) else identifier
                    title = name
                    norm_title = unicodedata.normalize('NFKD', name)\
                                 .encode('ASCII', 'ignore')\
                                 .lower().replace(' ','_')[:35]
                    slug = ''.join(e for e in norm_title if e in string.ascii_letters+'_')
                    name = slug
                    creator = metadata['creator'][0] if len(metadata['creator']) else ''
                    description = metadata['description'][0] if len(metadata['description']) else ''
                    pkg = Package.by_name(name)
                    if not pkg:
                        pkg = Package(name=name, title=title)
                    extras = {}
                    for met in metadata.items():
                        key, value = met
                        if len(value) > 0:
                            if key == 'subject' or key == 'type':
                                for tag in value:
                                    if tag:
                                        tag = tag[:100]
                                        tag_obj = model.Tag.by_name(tag)
                                        if not tag_obj:
                                            tag_obj = model.Tag(name = tag)
                                        if tag_obj:
                                            pkgtag = model.PackageTag(tag = tag_obj, package = pkg)
                                            Session.add(tag_obj)
                                            Session.add(pkgtag)
                            else:
                                extras[key] = ' '.join(value)
                    pkg.author = creator
                    pkg.author_email = creator
                    pkg.title = title
                    pkg.notes = description
                    pkg.extras = extras
                    pkg.url = "%s?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc" % (harvest_object.job.source.url,
                                                      identifier)
                    pkg.save()
                    setup_default_user_roles(pkg)
                    url = ''
                    for ids in metadata['identifier']:
                        if ids.startswith('http://'):
                            url = ids
                    title = metadata['title'][0] if len(metadata['title']) else ''
                    description = metadata['description'][0] if len(metadata['description']) else ''
                    pkg.add_resource(url, description=description, name=title)
                    group.add_package_by_name(pkg.name)
                    subg_name = "%s - %s" % (domain, set_name)
                    subgroup = Group.by_name(subg_name)
                    if not subgroup:
                        subgroup = Group(name = subg_name, description = subg_name)
                    subgroup.add_package_by_name(pkg.name)
                    Session.add(group)
                    Session.add(subgroup)
                    setup_default_user_roles(group)
                    setup_default_user_roles(subgroup)
            model.repo.commit()
        else:
            return False
        return True
