# coding: utf-8
# vi:et:ts=8:
import httplib

import logging
import json
from itertools import islice
from lxml import etree
import urllib2
from pylons import config as c
from paste.deploy.converters import asbool

import oaipmh.client
import oaipmh.error
from dateutil.parser import parse as dp
from ckan.controllers.api import get_action
from ckanext.oaipmh.oai_dc_reader import dc_metadata_reader

import importformats

from ckan.model import Session, Package
from ckan.logic import NotFound, NotAuthorized, ValidationError
from ckan import model

from ckanext.harvest.model import HarvestJob, HarvestObject
from ckanext.harvest.harvesters.base import HarvesterBase
import ckanext.kata.utils
import ckanext.kata.plugin
import fnmatch
import re
import ckanext.kata.kata_ldap as ld
from ckanext.kata.utils import pid_to_name
from ckanext.kata.utils import generate_pid

log = logging.getLogger(__name__)


class OAIPMHHarvester(HarvesterBase):
    '''
    OAI-PMH Harvester
    '''
    md_format = "oai_dc"

    def _get_configuration(self, harvest_job):
        """ Parse configuration from given harvest object """
        configuration = {}
        if harvest_job.source.config:
            log.debug('Config: %s', harvest_job.source.config)
            try:
                configuration = json.loads(harvest_job.source.config)
            except ValueError as e:
                self._save_gather_error('Gather: Unable to decode config from: {c}, {e}'.
                                        format(e=e, c=harvest_job.source.config), harvest_job)
                raise
        return configuration

    def _recreate(self, harvest_object):
        """ Check if packages should be recreated or not.
            Default for IDA is false. For other true.
            Configuration parameter is `recreate`.
        """
        configuration = self._get_configuration(harvest_object)
        return configuration.get('recreate', configuration.get('type') != 'ida')

    def on_deleted(self, harvest_object, header):
        """ Called when metadata is deleted from server.
            Return False if dataset is ignored.
        """
        log.info("Metadata is deleted for %s. Ignoring.", harvest_object.guid)
        return False

    def metadata_registry(self, config, harvest_job):
        harvest_type = config.get('type', 'default')
        return importformats.create_metadata_registry(harvest_type, harvest_job.source.url)

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

        A complete example may be::

            {
                'name': 'csw',
                'title': 'CSW Server',
                'description': 'A server that implements OGC's Catalog Service
                                for the Web (CSW) standard'
            }

        :returns: A dictionary with the harvester descriptors
        '''
        return {
            'name': 'oai-pmh',
            'title': 'OAI-PMH DC',
            'description': 'Harvests OAI-PMH providers'
        }

    def validate_config(self, config):
        '''

        [optional]

        Harvesters can provide this method to validate the configuration entered in the
        form. It should return a single string, which will be stored in the database.
        Exceptions raised will be shown in the form's error messages.

        :param harvest_object_id: Config string coming from the form
        :returns: A string with the validated configuration options
        '''

        # TODO: Tests

        def validate_param(d, p, t):
            '''
            Check if 'p' is specified and is of type 't'
            '''
            if p in d and not isinstance(d[p], t):
                raise TypeError("'{p}' needs to be a '{t}'".format(t=t, p=p))
            return p in d

        def validate_date_param(d, p, t):
            '''
            Validate a date parameter by trying to parse it
            '''
            if validate_param(d, p, t):
                dp(d[p]).replace(tzinfo=None)

        # Todo: Write better try/except cases
        if config:
            dj = json.loads(config)
            validate_param(dj, 'set', list)
            validate_param(dj, 'limit', int)
            validate_param(dj, 'type', basestring)
            validate_date_param(dj, 'until', basestring)
            validate_date_param(dj, 'from', basestring)
        else:
            config = '{}'
        return config

    # def get_original_url(self, harvest_object_id):
    #     '''
    #
    #     [optional]
    #
    #     This optional but very recommended method allows harvesters to return
    #     the URL to the original remote document, given a Harvest Object id.
    #     Note that getting the harvest object you have access to its guid as
    #     well as the object source, which has the URL.
    #     This URL will be used on error reports to help publishers link to the
    #     original document that has the errors. If this method is not provided
    #     or no URL is returned, only a link to the local copy of the remote
    #     document will be shown.
    #
    #     Examples:
    #         * For a CKAN record: http://{ckan-instance}/api/rest/{guid}
    #         * For a WAF record: http://{waf-root}/{file-name}
    #         * For a CSW record: http://{csw-server}/?Request=GetElementById&Id={guid}&...
    #
    #     :param harvest_object_id: HarvestObject id
    #     :returns: A string with the URL to the original document
    #     '''

    def get_package_ids(self, set_ids, config, last_time, client):
        ''' Get package identifiers from given set identifiers.
        '''
        def filter_map_args(list_tuple):
            for key, value in list_tuple:
                if key in ['until', 'from']:
                    if key == 'from':
                        key = 'from_'
                    yield (key, dp(value).replace(tzinfo=None))

        kwargs = dict(filter_map_args(config.items()))
        kwargs['metadataPrefix'] = self.md_format
        if last_time and 'from_' not in kwargs:
            kwargs['from_'] = dp(last_time).replace(tzinfo=None)
        if set_ids:
            for set_id in set_ids:
                try:
                    for header in client.listIdentifiers(set=set_id, **kwargs):
                        yield header.identifier()
                except oaipmh.error.NoRecordsMatchError:
                    pass
        else:
            try:
                for header in client.listIdentifiers(**kwargs):
                    yield header.identifier()
            except oaipmh.error.NoRecordsMatchError:
                pass
                # package_ids = [header.identifier() for header in client.listRecords()]

    def gather_stage(self, harvest_job):
        '''
        The gather stage will receive a HarvestJob object and will be
        responsible for:
        - gathering all the necessary objects to fetch on a later.
        stage (e.g. for a CSW server, perform a GetRecords request)
        - creating the necessary HarvestObjects in the database, specifying
        the guid and a reference to its job. The HarvestObjects need a
        reference date with the last modified date for the resource, this
        may need to be set in a different stage depending on the type of
        source.
        - creating and storing any suitable HarvestGatherErrors that may
        occur.
        - returning a list with all the ids of the created HarvestObjects.

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        :type harvest_job: HarvestJob
        '''
        log.debug('Harvest source: %s', harvest_job.source.url)

        config = self._get_configuration(harvest_job)

        # Create a OAI-PMH Client
        registry = self.metadata_registry(config, harvest_job)
        client = oaipmh.client.Client(harvest_job.source.url, registry)

        available_sets = list(client.listSets())

        log.debug('available sets: %s', available_sets)

        set_ids = set()
        for set_id in config.get('set', []):
            if '*' in set_id:
                matcher = re.compile(fnmatch.translate(set_id))
                found = False
                for set_spec, _, _ in available_sets:
                    if matcher.match(set_spec):
                        set_ids.add(set_spec)
                        found = True

                if not found:
                    log.warning("No sets found with given wildcard string: %s", set_id)
            else:
                if not any(set_id in sets for sets in available_sets):
                    log.warning("Given set %s is not in available sets. Not removing.", set_id)
                set_ids.add(set_id)

        log.debug('Sets in config: %s', set_ids)
        return self.populate_harvest_job(harvest_job, set_ids, config, client)

    def populate_harvest_job(self, harvest_job, set_ids, config, client):
        # Check if this source has been harvested before
        previous_job = Session.query(HarvestJob) \
            .filter(HarvestJob.source == harvest_job.source) \
            .filter(HarvestJob.gather_finished != None) \
            .filter(HarvestJob.id != harvest_job.id) \
            .order_by(HarvestJob.gather_finished.desc()) \
            .limit(1).first()

        last_time = None
        if previous_job and previous_job.finished and model.Package.get(harvest_job.source.id).metadata_modified < previous_job.gather_started:
            last_time = previous_job.gather_started.isoformat()

        # Collect package ids
        package_ids = list(self.get_package_ids(set_ids, config, last_time, client))
        log.debug('Identifiers: %s', package_ids)

        # Ensure that IDA datasets are not reharvested unless "recreate == True".
        # TODO: Should this IDA specific part be somewhere else? Or can it be removed?
        if not self._recreate(harvest_job) and package_ids:
            converted_identifiers = {}
            for identifier in package_ids:
                converted_identifiers[pid_to_name(identifier)] = identifier
                if identifier.endswith(u'm'):
                    converted_identifiers[pid_to_name(u"%ss" % identifier[0:-1])] = identifier
            for package in model.Session.query(model.Package).filter(model.Package.name.in_(converted_identifiers.keys())).all():
                converted_name = package.name
                if converted_identifiers[converted_name] not in package_ids:
                    converted_name = "%sm" % converted_name[0:-1]
                package_ids.remove(converted_identifiers[converted_name])

        if previous_job:
            for previous_error in [error.guid for error in Session.query(HarvestObject).
                                   filter(HarvestObject.harvest_job_id == previous_job.id).
                                   filter(HarvestObject.state == 'ERROR').all()]:
                if previous_error not in package_ids:
                    package_ids.append(previous_error)

        try:
            object_ids = []
            if len(package_ids):
                for package_id in islice(package_ids, config['limit']) if 'limit' in config else package_ids:
                    # Create a new HarvestObject for this identifier
                    obj = HarvestObject(guid=package_id, job=harvest_job)
                    obj.save()
                    object_ids.append(obj.id)
                log.debug('Object ids: {i}'.format(i=object_ids))
                return object_ids
            else:
                self._save_gather_error('No packages received for URL: {u}'.format(
                    u=harvest_job.source.url), harvest_job)
                return None
        except Exception as e:
            self._save_gather_error('Gather: {e}'.format(e=e), harvest_job)
            raise

    def fetch_stage(self, harvest_object):
        '''
        The fetch stage will receive a HarvestObject object and will be
        responsible for:
        - getting the contents of the remote object (e.g. for a CSW server, perform a GetRecordById request).
        - saving the content in the provided HarvestObject.
        - creating and storing any suitable HarvestObjectErrors that may occur.
        - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''
        log.debug("fetch: %s", harvest_object.guid)
        # Get metadata content from provider
        try:
            # Create a OAI-PMH Client
            config = self._get_configuration(harvest_object)

            registry = self.metadata_registry(config, harvest_object)
            client = oaipmh.client.Client(harvest_object.job.source.url, registry)

            # Get source URL
            header, metadata, _about = client.getRecord(identifier=harvest_object.guid, metadataPrefix=self.md_format)
        except Exception as e:
            import traceback
            traceback.print_exc()
            self._save_object_error('Unable to get metadata from provider: {u}: {e}'.format(
                u=harvest_object.source.url, e=e), harvest_object)
            return False

        if header and header.isDeleted():
            return self.on_deleted(harvest_object, header)

        # Get contents
        try:
            content = json.dumps(metadata.getMap())
        except Exception as e:
            import traceback
            traceback.print_exc()
            self._save_object_error('Unable to get content for package: {u}: {e}'.format(
                u=harvest_object.source.url, e=e), harvest_object)
            return False

        # Save the fetched contents in the HarvestObject
        harvest_object.content = content
        harvest_object.save()

        return True

    def get_schema(self, config, pkg):
        if config.get('type', 'default') != 'ida':
            return ckanext.kata.plugin.KataPlugin.update_package_schema_oai_dc() if pkg \
                else ckanext.kata.plugin.KataPlugin.create_package_schema_oai_dc()
        else:
            return ckanext.kata.plugin.KataPlugin.update_package_schema_oai_dc_ida() if pkg \
                else ckanext.kata.plugin.KataPlugin.create_package_schema_oai_dc_ida()

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
        # import pprint; pprint.pprint(content)

        package_dict = content.pop('unified')
        package_dict['xpaths'] = content

        # If package exists use old PID, otherwise create new
        pkg_id = ckanext.kata.utils.get_package_id_by_primary_pid(package_dict)
        pkg = Session.query(Package).filter(Package.id == pkg_id).first() if pkg_id else None
        log.debug('Package: "{pkg}"'.format(pkg=pkg))

        if pkg and not self._recreate(harvest_object):
            log.debug("Not re-creating package: %s", pkg_id)
            return True
        if not package_dict.get('id', None):
            package_dict['id'] = pkg.id if pkg else generate_pid()

        uploader = ''

        try:
            package = model.Package.get(harvest_object.harvest_source_id)
            if package and package.owner_org:
                package_dict['owner_org'] = package.owner_org

            config = self._get_configuration(harvest_object)
            if config.get('type') == 'ida':
                if package_dict.get('owner_org', False):
                    package_dict['private'] = "true"
                uploader = package_dict.get('uploader', False)
                package_dict.pop('uploader')
            if config.get('type') == 'ida':
                package_dict['persist_schema'] = u'True'
            schema = self.get_schema(config, pkg)
            # schema['xpaths'] = [ignore_missing, ckanext.kata.converters.xpath_to_extras]
            # TODO: Can't use ckanext-harvest's function. Have to write own one.
            result = self._create_or_update_package(package_dict,
                                                    harvest_object,
                                                    schema=schema,
                                                    # s_schema=ckanext.kata.plugin.KataPlugin.show_package_schema()
                                                    )
            if uploader and asbool(c.get('kata.ldap.enabled', False)):
                try:
                    usr = ld.get_user_from_ldap(uploader)
                    if usr:
                        # by_openid leaves session hanging if usr is not set
                        usrname = model.User.by_openid(usr)
                    if usrname:
                        editor_dict = {"name": package_dict['name'],
                                       "role": "admin",
                                       "username": usrname.name
                                       }
                        context = {'model': model, 'session': model.Session,
                                   'user': 'harvest'}
                        try:
                            # if we fail the adding, no problem
                            ckanext.kata.actions.dataset_editor_add(context, editor_dict)
                        except ValidationError:
                            pass
                        except NotFound:
                            pass
                        except NotAuthorized:
                            pass
                except:
                    pass
        except Exception as e:
            import traceback
            traceback.print_exc()
            self._save_object_error('Import: Could not create {id}. {e}'.format(
                id=harvest_object.id, e=e), harvest_object)
            return False

        return result

    def parse_xml(self, f, context, orig_url=None, strict=True):
        """ Parse XML and return package data dictionary.

        :param f: data as string
        :param context: CKAN context
        :param orig_url: orgininal URL
        :param strict: No used here, required by caller
        :return: package dictionary (used for package creation)
        """
        metadata = dc_metadata_reader('default')(etree.fromstring(f))
        return metadata['unified']

    def fetch_xml(self, url, context):
        '''Get xml for import. Shortened from :meth:`fetch_stage`

        :param url: the url for metadata file
        :param type: string

        :return: a xml file
        :rtype: string
        '''
        try:
            log.debug('Requesting url {ur}'.format(ur=url))
            f = urllib2.urlopen(url).read()
            return self.parse_xml(f, context, url)
        except (urllib2.URLError, urllib2.HTTPError,):
            log.debug('fetch_xml: Could not fetch from url {ur}!'.format(ur=url))
        except httplib.BadStatusLine:
            log.debug('Bad HTTP response status line.')
