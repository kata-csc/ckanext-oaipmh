'''
Harvester for OAI-PMH interfaces.
'''
#pylint: disable-msg=E1101,E0611,F0401
import logging
import json
import unicodedata
import string
import urllib2
import urllib
import datetime
import sys

from ckan.model import Session, Package, Group
from ckan import model

from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObject, HarvestJob
from ckan.model.authz import setup_default_user_roles
from ckan.controllers.storage import BUCKET, get_ofs
from ckan.lib import helpers as h
from pylons import config

import oaipmh.client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
from oaipmh.error import NoSetHierarchyError, NoRecordsMatchError

from ckanext.harvest.harvesters.retry import HarvesterRetry
from ckanext.kata.dataconverter import oai_dc2ckan

log = logging.getLogger(__name__)

import socket
socket.setdefaulttimeout(30)

import traceback
import random
random.seed()

class OAIPMHHarvester(HarvesterBase):
    '''
    OAI-PMH Harvester for ckanext-harvester.
    '''

    config = None

    metadata_prefix_key = 'metadataPrefix'
    metadata_prefix_value = 'oai_dc'

    def _set_config(self, config_str):
        '''Set the configuration string.
        '''
        if config_str:
            self.config = json.loads(config_str)
        else:
            self.config = {}

    def info(self):
        '''
        Return information about this harvester.
        '''
        return {
                'name': 'OAI-PMH',
                'title': 'OAI-PMH',
                'description': 'A server which has a OAI-PMH interface available.'
                }


    def _datetime_from_str(self, s):
        # Used to get date from settings file when testing harvesting with
        # (semi-open) date interval.
        if s == None:
            return s
        try:
            t = datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S')
            return t
        except ValueError:
            pass
        try:
            t = datetime.datetime.strptime(s, '%Y-%m-%d')
            return t
        except ValueError:
            log.debug('Bad date for %s: %s' % (key, s,))
        return None

    def _str_from_datetime(self, dt):
        return dt.strftime('%Y-%m-%dT%H:%M:%S')

    def _add_retry(self, harvest_object):
        HarvesterRetry.mark_for_retry(harvest_object)

    def _scan_retries(self, harvest_job):
        self._retry = HarvesterRetry()
        ident2obj = {}
        ident2set = {}
        for harvest_object in self._retry.find_all_retries(harvest_job):
            data = json.loads(harvest_object.content)
            if data['fetch_type'] == 'record':
                ident2obj[data['record']] = harvest_object
            elif data['fetch_type'] == 'set':
                ident2set[data['set_name']] = harvest_object
            else:
                # This should not happen...
                log.debug('Unknown retry fetch type: %s' % data['fetch_type'])
        return (ident2obj, ident2set,)

    def _clear_retries(self):
        self._retry.clear_retry_marks()

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
        model.repo.new_revision()
        self._set_config(harvest_job.source.config)
        def date_from_config(key):
            return self._datetime_from_str(config.get(key, None))
        from_ = date_from_config('ckanext.harvest.test.from')
        until = date_from_config('ckanext.harvest.test.until')
        previous_job = Session.query(HarvestJob) \
            .filter(HarvestJob.source==harvest_job.source) \
            .filter(HarvestJob.gather_finished!=None) \
            .filter(HarvestJob.id!=harvest_job.id) \
            .order_by(HarvestJob.gather_finished.desc()).limit(1).first()
        # Settings for debugging override old existing value.
        if previous_job and not from_ and not until:
            from_ = previous_job.gather_started
        from_until = {}
        if from_:
            from_until['from_'] = from_
        if until:
            from_until['until'] = until
        registry = MetadataRegistry()
        registry.registerReader(self.metadata_prefix_value, oai_dc_reader)
        client = oaipmh.client.Client(harvest_job.source.url, registry)
        try:
            identifier = client.identify()
        except urllib2.URLError:
            self._save_gather_error('Could not gather anything from %s!' %
                                    harvest_job.source.url, harvest_job)
            return None
        except socket.error:
            errno, errstr = sys.exc_info()[:2]
            self._save_object_error('Socket error OAI-PMH %s, details:\n%s' % (errno, errstr))
            return None
        #query = self.config['query'] if 'query' in self.config else ''
        harvest_objs = []
        # Get things to retry.
        ident2rec, ident2set = self._scan_retries(harvest_job)
        # Create a new harvest object that links to this job for every object.
        for ident, harv in ident2rec.items():
            info = json.loads(harv.content)
            harvest_obj = HarvestObject(job=harvest_job)
            harvest_obj.content = json.dumps(info)
            harvest_obj.save()
            harvest_objs.append(harvest_obj.id)
            log.debug('Retrying record: %s' % harv.id)
        try:
            domain = identifier.repositoryName()
            args = { self.metadata_prefix_key:self.metadata_prefix_value }
            args.update(from_until)
            for ident in client.listIdentifiers(**args):
                if ident.identifier() in ident2rec:
                    continue # On our retry list already, do not fetch twice.
                harvest_obj = HarvestObject(job=harvest_job)
                info = {
                    'fetch_type':'record',
                    'record':ident.identifier(),
                    'domain':domain}
                harvest_obj.content = json.dumps(info)
                harvest_obj.save()
                harvest_objs.append(harvest_obj.id)
        except NoRecordsMatchError as e:
            log.debug('No records matched: %s' % domain)
            pass # Ok. Just nothing to get.
        except Exception as e:
            # Todo: handle exceptions better.
            log.debug(traceback.format_exc(e))
            self._save_gather_error(
                'Could not fetch identifier list.', harvest_job)
            return None
        log.info('Gathered %i records from %s.' % (len(harvest_objs), domain,))
        # Gathering the set list here. Member identifiers in fetch.
        group = Group.by_name(domain)
        if not group:
            group = Group(name=domain, description=domain)
            setup_default_user_roles(group)
            group.save()
        def update_until(info, from_until):
            if 'until' not in info:
                return # Wanted up to current time earlier.
            if 'until' not in from_until:
                del info['until'] # Want up to current time now.
                return
            fu = self._str_from_datetime(from_until['until'])
            if info['until'] < fu: # Keep latest date from the two alternatives.
                info['until'] = fu
        def store_times(info, from_until):
            if 'from_' in from_until:
                info['from_'] = self._str_from_datetime(from_until['from_'])
            if 'until' in from_until:
                info['until'] = self._str_from_datetime(from_until['until'])
        # Add sets to retry first.
        for name, obj in ident2set.items():
            info = json.loads(obj.content)
            update_until(info, from_until)
            harvest_obj = HarvestObject(job=harvest_job)
            harvest_obj.content = json.dumps(info)
            harvest_obj.save()
            harvest_objs.append(harvest_obj.id)
            log.debug('Retrying set: %s' % obj.id)
        sets = []
        try:
            for set_ in client.listSets():
                identifier, name, _ = set_
                if name in ident2set:
                    continue # Set is already due for retry.
                sets.append((identifier, name,))
        except NoSetHierarchyError:
            # Is this an actual error or just a feature of the source?
            log.debug('No sets: %s' % domain)
            #self._save_gather_error('No set hierarchy.', harvest_job)
        for set_id, set_name in sets:
            harvest_obj = HarvestObject(job=harvest_job)
            info = { 'fetch_type':'set', 'set': set_id, 'set_name': set_name,
                'domain': domain, }
            store_times(info, from_until)
            harvest_obj.content = json.dumps(info)
            harvest_obj.save()
            harvest_objs.append(harvest_obj.id)
        self._clear_retries()
        model.repo.commit()
        log.info(
            'Gathered %i records/sets from %s.' % (len(harvest_objs), domain,))
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
        # Do common tasks and then call different methods depending on what
        # kind of info the harvest object contains.
        ident = json.loads(harvest_object.content)
        registry = MetadataRegistry()
        registry.registerReader(self.metadata_prefix_value, oai_dc_reader)
        client = oaipmh.client.Client(harvest_object.job.source.url, registry)
        try:
            if ident['fetch_type'] == 'record':
                return self._fetch_record(harvest_object, ident, client)
            if ident['fetch_type'] == 'set':
                return self._fetch_set(harvest_object, ident, client)
            # This should not happen...
            log.debug('Unknown fetch type: %s' % ident['fetch_type'])
        except Exception as e:
            # Guard against miscellaneous stuff. Probably plain bugs.
            log.debug(traceback.format_exc(e))
        return False

    def _fetch_record(self, harvest_object, ident, client):
        try:
            header, metadata, _ = client.getRecord(
                metadataPrefix=self.metadata_prefix_value,
                identifier=ident['record'])
        except socket.error:
            errno, errstr = sys.exc_info()[:2]
            self._save_object_error('Socket error OAI-PMH %s, details:\n%s' % (errno, errstr))
            self._add_retry(harvest_object)
            return False
        except urllib2.URLError:
            self._save_gather_error('Failed to fetch record.')
            self._add_retry(harvest_object)
            return False
        if not metadata:
            # Assume that there is no metadata and not an error.
            return False
        ident['record'] = ( header.identifier(), metadata.getMap(), )
        harvest_object.content = json.dumps(ident)
        return True

    def _fetch_set(self, harvest_object, ident, client):
        args = { self.metadata_prefix_key:self.metadata_prefix_value,
            'set':ident['set'] }
        if 'from_' in ident:
            args['from_'] = self._datetime_from_str(ident['from_'])
        if 'until' in ident:
            args['until'] = self._datetime_from_str(ident['until'])
        ids = []
        try:
            for identity in client.listIdentifiers(**args):
                ids.append(identity.identifier())
        except NoRecordsMatchError:
            return False # Ok, empty set. Nothing to do.
        except socket.error:
            errno, errstr = sys.exc_info()[:2]
            self._save_object_error('Socket error OAI-PMH %s, details:\n%s' %
                (errno, errstr,))
            self._add_retry(harvest_object)
            return False
        ident['record_ids'] = ids
        harvest_object.content = json.dumps(ident)
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
        try:
            data = json.loads(harvest_object.content)
            domain = data['domain']
            group = Group.get(domain) # Checked in gather_stage so exists.
            if data['fetch_type'] == 'record':
                return self._import_record(harvest_object, data, group)
            if data['fetch_type'] == 'set':
                return self._import_set(harvest_object, data, group)
            log.debug('Unknown fetch_type in import: %s' % data['fetch_type'])
        except Exception as e:
            log.debug(traceback.format_exc(e))
        return False

    def _package_name_from_identifier(self, identifier):
        return urllib.quote_plus(urllib.quote_plus(identifier))

    def _import_record(self, harvest_object, master_data, group):
        # Gather all relevant information into a dictionary.
        data = {}
        data['identifier'] = master_data['record'][0]
        data['metadata'] = master_data['record'][1]
        data['package_name'] = self._package_name_from_identifier(data['identifier'])
        data['package_url'] = '%s?verb=GetRecord&identifier=%s&%s=%s' % (
            harvest_object.job.source.url, data['identifier'],
            self.metadata_prefix_key, self.metadata_prefix_value,)
        # Should failure with metadata be considered grounds for retry?
        # This should fetch the data into the dictionary and not create a file.
        try:
            ofs = get_ofs()
            nowstr = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')
            label = '%s/%s.xml' % (nowstr, data['identifier'])
            f = urllib2.urlopen(data['package_url'])
            ofs.put_stream(BUCKET, label, f, {})
            fileurl = config.get('ckan.site_url') + h.url_for('storage_file', label=label)
            # This could be a list of dictionaries in case there are more.
            data['package_resource'] = { 'url':fileurl,
                'description':'Original metadata record',
                'format':'xml', 'size':len(f.read()) }
        except urllib2.HTTPError:
            self._save_object_error('Could not get original metadata record!',
                                    harvest_object, stage='Import')
            self._add_retry(harvest_object)
            return False
        except socket.error:
            errno, errstr = sys.exc_info()[:2]
            self._save_object_error(
                'Socket error original metadata record %s, details:\n%s' %
                    (errno, errstr), harvest_object, stage='Import')
            self._add_retry(harvest_object)
            return False
        harvest_object.content = '' # Clear now of useless record data.
        return oai_dc2ckan(data, group, harvest_object)

    def _import_set(self, harvest_object, master_data, group):
        model.repo.new_revision()
        subg_name = '%s - %s' % (group.name, master_data['set_name'],)
        subgroup = Group.by_name(subg_name)
        if not subgroup:
            subgroup = Group(name=subg_name, description=subg_name)
            setup_default_user_roles(subgroup)
            subgroup.save()
        for ident in master_data['record_ids']:
            pkg_name = self._package_name_from_identifier(ident)
            # Package may have been omitted due to missing metadata.
            pkg = Package.get(pkg_name)
            if pkg:
                subgroup.add_package_by_name(pkg_name)
                subgroup.save()
        harvest_object.content = '' # Clear list.
        model.repo.commit()
        return True

