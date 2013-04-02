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
from ckan.lib.munge import  munge_tag
from ckanext.harvest.model import HarvestObject, HarvestJob
from ckan.model.authz import setup_default_user_roles
from ckan.controllers.storage import BUCKET, get_ofs
from ckan.lib import helpers as h
from pylons import config

import oaipmh.client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
from oaipmh.error import NoSetHierarchyError
from oaipmh.error import NoRecordsMatchError

from ckanext.harvest.harvesters.retry import HarvesterRetry

log = logging.getLogger(__name__)

import socket
socket.setdefaulttimeout(30)

import traceback

class OAIPMHHarvester(HarvesterBase):
    '''
    OAI-PMH Harvester for ckanext-harvester.
    '''

    config = None
    incremental = None

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

    def _add_retry(self, harvest_object):
        HarvesterRetry.mark_for_retry(harvest_object)

    def _scan_retries(self, harvest_job):
        self._retry = HarvesterRetry()
        ident2obj = {}
        ident2set = {}
        for harvest_object in self._retry.find_all_retries(harvest_job):
            data = json.loads(harvest_object.content)
            if data['fetch_type'] == 'record':
                ident2obj[data['ident']] = None # Not needed now.
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
        from_ = date_from_config('ckan.test.harvest.from')
        until = date_from_config('ckan.test.harvest.until')
        previous_job = Session.query(HarvestJob) \
            .filter(HarvestJob.source==harvest_job.source) \
            .filter(HarvestJob.gather_finished!=None) \
            .filter(HarvestJob.id!=harvest_job.id) \
            .order_by(HarvestJob.gather_finished.desc()).limit(1).first()
        # Settings for debugging override old existing value.
        if previous_job and not from_ and not until:
            self.incremental = True
            from_ = previous_job.gather_started
        registry = MetadataRegistry()
        registry.registerReader('oai_dc', oai_dc_reader)
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
        domain = identifier.repositoryName()
        harvest_objs = []
        args = { 'metadataPrefix':'oai_dc' }
        # Get things to retry.
        ident2rec, ident2set = self._scan_retries(harvest_job)
        # Create a new harvest object that links to this job for every object.
        for ident in ident2rec.keys():
            harvest_obj = HarvestObject(job=harvest_job)
            harvest_obj.content = json.dumps({
                'fetch_type':'record',
                'record':ident,
                'metadataPrefix':args['metadataPrefix'],
                'domain':domain})
            harvest_obj.save()
            harvest_objs.append(harvest_obj.id)
        try:
            if from_ != None:
                args['from_'] = from_
            if until:
                args['until'] = until
            for ident in client.listIdentifiers(**args):
                if ident.identifier() in ident2rec:
                    continue # On our retry list already, do not fetch twice.
                harvest_obj = HarvestObject(job=harvest_job)
                harvest_obj.content = json.dumps({
                    'fetch_type':'record',
                    'record':ident.identifier(),
                    'metadataPrefix':args['metadataPrefix'],
                    'domain':domain})
                harvest_obj.save()
                harvest_objs.append(harvest_obj.id)
        except NoRecordsMatchError:
            pass # Ok. Just nothing to get.
        except Exception as e:
            # Todo: handle exceptions better.
            log.debug(traceback.format_exc(e))
            self._save_gather_error(
                'Could not fetch identifier list.', harvest_job)
            return None
        log.info(
            'Gathered %i records from %s.' % (len(harvest_objs), domain,))
        # Gathering the set list here. Member identifiers in fetch.
        group = Group.by_name(domain)
        if not group:
            group = Group(name=domain, description=domain)
            setup_default_user_roles(group)
        Session.add(group)
        sets = []
        # Add sets to retry first.
        for name, obj in ident2set.items():
            data = json.loads(obj.content)
            sets.append((data['set_id'], name,))
        try:
            for set_ in client.listSets():
                identifier, name, _ = set_
                if name in ident2set:
                    continue # Set is already due for retry.
                sets.append((identifier, name,))
        except NoSetHierarchyError:
            # Is this an actual error or just a feature?
            self._save_gather_error('No set hierarchy.', harvest_job)
        for set_id, set_name in sets:
            harvest_obj = HarvestObject(job=harvest_job)
            info = { 'fetch_type':'set', 'set': set_id, 'set_name': set_name,
                'metadataPrefix':'oai_dc', 'domain': domain, }
            if from_:
                info['from_'] = from_.strftime('%Y-%m-%dT%H:%M:%S')
            if until:
                info['until'] = until.strftime('%Y-%m-%dT%H:%M:%S')
            harvest_obj.content = json.dumps(info)
            harvest_obj.save()
            harvest_objs.append(harvest_obj.id)
        model.repo.commit()
        self._clear_retries()
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
        registry.registerReader(ident['metadataPrefix'], oai_dc_reader)
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
            #self._save_gather_error(traceback.format_exc(e), harvest_obj)
            log.debug(traceback.format_exc(e))
        return False

    def _fetch_record(self, harvest_object, ident, client):
        try:
            header, metadata, _ = client.getRecord(
                metadataPrefix=ident['metadataPrefix'],
                identifier=ident['record'])
        except socket.error:
            errno, errstr = sys.exc_info()[:2]
            self._save_object_error('Socket error OAI-PMH %s, details:\n%s' % (errno, errstr))
            self._add_retry(harvest_object)
            return False
        if not metadata:
            # Assume that there is no metadata and not an error.
            return False
        ident['record'] = ( header.identifier(), metadata.getMap(), )
        harvest_object.content = json.dumps(ident)
        return True

    def _fetch_set(self, harvest_object, ident, client):
        args = { 'metadataPrefix':ident['metadataPrefix'], 'set':ident['set'] }
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
        identifier = master_data['record'][0]
        metadata = master_data['record'][1]
        title = metadata['title'][0] if len(metadata['title']) else identifier
        description = metadata['description'][0] if len(metadata['description']) else ''
        name = self._package_name_from_identifier(identifier)
        pkg = Package.get(name)
        if not pkg:
            pkg = Package(name=name, title=title, id=identifier)
        extras = {}
        lastidx = 0
        for met in metadata.items():
            key, value = met
            if len(value) == 0:
                continue
            if key == 'subject' or key == 'type':
                for tag in value:
                    if not tag:
                        continue
                    for tagi in tag.split(','):
                        tagi = tagi.strip()
                        tagi = munge_tag(tagi[:100])
                        tag_obj = model.Tag.by_name(tagi)
                        if not tag_obj:
                            tag_obj = model.Tag(name=tagi)
                        else:
                            pkgtag = model.PackageTag(tag=tag_obj, package=pkg)
                            Session.add(tag_obj)
                            Session.add(pkgtag)
            elif key == 'creator' or key == 'contributor':
                for auth in value:
                    extras['organization_%d' % lastidx] = ''
                    extras['author_%d' % lastidx] = auth
                    lastidx += 1
            elif key != 'title':
                extras[key] = ' '.join(value)
        pkg.title = title
        pkg.notes = description
        extras['lastmod'] = extras['date']
        pkg.extras = extras
        pkg.url = '%s?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc'\
                    % (harvest_object.job.source.url, identifier)
        pkg.save()
        ofs = get_ofs()
        nowstr = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')
        label = '%s/%s.xml' % (nowstr, identifier)
        # Should failure with metadata be considered grounds for retry?
        try:
            f = urllib2.urlopen(pkg.url)
            ofs.put_stream(BUCKET, label, f, {})
            fileurl = config.get('ckan.site_url') + h.url_for('storage_file', label=label)
            pkg.add_resource(url=fileurl,
                description='Original metadata record',
                format='xml', size=len(f.read()))
        except urllib2.HTTPError:
            self._save_object_error('Could not get original metadata record!',
                                    harvest_object, stage='Import')
        except socket.error:
            errno, errstr = sys.exc_info()[:2]
            self._save_object_error(
                'Socket error original metadata record %s, details:\n%s' % (errno, errstr),
                harvest_object, stage='Import')
        harvest_object.package_id = pkg.id
        harvest_object.current = True
        harvest_object.save()
        Session.add(harvest_object)
        setup_default_user_roles(pkg)
        title = metadata['title'][0] if len(metadata['title']) else ''
        description = metadata['description'][0]\
                        if len(metadata['description']) else ''
        url = ''
        for ids in metadata['identifier']:
            if ids.startswith('http://'):
                url = ids
        if url != '':
            pkg.add_resource(url, description=description, name=title,
                format='html' if url.startswith('http://') else '')
        # All belong to the main group even if they do not belong to any set.
        group.add_package_by_name(pkg.name)
        model.repo.commit()
        return True

    def _import_set(self, harvest_object, master_data, group):
        subg_name = '%s - %s' % (group.name, master_data['set_name'],)
        subgroup = Group.by_name(subg_name)
        if not subgroup:
            subgroup = Group(name=subg_name, description=subg_name)
            setup_default_user_roles(subgroup)
        Session.add(subgroup)
        for ident in master_data['record_ids']:
            pkg_name = self._package_name_from_identifier(ident)
            # Package may have been omitted due to missing metadata.
            pkg = Package.get(pkg_name)
            if pkg:
                subgroup.add_package_by_name(pkg_name)
        return True

