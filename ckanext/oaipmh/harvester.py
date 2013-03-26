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
        from_ = config.get("ckan.test.harvest.from", None)
        until = config.get("ckan.test.harvest.until", None)
        previous_jobs = Session.query(HarvestJob) \
            .filter(HarvestJob.source==harvest_job.source) \
            .filter(HarvestJob.gather_finished!=None) \
            .filter(HarvestJob.id!=harvest_job.id) \
            .order_by(HarvestJob.gather_finished.desc()).all()
        # Error object with foreign key to this should also be deleted.
        # Do deletion here or just elsewhere?
        #if len(previous_jobs) > 1:
        #    for job in previous_jobs[1:]:
        #        job.delete()
        previous_job = previous_jobs[0] if len(previous_jobs) > 0 else None
        if previous_job:
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
        #domain = identifier.repositoryName()
        #group = Group.by_name(domain)
        #if not group:
        #    group = Group(name=domain, description=domain)
        query = self.config['query'] if 'query' in self.config else ''
        harvest_objs = []
        try:
            args = { "metadataPrefix":"oai_dc" }
            if from_:
                args["from"] = from_
            if until:
                args["until"] = until
            for ident in client.listIdentifiers(**args):
                harvest_obj = HarvestObject(job=harvest_job)
                harvest_obj.content = json.dumps({
                    "record":ident.identifier(),
                    "metadataPrefix":"oai_dc"})
                harvest_obj.save()
                harvest_objs.append(harvest_obj.id)
                if len(harvest_objs) > 1000:
                    break
            model.repo.commit()
        except Exception as e:
            # Todo: handle exceptions better.
            log.debug(traceback.format_exc(e))
            self._save_gather_error(
                "Could not fetch identifier list.", harvest_job)
            raise
        log.info("Gathered %i records." % len(harvest_objs))
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
        log.debug("Enter fetch_stage %s" % harvest_object.id);
        ident = json.loads(harvest_object.content)
        registry = MetadataRegistry()
        registry.registerReader(ident["metadataPrefix"], oai_dc_reader)
        client = oaipmh.client.Client(harvest_object.job.source.url, registry)
        try:
            header, metadata, _ = client.getRecord(
                metadataPrefix=ident["metadataPrefix"],
                identifier=ident["record"])
        except socket.error:
            errno, errstr = sys.exc_info()[:2]
            self._save_object_error('Socket error OAI-PMH %s, details:\n%s' % (errno, errstr))
            return False
        except Exception as e:
            self._save_gather_error(traceback.format_exc(e), harvest_obj)
            raise
        if metadata:
            ident["record"] = [ header.identifier(), metadata.getMap() ]
            harvest_object.content = json.dumps(ident)
            return True
        return False

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
            log.debug("Enter import_stage %s" % harvest_object.id);
            model.repo.new_revision()
            master_data = json.loads(harvest_object.content)
            if 'record' not in master_data:
                self._save_object_error('Could not receive any objects from fetch!'
                                        , harvest_object, stage='Import')
                return False
            log.debug(str(master_data))
            identifier = master_data['record'][0]
            metadata = master_data['record'][1]
            title = metadata['title'][0] if len(metadata['title']) else identifier
            description = metadata['description'][0] if len(metadata['description']) else ''
            name = urllib.quote_plus(urllib.quote_plus(identifier))
            pkg = Package.get(name)
            if not pkg:
                pkg = Package(name=name, title=title, id=identifier)
            extras = {}
            lastidx = 0
            log.debug("  Loop over metadata")
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
                        extras['organization_%d' % lastidx] = ""
                        extras['author_%d' % lastidx] = auth
                        lastidx += 1
                elif key != 'title':
                    extras[key] = ' '.join(value)
            log.debug("  Looped over metadata")
            pkg.title = title
            pkg.notes = description
            extras['lastmod'] = extras['date']
            pkg.extras = extras
            pkg.url = "%s?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc"\
                        % (harvest_object.job.source.url, identifier)
            pkg.save()
            ofs = get_ofs()
            nowstr = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')
            label = "%s/%s.xml" % (nowstr, identifier)
            log.debug("  Get original metadata record")
            try:
                f = urllib2.urlopen(pkg.url)
                ofs.put_stream(BUCKET, label, f, {})
                fileurl = config.get('ckan.site_url') + h.url_for('storage_file', label=label)
                pkg.add_resource(url=fileurl,
                    description="Original metadata record",
                    format="xml", size=len(f.read()))
            except urllib2.HTTPError:
                self._save_object_error('Could not get original metadata record!',
                                        harvest_object, stage='Import')
            except socket.error:
                errno, errstr = sys.exc_info()[:2]
                self._save_object_error(
                    'Socket error original metadata record %s, details:\n%s' % (errno, errstr),
                    harvest_object, stage='Import')
            log.debug("  Got original metadata record")
            harvest_object.package_id = pkg.id
            harvest_object.current = True
            harvest_object.save()
            Session.add(harvest_object)
            setup_default_user_roles(pkg)
            title = metadata['title'][0] if len(metadata['title']) else ''
            description = metadata['description'][0]\
                            if len(metadata['description']) else ''
            url = ''
            log.debug("  Find url")
            for ids in metadata['identifier']:
                if ids.startswith('http://'):
                    url = ids
            if url != '':
                pkg.add_resource(url, description=description, name=title,
                                 format='html' if url.startswith('http://') else '')
            # Someone explain what the group/subgroup is.
            #group.add_package_by_name(pkg.name)
            #subg_name = "%s - %s" % (domain, set_name)
            #subgroup = Group.by_name(subg_name)
            #if not subgroup:
            #    subgroup = Group(name=subg_name, description=subg_name)
            #subgroup.add_package_by_name(pkg.name)
            #Session.add(group)
            #Session.add(subgroup)
            #setup_default_user_roles(group)
            #setup_default_user_roles(subgroup)
            log.debug("  Commit")
            model.repo.commit()
        except Exception as e:
            log.debug(traceback.format_exc(e))
            raise
        return True
