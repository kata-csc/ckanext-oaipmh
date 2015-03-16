'''OAI-PMH implementation for CKAN datasets and groups.
'''
# pylint: disable=E1101,E1103
from datetime import datetime

from ckan.model import Package, Session, Group, PackageRevision
from ckan.lib.helpers import url_for

from pylons import config

from sqlalchemy import between

from oaipmh.common import ResumptionOAIPMH
from oaipmh import common
from ckanext.kata import helpers
import logging
from ckan.logic import get_action
from oaipmh.error import IdDoesNotExistError

log = logging.getLogger(__name__)


class CKANServer(ResumptionOAIPMH):
    '''A OAI-PMH implementation class for CKAN.
    '''
    def identify(self):
        '''Return identification information for this server.
        '''
        return common.Identify(
            repositoryName=config.get('site.title') if config.get('site.title') else 'repository',
            baseURL=url_for(controller='ckanext.oaipmh.controller:OAIPMHController', action='index'),
            protocolVersion="2.0",
            adminEmails=[config.get('email_to')],
            earliestDatestamp=datetime(2004, 1, 1),
            deletedRecord='no',
            granularity='YYYY-MM-DD',
            compression=['identity'])

    def _record_for_dataset(self, dataset):
        '''Show a tuple of a header and metadata for this dataset.
        '''
        package = get_action('package_show')({}, {'id': dataset.id})

        coverage = []
        temporal_begin = package.get('temporal_coverage_begin', '')
        temporal_end = package.get('temporal_coverage_end', '')

        geographic = package.get('geographic_coverage', '')
        if geographic:
            coverage.extend(geographic.split(','))
        if temporal_begin or temporal_end:
            coverage.append("%s/%s" % (temporal_begin, temporal_end))

        pids = [pid.get('id') for pid in package.get('pids', {}) if pid.get('id', False)]
        pids.append(config.get('ckan.site_url') + url_for(controller="package", action='read', id=package['id']))

        meta = {'title': [package.get('title', None) or package.get('name')],
                'creator': [author['name'] for author in helpers.get_authors(package) if 'name' in author],
                'publisher': [agent['name'] for agent in helpers.get_distributors(package) + helpers.get_contacts(package) if 'name' in agent],
                'contributor': [author['name'] for author in helpers.get_contributors(package) if 'name' in author],
                'identifier': pids,
                'type': ['dataset'],
                'description': [package.get('notes')] if package.get('notes', None) else None,
                'subject': [tag.get('display_name') for tag in package['tags']] if package.get('tags', None) else None,
                'date': [dataset.metadata_created.strftime('%Y-%m-%d')] if dataset.metadata_created else None,
                'rights': [package['license_title']] if package.get('license_title', None) else None,
                'coverage': coverage if coverage else None, }

        iters = dataset.extras.items()
        meta = dict(meta.items() + iters)
        metadata = {}
        # Fixes the bug on having a large dataset being scrambled to individual
        # letters
        for key, value in meta.items():
            if not isinstance(value, list):
                metadata[str(key)] = [value]
            else:
                metadata[str(key)] = value

        return (common.Header(dataset.id, dataset.metadata_created, [dataset.name], False),
                common.Metadata(metadata), None)

    def getRecord(self, metadataPrefix, identifier):
        '''Simple getRecord for a dataset.
        '''
        package = Package.get(identifier)
        if not package:
            raise IdDoesNotExistError("No dataset with id %s" % identifier)
        return self._record_for_dataset(package)

    def listIdentifiers(self, metadataPrefix, set=None, cursor=None,
                        from_=None, until=None, batch_size=None):
        '''List all identifiers for this repository.
        '''
        data = []
        packages = []
        if not set:
            if not from_ and not until:
                packages = Session.query(Package).filter(Package.type=='dataset').filter(Package.private!=True).all()
            else:
                if from_ and not until:
                    packages = Session.query(Package).filter(Package.type=='dataset').filter(Package.private!=True).\
                        filter(PackageRevision.revision_timestamp > from_).\
                        filter(Package.name==PackageRevision.name).all()
                if until and not from_:
                    packages = Session.query(Package).filter(Package.type=='dataset').filter(Package.private!=True).\
                        filter(PackageRevision.revision_timestamp < until).\
                        filter(Package.name==PackageRevision.name).all()
                if from_ and until:
                    packages = Session.query(Package).filter(Package.type=='dataset').filter(Package.private!=True).\
                        filter(between(PackageRevision.revision_timestamp, from_, until)).\
                        filter(Package.name==PackageRevision.name).all()
        else:
            group = Group.get(set)
            if group:
                packages = group.packages(return_query=True).filter(Package.type=='dataset').\
                    filter(Package.private!=True)
                if from_ and not until:
                    packages = packages.\
                        filter(PackageRevision.revision_timestamp > from_).filter(Package.name==PackageRevision.name)
                if until and not from_:
                    packages = packages.\
                        filter(PackageRevision.revision_timestamp < until).filter(Package.name==PackageRevision.name)
                if from_ and until:
                    packages = packages.filter(between(PackageRevision.revision_timestamp, from_, until)).\
                        filter(Package.name==PackageRevision.name)
                packages = packages.all()
        if cursor:
            packages = packages[cursor:]
        for package in packages:
            data.append(common.Header(package.id, package.metadata_created, [package.name], False))

        return data

    def listMetadataFormats(self):
        '''List available metadata formats.
        '''
        return [('oai_dc',
                'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
                'http://www.openarchives.org/OAI/2.0/oai_dc/'),
                ('rdf',
                 'http://www.openarchives.org/OAI/2.0/rdf.xsd',
                 'http://www.openarchives.org/OAI/2.0/rdf/')]

    def listRecords(self, metadataPrefix, set=None, cursor=None, from_=None,
                    until=None, batch_size=None):
        '''Show a selection of records, basically lists all datasets.
        '''
        data = []
        packages = []
        if not set:
            if not from_ and not until:
                packages = Session.query(Package).filter(Package.type=='dataset').filter(Package.private!=True).all()
            if from_ and not until:
                packages = Session.query(Package).filter(Package.type=='dataset').filter(Package.private!=True).\
                    filter(PackageRevision.revision_timestamp > from_).filter(Package.name==PackageRevision.name).all()
            if until and not from_:
                packages = Session.query(Package).filter(Package.type=='dataset').filter(Package.private!=True).\
                    filter(PackageRevision.revision_timestamp < until).filter(Package.name==PackageRevision.name).all()
            if from_ and until:
                packages = Session.query(Package).filter(Package.type=='dataset').filter(Package.private!=True).filter(
                    between(PackageRevision.revision_timestamp, from_, until)).\
                    filter(Package.name==PackageRevision.name).all()
        else:
            group = Group.get(set)
            if group:
                packages = group.packages(return_query=True)
                if from_ and not until:
                    packages = packages.\
                        filter(PackageRevision.revision_timestamp > from_).filter(Package.type=='dataset').\
                        filter(Package.private!=True).all()
                if until and not from_:
                    packages = packages.\
                        filter(PackageRevision.revision_timestamp < until).filter(Package.type=='dataset').\
                        filter(Package.private!=True).filter(Package.name==PackageRevision.name).all()
                if from_ and until:
                    packages = packages.filter(between(PackageRevision.revision_timestamp, from_, until)).\
                        filter(Package.type=='dataset').filter(Package.name==PackageRevision.name).\
                        filter(Package.private!=True).all()
        if cursor:
            packages = packages[cursor:]
        for res in packages:
            data.append(self._record_for_dataset(res))
        return data

    def listSets(self, cursor=None, batch_size=None):
        '''List all sets in this repository, where sets are groups.
        '''
        data = []
        groups = Session.query(Group).filter(Group.state == 'active')
        if cursor:
            groups = groups[cursor:]
        for dataset in groups:
            data.append((dataset.id, dataset.name, dataset.description))
        return data
