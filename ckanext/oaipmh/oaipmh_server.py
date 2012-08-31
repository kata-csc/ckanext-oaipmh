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

import logging

log = logging.getLogger(__name__)


class CKANServer(ResumptionOAIPMH):
    '''A OAI-PMH implementation class for CKAN.
    '''
    def identify(self):
        '''Return identification information for this server.
        '''
        return common.Identify(
            repositoryName=config.get('site.title') if config.get('site.title')
                                                    else 'repository',
            baseURL=url_for(
                        controller='ckanext.oaipmh.controller:OAIPMHController',
                        action='index'),
            protocolVersion="2.0",
            adminEmails=[config.get('email_to')],
            earliestDatestamp=datetime(2004, 1, 1),
            deletedRecord='no',
            granularity='YYYY-MM-DD',
            compression=['identity'])

    def _record_for_dataset(self, dataset):
        '''Show a tuple of a header and metadata for this dataset.
        '''
        meta = {
                'title': [dataset.name],
                'creator': [dataset.author] if dataset.author else None,
                'contributor': [dataset.maintainer]
                    if dataset.maintainer else None,
                'identifier': [
                    config.get('ckan.site_url') +
                    url_for(controller="package", action='read', id=dataset.id),
                    dataset.url if dataset.url else dataset.id],
                'type': ['dataset'],
                'description': [dataset.notes] if dataset.notes else None,
                'subject': [tag.name for tag in dataset.get_tags()]
                    if len(dataset.get_tags()) >= 1 else None,
                'date': [dataset.metadata_created.strftime('%Y-%m-%d')]
                    if dataset.metadata_created else None,
                'rights': [dataset.license.title if dataset.license else '']
                    if dataset.license else None,
        }
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
        return (common.Header(dataset.id,
                              dataset.metadata_created,
                              [dataset.name],
                              False),
                common.Metadata(metadata),
                None)

    def getRecord(self, metadataPrefix, identifier):
        '''Simple getRecord for a dataset.
        '''
        package = Package.get(identifier)
        return self._record_for_dataset(package)

    def listIdentifiers(self, metadataPrefix, set=None, cursor=None,
                        from_=None, until=None, batch_size=None):
        '''List all identifiers for this repository.
        '''
        data = []
        packages = []
        if not set:
            if not from_ and not until:
                packages = Session.query(Package).all()
            else:
                if from_:
                    packages = Session.query(Package).\
                        filter(PackageRevision.revision_timestamp > from_).\
                        all()
                if until:
                    packages = Session.query(Package).\
                        filter(PackageRevision.revision_timestamp < until).\
                        all()
                if from_ and until:
                    packages = Session.query(Package).\
                        filter(between(PackageRevision.revision_timestamp,
                                       from_,
                                       until)\
                               ).all()
        else:
            group = Group.get(set)
            if group:
                packages = group.active_packages()
                if from_ and not until:
                    packages = packages.\
                        filter(PackageRevision.revision_timestamp > from_)
                if until and not from_:
                    packages = packages.\
                        filter(PackageRevision.revision_timestamp < until)
                if from_ and until:
                    packages = packages.filter(
                        between(PackageRevision.revision_timestamp,
                                from_,
                                until))
                packages = packages.all()
        if cursor:
            packages = packages[:cursor]
        for package in packages:
            data.append(common.Header(package.id,
                                      package.metadata_created,
                                      [package.name],
                                      False))
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
                packages = Session.query(Package).all()
            if from_:
                packages = Session.query(Package).\
                    filter(PackageRevision.revision_timestamp > from_).all()
            if until:
                packages = Session.query(Package).\
                    filter(PackageRevision.revision_timestamp < until).all()
            if from_ and until:
                packages = Session.query(Package).filter(
                    between(PackageRevision.revision_timestamp,from_,until)).\
                    all()
        else:
            group = Group.get(set)
            if group:
                packages = group.active_packages()
                if from_ and not until:
                    packages = packages.\
                        filter(PackageRevision.revision_timestamp > from_).\
                        all()
                if until and not from_:
                    packages = packages.\
                        filter(PackageRevision.revision_timestamp < until).\
                        all()
                if from_ and until:
                    packages = packages.filter(
                            between(PackageRevision.revision_timestamp,
                                    from_,
                                    until))\
                                    .all()
        if cursor:
            packages = packages[:cursor]
        for res in packages:
            data.append(self._record_for_dataset(res))
        return data

    def listSets(self, cursor=None, batch_size=None):
        '''List all sets in this repository, where sets are groups.
        '''
        data = []
        if not cursor:
            groups = Session.query(Group).all()
        else:
            groups = Session.query(Group).all()[:cursor]
        for dataset in groups:
            data.append((dataset.id, dataset.name, dataset.description))
        return data
