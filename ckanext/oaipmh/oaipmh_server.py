from datetime import datetime

from ckan.model import Package, Session, Group, PackageRevision
from ckan.lib.helpers import url_for

from pylons import config

from sqlalchemy import DateTime, cast, between

from oaipmh.common import ResumptionOAIPMH
from oaipmh import common

import logging

log = logging.getLogger(__name__)


class CKANServer(ResumptionOAIPMH):

    def identify(self):
        return common.Identify(
            repositoryName=config.get('site.title') if config.get('site.title') else 'repository',
            baseURL=url_for(controller='ckanext.oaipmh.controller:OAIPMHController',action='index'),
            protocolVersion="2.0",
            adminEmails=[config.get('email_to')],
            earliestDatestamp=datetime(2004, 1, 1),
            deletedRecord='no',
            granularity='YYYY-MM-DD',
            compression=['identity'])

    def _record_for_dataset(self, dataset):
        meta = {
                            'title': [dataset.name],
                            'creator': [dataset.author],
                            'identifier': [config.get('ckan.site_url') + url_for(controller="package",
                                                   action='read',
                                                   id=dataset.id),
                                           dataset.url if dataset.url else dataset.id],
                            'type': ['dataset'],
                            'description': [dataset.notes],
                            'subject': [tag.name for tag in dataset.get_tags()],
                            'date': [dataset.metadata_created.strftime('%Y-%m-%d')],
                            'rights': [dataset.license.title if dataset.license else ''],
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
        package = Package.get(identifier)
        return self._record_for_dataset(package)

    def listIdentifiers(self, metadataPrefix, set=None, cursor=None, from_=None, until=None, batch_size=None):
        data = []
        packages = []
        if not set:
            if not from_ and not until:
                packages = Session.query(Package).all()
            else:
                if from_:
                    packages = Session.query(Package).filter(PackageRevision.revision_timestamp > from_).all()
                if until:
                    packages = Session.query(Package).filter(PackageRevision.revision_timestamp < until).all()
                if from_ and until:
                    packages = Session.query(Package).filter(between(PackageRevision.revision_timestamp, from_, until)).all()
        else:
            group = Group.get(set)
            if group:
                packages = group.active_packages()
                if from_ and not until:
                    packages = packages.filter(PackageRevision.revision_timestamp > from_)
                if until and not from_:
                    packages = packages.filter(PackageRevision.revision_timestamp < until)
                if from_ and until:
                    packages = packages.filter(between(PackageRevision.revision_timestamp, from_, until))
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
        return [('oai_dc',
                'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
                'http://www.openarchives.org/OAI/2.0/oai_dc/'),
                ('rdf',
                 'http://www.openarchives.org/OAI/2.0/rdf.xsd',
                 'http://www.openarchives.org/OAI/2.0/rdf/')]

    def listRecords(self, metadataPrefix, set=None, cursor=None, from_=None, until=None, batch_size=None):
        data = []
        packages = []
        if not set:
            if not from_ and not until:
                packages = Session.query(Package).all()
            if from_:
                packages = Session.query(Package).filter(PackageRevision.revision_timestamp > from_).all()
            if until:
                packages = Session.query(Package).filter(PackageRevision.revision_timestamp < until).all()
            if from_ and until:
                packages = Session.query(Package).filter(between(PackageRevision.revision_timestamp,from_,until)).all()
        else:
            group = Group.get(set)
            if group:
                packages = group.active_packages()
                if from_ and not until:
                    packages = packages.filter(PackageRevision.revision_timestamp > from_).all()
                if until and not from_:
                    packages = packages.filter(PackageRevision.revision_timestamp < until).all()
                if from_ and until:
                    packages = packages.filter(between(PackageRevision.revision_timestamp,from_,until)).all()
        if cursor:
            packages = packages[:cursor]
        for res in packages:
            data.append(self._record_for_dataset(res))
        return data

    def listSets(self, cursor=None, batch_size=None):
        data = []
        if not cursor:
            datasets = Session.query(Group).all()
        else:
            datasets = Session.query(Group).all()[:cursor]
        for dataset in datasets:
            data.append((dataset.id, dataset.name, dataset.description))
        return data
