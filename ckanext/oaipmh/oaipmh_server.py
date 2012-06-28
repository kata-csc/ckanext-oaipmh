from datetime import datetime

from ckan.model import Resource, Package, Session, Group, ResourceGroup
from ckan.lib.helpers import url_for

from oaipmh.common import ResumptionOAIPMH
from oaipmh import common

import logging

log = logging.getLogger(__name__)

class CKANServer(ResumptionOAIPMH):

    def identify(self):
        return common.Identify(
            repositoryName='YourServer',
            baseURL='http://localhost:5000/oai/',
            protocolVersion="2.0",
            adminEmails=['faassen@infrae.com'],
            earliestDatestamp=datetime(2004, 1, 1),
            deletedRecord='no',
            granularity='YYYY-MM-DDThh:mm:ssZ',
            compression=['identity'])

    def _record_for_dataset(self, dataset):
        meta = {
                            'title': [dataset.name],
                            'creator': [dataset.author],
                            'identifier': [url_for(controller="package",
                                                   action='read',
                                                   id = dataset.id),
                                           dataset.url if dataset.url else dataset.id],
                            'type': [dataset.type],
                            'description': [dataset.notes]
        }
        iters = dataset.extras.items()
        meta = dict(meta.items() + iters)
        metadata = {}
        # Fixes the bug on having a large dataset being scrambled to individual
        # letters
        for key, value in meta.items():
            metadata[str(key)] = [unicode(str(value))] if not isinstance(value,list) else value
        return (common.Header(dataset.id,
                              dataset.metadata_created,
                              [dataset.name],
                              False), 
                common.Metadata(metadata),
                None)

    def getRecord(self, metadataPrefix, identifier):
        package = Package.get(identifier)
        if not package:
            package = Package.by_name(identifier)
        return self._record_for_dataset(package)

    def listIdentifiers(self, metadataPrefix, set=None, resumption_token=None):
        data = []
        packages = []
        if not set:
            packages = Session.query(Package).all()
        else:
            group = Group.get(set)
            if group:
                packages = group.active_packages()
        if resumption_token:
            res_token = urllib2.unquote(resumption_token)
            cursor = res_token.split('&')[0]
            cursor = int(cursor.split(':')[-1])
            packages = packages[cursor:]
        for package in packages:
            data.append(common.Header(package.id,
                                      package.metadata_created,
                                      [package.name],
                                      False))
        return data

    def listMetadataFormats(self):
        return [('oai_dc',
                'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
                'http://www.openarchives.org/OAI/2.0/oai_dc/')]

    def listRecords(self, metadataPrefix, set=None, resumption_token=None):
        data = []
        packages = []
        if not set:
            packages = Session.query(Package).all()
        else:
            group = Group.get(set)
            if group:
                packages = group.active_packages()
        if resumption_token:
            res_token = urllib2.unquote(resumption_token)
            cursor = res_token.split('&')[0]
            cursor = int(cursor.split(':')[-1])
            packages = packages[cursor:]
        for res in packages:
            data.append(self._record_for_dataset(res))
        return data

    def listSets(self, resumption_token=None):
        data = []
        if not resumption_token:
            datasets = Session.query(Group).all()
        else:
            cursor = int(urllib2.unquote(resumption_token).split(':')[-1])
            datasets = Session.query(Group).all()[cursor:]
        for dataset in datasets:
            data.append((dataset.id, dataset.name, dataset.description))
        return data

    def handleVerb(self, verb, params):
        log.debug(verb)
        log.debug(params)