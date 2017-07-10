'''OAI-PMH implementation for CKAN datasets and groups.
'''
# pylint: disable=E1101,E1103
import json
import logging

from oaipmh import common
from oaipmh.common import ResumptionOAIPMH
from oaipmh.error import IdDoesNotExistError
from pylons import config
from sqlalchemy import between

from ckan.lib.helpers import url_for
from ckan.logic import get_action
from ckan.model import Package, Session, Group, PackageRevision, Tag
from ckanext.dcat.processors import RDFSerializer
from ckanext.kata import helpers
import utils

log = logging.getLogger(__name__)

rdfserializer = RDFSerializer()


class CKANServer(ResumptionOAIPMH):
    '''A OAI-PMH implementation class for CKAN.
    '''
    def identify(self):
        '''Return identification information for this server.
        '''
        return common.Identify(
            repositoryName=config.get('ckan.site_title', 'repository'),
            baseURL=config.get('ckan.site_url', None) + url_for(controller='ckanext.oaipmh.controller:OAIPMHController', action='index'),
            protocolVersion="2.0",
            adminEmails=['etsin@csc.fi'],
            earliestDatestamp=utils.get_earliest_datestamp(),
            deletedRecord='no',
            granularity='YYYY-MM-DDThh:mm:ssZ',
            compression=['identity'])

    def _get_json_content(self, js):
        '''
        Gets all items from JSON

        :param js: json string
        :return: list of items
        '''

        try:
            json_data = json.loads(js)
            json_titles = list()
            for key, value in json_data.iteritems():
                if value:
                    json_titles.append(value)
            return json_titles
        except:
            return [js]

    def _record_for_dataset_dcat(self, dataset, set_spec):
        '''Show a tuple of a header and metadata for this dataset.
        Note that dataset_xml (metadata) returned is just a string containing
        ready rdf xml. This is contrary to the common practice of pyoia's
        getRecord method.
        '''
        package = get_action('package_show')({}, {'id': dataset.id})
        dataset_xml = rdfserializer.serialize_dataset(package, _format='xml')
        return (common.Header('', dataset.id, dataset.metadata_created, set_spec, False),
                dataset_xml, None)


    def _record_for_dataset_datacite(self, dataset, set_spec):
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

        identifier = [pid.get('id') for pid in package.get('pids', {}) if
                pid.get('id', False) and pid.get('type', False) == 'primary']
        pids = [pid.get('id') for pid in package.get('pids', {}) if pid.get('id', False) and pid.get('type', False) == 'primary']
        pids.append(package.get('id'))
        pids.append(config.get('ckan.site_url') + url_for(controller="package", action='read', id=package['name']))

        publ_events = filter(lambda x: x.get('type') in [u'published', u'collection', u'creation'], package.get('event', []))
        publ_date = publ_events[0].get('when') if publ_events else package.get('metadata_created')
        publ_year = publ_date.split('-')[0]

        dates = filter(lambda x: x.get('type') in [u'collection', u'creation', u'extended', u'changed', u'published',
                                                   u'sent', u'received', u'modified'],
                       package.get('event', []))
        dates.append({'when': package.get('version', package.get('metadata_created', '')), 'type': 'published'})

        meta = {'titles': json.loads(package.get('title', None) or package.get('name')),
                'creators': helpers.get_authors(package),
                'publisher': [agent['name'] for agent in helpers.get_contacts(package) + helpers.get_distributors(package) if 'name' in agent],
                'contributors': helpers.get_contributors(package),
                'funders': helpers.get_funders(package),
                'identifier': identifier,
                'identifier/@identifierType': 'URN',
                'dates': dates,
                'language': [l.strip() for l in package.get('language').split(",")] if package.get('language', None) else None,
                'description': self._get_json_content(package.get('notes')) if package.get('notes', None) else None,
                'subjects': [tag.get('display_name') for tag in package['tags']] if package.get('tags', None) else None,
                'publicationYear': publ_year,
                'rights': [package['license_title']] if package.get('license_title', None) else None,
                'coverage': coverage if coverage else None, }

        metadata = {}
        # Fixes the bug on having a large dataset being scrambled to individual
        # letters
        for key, value in meta.items():
            if value and not isinstance(value, list):
                metadata[str(key)] = [value]
            else:
                metadata[str(key)] = value
        return (common.Header('', dataset.id, dataset.metadata_created, set_spec, False),
                common.Metadata('', metadata), None)


    def _record_for_dataset(self, dataset, set_spec):
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
        pids.append(package.get('id'))
        pids.append(config.get('ckan.site_url') + url_for(controller="package", action='read', id=package['name']))

        meta = {'title': self._get_json_content(package.get('title', None) or package.get('name')),
                'creator': [author['name'] for author in helpers.get_authors(package) if 'name' in author],
                'publisher': [agent['name'] for agent in helpers.get_distributors(package) + helpers.get_contacts(package) if 'name' in agent],
                'contributor': [author['name'] for author in helpers.get_contributors(package) if 'name' in author],
                'identifier': pids,
                'type': ['dataset'],
                'language': [l.strip() for l in package.get('language').split(",")] if package.get('language', None) else None,
                'description': self._get_json_content(package.get('notes')) if package.get('notes', None) else None,
                'subject': [tag.get('display_name') for tag in package['tags']] if package.get('tags', None) else None,
                'date': [dataset.metadata_created.strftime('%Y-%m-%d')] if dataset.metadata_created else None,
                'rights': [package['license_title']] if package.get('license_title', None) else None,
                'coverage': coverage if coverage else [], }

        iters = dataset.extras.items()
        meta = dict(iters + meta.items())
        metadata = {}
        # Fixes the bug on having a large dataset being scrambled to individual
        # letters
        for key, value in meta.items():
            if not isinstance(value, list):
                metadata[str(key)] = [value]
            else:
                metadata[str(key)] = value
        return (common.Header('', dataset.id, dataset.metadata_created, set_spec, False),
                common.Metadata('', metadata), None)

    @staticmethod
    def _filter_packages(set, cursor, from_, until, batch_size):
        '''Get a part of datasets for "listNN" verbs.
        '''
        packages = []
        setspc = None
        if not set:
            packages = Session.query(Package).filter(Package.type=='dataset'). \
                filter(Package.state == 'active').filter(Package.private!=True)
            if from_ and not until:
                packages = packages.filter(PackageRevision.revision_timestamp > from_).\
                    filter(Package.name==PackageRevision.name)
            if until and not from_:
                packages = packages.filter(PackageRevision.revision_timestamp < until).\
                    filter(Package.name==PackageRevision.name)
            if from_ and until:
                packages = packages.filter(between(PackageRevision.revision_timestamp, from_, until)).\
                    filter(Package.name==PackageRevision.name)
            packages = packages.all()
        elif set == 'openaire_data':
            oa_tag = Session.query(Tag).filter(Tag.name == 'openaire_data').first()
            if oa_tag:
                packages = oa_tag.packages
            setspc = set
        else:
            group = Group.get(set)
            if group:
                # Note that group.packages never returns private datasets regardless of 'with_private' parameter.
                packages = group.packages(return_query=True, with_private=False).filter(Package.type=='dataset'). \
                    filter(Package.state == 'active')
                if from_ and not until:
                    packages = packages.filter(PackageRevision.revision_timestamp > from_).\
                        filter(Package.name==PackageRevision.name)
                if until and not from_:
                    packages = packages.filter(PackageRevision.revision_timestamp < until).\
                        filter(Package.name==PackageRevision.name)
                if from_ and until:
                    packages = packages.filter(between(PackageRevision.revision_timestamp, from_, until)).\
                        filter(Package.name==PackageRevision.name)
                packages = packages.all()
        if cursor is not None:
            cursor_end = cursor + batch_size if cursor + batch_size < len(packages) else len(packages)
            packages = packages[cursor:cursor_end]
        return packages, setspc

    def getRecord(self, metadataPrefix, identifier):
        '''Simple getRecord for a dataset.
        '''
        package = Package.get(identifier)
        if not package:
            raise IdDoesNotExistError("No dataset with id %s" % identifier)
        set_spec = []
        if package.owner_org:
            group = Group.get(package.owner_org)
            if group and group.name:
                set_spec.append(group.name)
        if 'openaire_data' in package.as_dict().get('tags'):
            set_spec.append('openaire_data')
        if not set_spec:
            set_spec = [package.name]
        if metadataPrefix == 'rdf':
            return self._record_for_dataset_dcat(package, set_spec)
        if metadataPrefix == 'oai_datacite':
            return self._record_for_dataset_datacite(package, set_spec)
        return self._record_for_dataset(package, set_spec)

    def listIdentifiers(self, metadataPrefix=None, set=None, cursor=None,
                        from_=None, until=None, batch_size=None):
        '''List all identifiers for this repository.
        '''
        data = []
        packages, setspc = self._filter_packages(set, cursor, from_, until, batch_size)
        for package in packages:
            set_spec = []
            if setspc:
                set_spec.append(setspc)
            if package.owner_org:
                group = Group.get(package.owner_org)
                if group and group.name:
                    set_spec.append(group.name)
            if not set_spec:
                set_spec = [package.name]
            data.append(common.Header('', package.id, package.metadata_created, set_spec, False))
        return data

    def listMetadataFormats(self, identifier=None):
        '''List available metadata formats.
        '''
        return [('oai_dc',
                 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
                 'http://www.openarchives.org/OAI/2.0/oai_dc/'),
                ('oai_datacite',
                 'http://schema.datacite.org/meta/kernel-3/metadata.xsd',
                 'http://datacite.org/schema/kernel-3'),
                ('rdf',
                 'http://www.openarchives.org/OAI/2.0/rdf.xsd',
                 'http://www.openarchives.org/OAI/2.0/rdf/')]

    def listRecords(self, metadataPrefix=None, set=None, cursor=None, from_=None,
                    until=None, batch_size=None):
        '''Show a selection of records, basically lists all datasets.
        '''
        data = []
        packages, setspc = self._filter_packages(set, cursor, from_, until, batch_size)
        for package in packages:
            set_spec = []
            if setspc:
                set_spec.append(setspc)
            if package.owner_org:
                group = Group.get(package.owner_org)
                if group and group.name:
                    set_spec.append(group.name)
            if not set_spec:
                set_spec = [package.name]
            if metadataPrefix == 'rdf':
                data.append(self._record_for_dataset_dcat(package, set_spec))
            if metadataPrefix == 'oai_datacite':
                data.append(self._record_for_dataset_datacite(package, set_spec))
            else:
                data.append(self._record_for_dataset(package, set_spec))
        return data

    def listSets(self, cursor=None, batch_size=None):
        '''List all sets in this repository, where sets are groups.
        '''
        data = []
        if not cursor or cursor == 0:
            data.append(('openaire_data', 'OpenAIRE data', ''))
        groups = Session.query(Group).filter(Group.state == 'active')
        if cursor is not None:
            cursor_end = cursor+batch_size if cursor+batch_size < groups.count() else groups.count()
            groups = groups[cursor:cursor_end]
        for group in groups:
            data.append((group.name, group.title, group.description))
        return data
