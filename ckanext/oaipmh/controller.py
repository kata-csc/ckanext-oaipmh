'''Serving controller interface for OAI-PMH
'''
import logging

from ckan.lib.base import BaseController, render

from pylons import request, response

from oaipmh.server import BatchingServer, oai_dc_writer
from oaipmh import metadata
from oaipmh.metadata import oai_dc_reader

from oaipmh_server import CKANServer
from rdftools import rdf_reader, dcat2rdf_writer

log = logging.getLogger(__name__)


class OAIPMHController(BaseController):
    '''Controller for OAI-PMH server implementation. Returns only the index
    page if no verb is specified.
    '''
    def index(self):
        '''Return the result of the handled request of a batching OAI-PMH
        server implementation.
        '''
        if 'verb' in request.params:
            verb = request.params['verb'] if request.params['verb'] else None
            if verb:
                client = CKANServer()
                metadata_registry = metadata.MetadataRegistry()
                if 'metadataPrefix' in request.params:
                    if request.params['metadataPrefix'] == 'oai_dc':
                        metadata_registry.registerReader('oai_dc',
                                                         oai_dc_reader)
                        metadata_registry.registerWriter('oai_dc',
                                                         oai_dc_writer)
                    else:
                        metadata_registry.registerReader('rdf', rdf_reader)
                        metadata_registry.registerWriter('rdf', dcat2rdf_writer)
                else:
                    metadata_registry.registerReader('oai_dc', oai_dc_reader)
                    metadata_registry.registerWriter('oai_dc', oai_dc_writer)
                serv = BatchingServer(client,
                                      metadata_registry=metadata_registry)
                parms = request.params.mixed()
                res = serv.handleRequest(parms)
                response.headers['content-type'] = 'text/xml; charset=utf-8'
                return res
        else:
            return render('ckanext/oaipmh/oaipmh.html')
