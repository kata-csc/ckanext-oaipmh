'''Serving controller interface for OAI-PMH
'''
import logging

import oaipmh.metadata as oaimd
import oaipmh.server as oaisrv
from pylons import request, response

from ckan.lib.base import BaseController, render
from oaipmh_server import CKANServer
from rdftools import rdf_reader, dcat2rdf_writer, datacite_writer

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
                metadata_registry = oaimd.MetadataRegistry()
                metadata_registry.registerReader('oai_dc', oaimd.oai_dc_reader)
                metadata_registry.registerWriter('oai_dc', oaisrv.oai_dc_writer)
                metadata_registry.registerReader('rdf', rdf_reader)
                metadata_registry.registerWriter('rdf', dcat2rdf_writer)
                metadata_registry.registerWriter('datacite', datacite_writer)
                serv = oaisrv.BatchingServer(client,
                                             metadata_registry=metadata_registry,
                                             resumption_batch_size=10)
                parms = request.params.mixed()
                res = serv.handleRequest(parms)
                response.headers['content-type'] = 'text/xml; charset=utf-8'
                return res
        else:
            return render('ckanext/oaipmh/oaipmh.html')
