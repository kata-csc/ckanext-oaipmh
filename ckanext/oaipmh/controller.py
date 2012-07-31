import logging
import os
from datetime import datetime

from ckan.lib.base import BaseController, render
from ckan.lib.helpers import redirect_to, flash_error
from ckan.model import Resource, Package

from pylons import request, response

from oaipmh.server import XMLTreeServer, Resumption, Server, oai_dc_writer
from oaipmh import metadata, server
from oaipmh.metadata import oai_dc_reader
from oaipmh.tests import fakeclient
from oaipmh import common

from oaipmh_server import CKANServer

log = logging.getLogger(__name__)

class OAIPMHController(BaseController):


    def index(self):
        if 'verb' in request.params:
            verb = request.params['verb'] if request.params['verb'] else None
            if verb:
                client = CKANServer()
                metadata_registry = metadata.MetadataRegistry()
                metadata_registry.registerReader('oai_dc', oai_dc_reader)
                metadata_registry.registerWriter('oai_dc', oai_dc_writer)
                serv = Server(client, metadata_registry=metadata_registry)
                parms = request.params.mixed()
                try:
                    res = serv.handleRequest(parms)
                except common.error.ErrorBase, e:
                    return serv.handleException(parms, (None, e, None))
                response.headers['content-type'] = 'text/xml; charset=utf-8' 
                return res
        else:
            return render('ckanext/oaipmh/oaipmh.xhtml')