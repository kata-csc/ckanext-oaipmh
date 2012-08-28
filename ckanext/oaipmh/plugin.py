import logging
import os
from ckan.plugins import implements, SingletonPlugin
from ckan.plugins import IRoutes, IConfigurer

log = logging.getLogger(__name__)


class OAIPMHPlugin(SingletonPlugin):
    implements(IRoutes, inherit=True)
    implements(IConfigurer)

    def update_config(self, config):
        """This IConfigurer implementation causes CKAN to look in the
        ```public``` and ```templates``` directories present in this
        package for any customisations.

        It also shows how to set the site title here (rather than in
        the main site .ini file), and causes CKAN to use the
        customised package form defined in ``package_form.py`` in this
        directory.
        """
        here = os.path.dirname(__file__)
        rootdir = os.path.dirname(os.path.dirname(here))
        template_dir = os.path.join(rootdir, 'ckanext',
                                    'oaipmh', 'templates')
        config['extra_template_paths'] = ','.join([template_dir,
                config.get('extra_template_paths', '')])

    def before_map(self, map):
        controller = 'ckanext.oaipmh.controller:OAIPMHController'
        map.connect('oai', '/oai', controller=controller, action='index')
        return map
