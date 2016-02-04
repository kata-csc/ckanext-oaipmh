from setuptools import setup, find_packages

version = '0.6.2'

setup(
    name='ckanext-oaipmh',
    version=version,
    description="OAI-PMH server and harvester for CKAN",
    long_description="""\
        """,
    classifiers=[],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='CSC - IT Center for Science Ltd.',
    author_email='kata-project@postit.csc.fi',
    url='https://github.com/kata-csc/ckanext-oaipmh',
    license='AGPL',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.oaipmh'],
    include_package_data=True,
    zip_safe=False,
    entry_points="""
        [ckan.plugins]
        oaipmh=ckanext.oaipmh.plugin:OAIPMHPlugin
        oaipmh_harvester=ckanext.oaipmh.harvester:OAIPMHHarvester
        ida_harvester=ckanext.oaipmh.ida:IdaHarvester
        cmdi_harvester=ckanext.oaipmh.cmdi:CMDIHarvester
        """,
)
