from setuptools import setup, find_packages

version = '0.2'

setup(
    name='ckanext-oaipmh',
    version=version,
    description="OAI-PMH harvester for CKAN",
    long_description="""\
        """,
    classifiers=[],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='Mikael Karlsson',
    author_email='i8myshoes@gmail.com',
    url='http://not.yet.there',
    license='AGPL',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.oaipmh'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # -*- Extra requirements: -*-
        'pyoai',
        'ckanext-harvest',
        'lxml',
        'rdflib',
        'beautifulsoup4',
        'pointfree',
        'functionally'
    ],
    setup_requires=[
        'nose>=1.0',
        'coverage'
    ],
    tests_require=[
        'nose',
        'mock',
    ],
    entry_points=\
        """
        [ckan.plugins]
        # Add plugins here, eg
        oaipmh_harvester=ckanext.oaipmh.harvester:OAIPMHHarvester
        """,
)
