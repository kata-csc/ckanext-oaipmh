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
        'functionally',
        'fn',

        # These are required only for testing, but are listed here as
        # production requirements as PIP doesn't support 'tests_require' keyword.
        # Another option would be to include them in a pip requirements file
        # and install with: pip install -e -r 'pip-requirements-test.txt'.
        'nose>=1.0',
        'coverage',
        'mock',
    ],
    entry_points=\
        """
        [ckan.plugins]
        # Add plugins here, eg
        oaipmh_harvester=ckanext.oaipmh.harvester:OAIPMHHarvester
        """,
)
