from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(
	name='ckanext-oaipmh',
	version=version,
	description="OAI-PMH server for CKAN",
	long_description="""\
	""",
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='Aleksi Suomalainen',
	author_email='aleksi.suomalainen@nomovok.com',
	url='http://not.yet.there',
	license='AGPL',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.oaipmh'],
	include_package_data=True,
	zip_safe=False,
	install_requires=[
		# -*- Extra requirements: -*-
		'pyoai',
		'ckanext-harvest'
	],
	tests_require=[
		'nose',
		'mock',
	],
	entry_points=\
	"""
        [ckan.plugins]
	# Add plugins here, eg
	oaipmh=ckanext.oaipmh.plugin:OAIPMHPlugin
	oaipmh_harvester=ckanext.oaipmh.harvester:OAIPMHHarvester
	""",
)
