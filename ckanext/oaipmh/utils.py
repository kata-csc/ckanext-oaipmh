import datetime

from iso639 import languages

import ckan.model as model

def convert_language(lang):
    '''
    Convert alpha2 language (eg. 'en') to terminology language (eg. 'eng')
    '''

    if not lang:
        return "und"

    try:
        lang_object = languages.get(part1=lang)
        return lang_object.terminology
    except KeyError as ke:
        try:
            lang_object = languages.get(part2b=lang)
            return lang_object.terminology
        except KeyError as ke:
            return ''

def get_earliest_datestamp():
    '''
    Return earliest datestamp of packages as defined in:
    http://www.openarchives.org/OAI/openarchivesprotocol.html#Identify
    '''
    ds = model.Session.query(model.Package.metadata_modified).order_by(model.Package.metadata_modified).first()
    return ds[0] if ds else datetime.datetime.today()
