from iso639 import languages

def convert_language(lang):
    '''
    Convert alpha2 language (eg. 'en') to terminology language (eg. 'eng')
    '''
    try:
        lang_object = languages.get(part1=lang)
        return lang_object.terminology
    except KeyError as ke:
        #log.debug('Invalid language: {ke}'.format(ke=ke))
        return ''