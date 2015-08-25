from iso639 import languages

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
