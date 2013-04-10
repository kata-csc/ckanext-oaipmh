'''
Contains code to convert metadata dictionary into form that's stored in CKAN
database. Harvester would get a record in import_stage and pass it to this
for storing the actual data in the database. Technically this could be used for
conversions outside harvesting, so if we have similar data exported from another
database then this could be used to handle that.
'''

import logging
import traceback

from ckan import model
from ckan.model import Package, Group
from ckan.model.authz import setup_default_user_roles

from ckan.lib.munge import munge_tag

log = logging.getLogger(__name__)

def oai_dc2ckan(data, group=None, harvest_object=None):
    try:
        return _oai_dc2ckan(data, group, harvest_object)
    except Exception as e:
        log.debug(traceback.format_exc(e))
    return False

def _oai_dc2ckan(data, group, harvest_object):
    model.repo.new_revision()
    identifier = data['identifier']
    metadata = data['metadata']
    title = metadata['title'][0] if len(metadata['title']) else identifier
    name = data['package_name']
    pkg = Package.get(name)
    if not pkg:
        pkg = Package(name=name, title=title, id=identifier)
        pkg.save()
    else:
        log.debug('Updating: %s' % name)
        # There are old resources which are replaced by new ones if they are
        # relevant anymore so "delete" all existing resources now.
        for r in pkg.resources:
            r.state = 'deleted'
    extras = {}
    lastidx = 0
    for met in metadata.items():
        key, value = met
        if len(value) == 0:
            continue
        if key == 'subject' or key == 'type':
            for tag in value:
                if not tag:
                    continue
                for tagi in tag.split(','):
                    tagi = tagi.strip()
                    tagi = munge_tag(tagi[:100])
                    tag_obj = model.Tag.by_name(tagi)
                    if not tag_obj:
                        tag_obj = model.Tag(name=tagi)
                    else:
                        pkgtag = model.PackageTag(tag=tag_obj, package=pkg)
                        #Session.add(tag_obj)
                        #Session.add(pkgtag)
        elif key == 'creator' or key == 'contributor':
            for auth in value:
                extras['organization_%d' % lastidx] = ''
                extras['author_%d' % lastidx] = auth
                lastidx += 1
        elif key != 'title':
            extras[key] = ' '.join(value)
    pkg.title = title
    description = metadata['description'][0] if len(metadata['description']) else ''
    pkg.notes = description
    extras['lastmod'] = extras['date']
    pkg.extras = extras
    pkg.url = data['package_url']
    # This could be a list of dictionaries in case there are more.
    if 'package_resource' in data:
        pkg.add_resource(**(data['package_resource']))
    if harvest_object != None:
        harvest_object.package_id = pkg.id
        harvest_object.current = True
        harvest_object.save()
    setup_default_user_roles(pkg)
    title = metadata['title'][0] if len(metadata['title']) else ''
    url = ''
    for ids in metadata['identifier']:
        if ids.startswith('http://'):
            url = ids
    if url != '':
        pkg.add_resource(url, description=description, name=title,
            format='html' if url.startswith('http://') else '')
    # All belong to the main group even if they do not belong to any set.
    if group != None:
        group.add_package_by_name(pkg.name)
        group.save()
    model.repo.commit()
    return True

