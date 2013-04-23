'''
Contains code to convert metadata dictionary into form that's stored in CKAN
database. Harvester would get a record in import_stage and pass it to this
for storing the actual data in the database. Technically this could be used for
conversions outside harvesting, so if we have similar data exported from another
database then this could be used to handle that.
'''

import logging
import traceback
import datetime

from ckan import model
from ckan.model import Package, Group
from ckan.model.authz import setup_default_user_roles
from ckan.model.license import LicenseRegister, LicenseOtherPublicDomain
from ckan.model.license import LicenseOtherClosed, LicenseNotSpecified
from ckan.controllers.storage import BUCKET, get_ofs

from ckan.lib.munge import munge_tag
from lxml import etree

log = logging.getLogger(__name__)

def oai_dc2ckan(data, namespaces, group=None, harvest_object=None):
    try:
        return _oai_dc2ckan(data, namespaces, group, harvest_object)
    except Exception as e:
        log.debug(traceback.format_exc(e))
    return False

# Annoyingly, attribute such as rdf:about is presented with key such as
# {http://www.w3.org/1999/02/22-rdf-syntax-ns#}about so we have to check the
# end of the key. 
def _find_value(node, key_end):
    for key in node.keys():
        loc = key.find(key_end)
        if loc == len(key) - len(key_end):
            return node.get(key)
    return None

# Given information about the license, try to match it with some known one.
def _match_license(text):
    lr = LicenseRegister()
    for lic in lr.licenses:
        if text in (lic.url, lic.id, lic.title,):
            return lic.id
    return None

def _handle_rights(node, namespaces):
    if node is None:
        return {}
    d = {}
    decls = node.xpath('./*[local-name() = "RightsDeclaration"]',
        namespaces=namespaces)
    if len(decls):
        if len(decls) > 1:
            log.warning('Multiple RightsDeclarations in one record.')
        category = decls[0].get('RIGHTSCATEGORY')
        text = decls[0].text
    else: # Probably just old-fashioned text. Fix when counter-example found.
        text = node.text
        category = 'LICENSED' # Let's give recognizing the license a try.
    if category == 'LICENSED' and text:
        lic = _match_license(text)
        if lic is not None:
            d['package.license'] = { 'id':lic }
        else:
            # Something unknown. Store text or license.
            if text.startswith('http://'):
                d['licenseURL'] = text
            else:
                d['licenseText'] = text
    elif category == 'PUBLIC DOMAIN':
        lic = LicenseOtherPublicDomain()
        d['package.license'] = { 'id': lic.id }
    elif category in ('CONTRACTUAL', 'OTHER',):
        lic = LicenseOtherClosed()
        d['package.license'] = { 'id': lic.id }
    elif category == 'COPYRIGHTED':
        lic = LicenseNotSpecified()
        d['package.license'] = { 'id': lic.id }
    return d

def _handle_contributor(node, namespaces):
    if node is None:
        return {}
    d = {}
    projs = node.xpath('./foaf:Project', namespaces=namespaces)
    text = True
    if len(projs):
        text = False
        idx = 0
        for pro in projs:
            d['project_%i' % idx] = _find_value(pro, 'about')
            # Uncomment and remane keys as needed.
            #n = pro.xpath('./foaf:name', namespaces=namespaces)
            #if len(n):
            #    d['project_name_%i' % idx] = n[0].text
            #n = pro.xpath('./rdfs:comment', namespaces=namespaces)
            #if len(n):
            #    d['project_comment_%i' % idx] = n[0].text
            #    d['project_comment_lang_%i' % idx] = _find_value(n[0], 'lang')
            idx += 1
    # Add iteration over something else when those show up.
    # Questionable but let's say it's just a contributor.
    if text:
        d['contributor'] = node.text
    return d

def _handle_publisher(node, namespaces):
    if node is None:
        return {}
    d = {}
    persons = node.xpath('./foaf:person', namespaces=namespaces)
    if len(persons):
        if len(persons) > 1:
            log.warning('Node with several publishers.')
        url = _find_value(persons[0], 'about')
        ns = persons[0].xpath('./foaf:mbox', namespaces=namespaces)
        email = _find_value(ns[0], 'resource') if len(ns) else None
        ns = persons[0].xpath('./foaf:phone', namespaces=namespaces)
        phone = _find_value(ns[0], 'resource') if len(ns) else None
        if url:
            d['contactURL'] = url
        if phone and len(phone) > 5: # Filter out '-' and other possibilites.
            d['phone'] = phone
        if email:
            d['package.maintainer_email'] = email
    # If not persons, then what is this? Just email? Could be anything?
    return d 

def _oai_dc2ckan(data, namespaces, group, harvest_object):
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
    handled = [ 'title' ]
    for s in ('subject', 'type',):
        for tag in metadata.get(s, ''):
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
    # Handle creators before contributors so that numbering order is ok.
    if 'creator' in metadata and len(metadata['creator']):
        for auth in metadata['creator']:
            extras['organization_%d' % lastidx] = ''
            extras['author_%d' % lastidx] = auth
            lastidx += 1
    extras.update(_handle_contributor(metadata.get('contributorNode'), namespaces))
    extras.update(_handle_publisher(metadata.get('publisherNode'), namespaces))
    # This value belongs to elsewhere.
    if 'package.maintainer_email' in extras:
        pkg.maintainer_email = extras['package.maintainer_email']
        del extras['package.maintainer_email']
    extras.update(_handle_rights(metadata.get('rightsNode'), namespaces))
    if 'package.license' in extras:
        pkg.license = extras['package.license']
        del extras['package.license']
    # The rest.
    # description below goes to pkg.notes. I think it should not added here.
    for key, value in metadata.items():
        if value is None or len(value) == 0 or key in ('title', 'subject', 'type', 'rightsNode', 'publisherNode', 'creator', 'contributorNode',):
            continue
        extras[key] = ' '.join(value)
    pkg.title = title
    # Should everything in the list be joined together? Now this also would
    # go to extras. Surely duplicate is not necessary?
    description = metadata['description'][0] if len(metadata['description']) else ''
    pkg.notes = description
    # Date is missing with low probability. I presume this is adequate.
    # Solved by check and retry in fetch stage.
    #if 'date' not in extras:
    #    extras['date'] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    # Are both needed to have the same value?
    extras['lastmod'] = extras['date']
    pkg.extras = extras
    pkg.url = data['package_url']
    if 'package_resource' in data:
        ofs = get_ofs()
        ofs.put_stream(BUCKET, data['package_xml_save']['label'],
            data['package_xml_save']['xml'], {})
        pkg.add_resource(**(data['package_resource']))
    if harvest_object is not None:
        harvest_object.package_id = pkg.id
        harvest_object.content = None
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
    if group is not None:
        group.add_package_by_name(pkg.name)
        group.save()
    model.repo.commit()
    return True

