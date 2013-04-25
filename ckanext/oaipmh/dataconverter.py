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

import pprint

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

def _handle_rights(nodes, namespaces):
    d = {}
    lic_url_idx = 0
    lic_text_idx = 0
    for node in nodes:
        decls = node.xpath('./*[local-name() = "RightsDeclaration"]',
            namespaces=namespaces)
        if len(decls):
            if len(decls) > 1:
                # This is actually repeatable but not handled so thus far.
                # Package.license field does not allow for multiple values.
                # Convert to loop once multiple licenses are handled.
                log.warning('Multiple RightsDeclarations in one record.')
            category = decls[0].get('RIGHTSCATEGORY')
            text = decls[0].text
        else: # Probably just old-fashioned text.
            text = node.text
            category = 'LICENSED' # Let's give recognizing the license a try.
        if category == 'LICENSED' and text:
            lic = _match_license(text)
            if lic is not None:
                d['package.license'] = { 'id':lic }
            else:
                # Something unknown. Store text or license.
                if text.startswith('http://'):
                    d['licenseURL_%i' % lic_url_idx] = text
                    lic_url_idx += 1
                else:
                    d['licenseText_%i' % lic_text_idx] = text
                    lic_text_idx += 1
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

def _handle_contributor(nodes, namespaces):
    d = {}
    contr_idx = 0
    proj_idx = 0
    for node in nodes:
        # Add iteration over something else when those show up.
        projs = node.xpath('./foaf:Project', namespaces=namespaces)
        if len(projs):
            for pro in projs:
                d['project_%i' % proj_idx] = _find_value(pro, 'about')
                proj_idx += 1
        elif node.text: # Plain text field has none of the above.
            d['contributor_%i' % contr_idx] = node.text
            contr_idx += 1
    return d

def _handle_publisher(nodes, namespaces):
    d = {}
    person_idx = 0
    for node in nodes:
        persons = node.xpath('./foaf:person', namespaces=namespaces)
        for p in persons:
            url = _find_value(p, 'about')
            ns = p.xpath('./foaf:mbox', namespaces=namespaces)
            email = _find_value(ns[0], 'resource') if len(ns) else None
            ns = p.xpath('./foaf:phone', namespaces=namespaces)
            phone = _find_value(ns[0], 'resource') if len(ns) else None
            if url:
                d['contactURL_%i' % person_idx] = url
            if phone and len(phone) > 5: # Filter out '-' and similar.
                d['phone_%i' % person_idx] = phone
            if email and person_idx == 0: # Just keep first. The rest later?
                d['package.maintainer_email'] = email
            person_idx += 1
        # If not persons, then what is this? Apparently just text. Ignore?
        # Can be name of an organization.
    return d 

def _oai_dc2ckan(data, namespaces, group, harvest_object):
    #pprint.pprint(data)
    model.repo.new_revision()
    identifier = data['identifier']
    metadata = data['metadata']
    title = metadata['title'][0] if len(metadata['title']) else identifier
    name = data['package_name']
    pkg = Package.get(name)
    if not pkg:
        pkg = Package(name=name, title=title, id=identifier)
        pkg.save()
        setup_default_user_roles(pkg)
    else:
        log.debug('Updating: %s' % name)
        # There are old resources which are replaced by new ones if they are
        # relevant anymore so "delete" all existing resources now.
        for r in pkg.resources:
            r.state = 'deleted'
    extras = {}
    lastidx = 0
    # Is 'type' here needed at all?
    for s in ('subject', 'type',):
        for tag in metadata.get(s, []):
            # Turn each subject or type field into it's own tag.
            tagi = tag.strip()
            tagi = munge_tag(tagi[:100]) # 100 char limit in DB.
            tag_obj = model.Tag.by_name(tagi)
            if not tag_obj:
                tag_obj = model.Tag(name=tagi)
                tag_obj.save()
            pkgtag = model.Session.query(model.PackageTag).filter(
                model.PackageTag.package_id==pkg.id).filter(
                model.PackageTag.tag_id==tag_obj.id).limit(1).first()
            if pkgtag is None:
                pkgtag = model.PackageTag(tag=tag_obj, package=pkg)
    # Handle creators before contributors so that numbering order is ok.
    for auth in metadata.get('creator', []):
        extras['organization_%d' % lastidx] = ''
        extras['author_%d' % lastidx] = auth
        lastidx += 1
    extras.update(
        _handle_contributor(metadata.get('contributorNode', []), namespaces))
    extras.update(
        _handle_publisher(metadata.get('publisherNode', []), namespaces))
    # This value belongs to elsewhere.
    if 'package.maintainer_email' in extras:
        pkg.maintainer_email = extras['package.maintainer_email']
        del extras['package.maintainer_email']
    extras.update(_handle_rights(metadata.get('rightsNode', []), namespaces))
    if 'package.license' in extras:
        pkg.license = extras['package.license']
        del extras['package.license']
    # There may be multiple identifiers (URL, ISBN, ...) in the metadata.
    id_idx = 0
    for ident in metadata.get('identifier', []):
        extras['identifier_%i' % id_idx] = ident
        id_idx += 1
    # The rest.
    # description below goes to pkg.notes. I think it should not added here.
    for key, value in metadata.items():
        if value is None or len(value) == 0 or key in ('title', 'subject', 'type', 'rightsNode', 'publisherNode', 'creator', 'contributorNode', 'description', 'identifier',):
            continue
        extras[key] = ' '.join(value)
    #description = metadata['description'][0] if len(metadata['description']) else ''
    notes = ' '.join(metadata.get('description', []))
    pkg.notes = notes.replace('\n', ' ').replace('  ', ' ')
    pkg.version = extras['date']
    del extras['date']
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
    # Metadata may have different identifiers, pick link, if exists.
    url = ''
    for ids in metadata['identifier']:
        if ids.startswith('http://'):
            pkg.add_resource(url, name=pkg.title, format='html')
            break # Previous code added only one so break.
    # All belong to the main group even if they do not belong to any set.
    if group is not None:
        group.add_package_by_name(pkg.name)
    model.repo.commit()
    return True

