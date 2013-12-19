# coding: utf-8
# vi:et:ts=8:

import cStringIO

import oaipmh.common
import lxml.etree
import rdflib

default_namespaces = [
        ('dc', 'http://purl.org/dc/elements/1.1/'),
        ('dct', 'http://purl.org/dc/terms/'),
        ('xsd', 'http://www.w3.org/2001/XMLSchema#'),
        ('rdf', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'),
        ('rdfs', 'http://www.w3.org/2000/01/rdf-schema#'),
        ('skos', 'http://www.w3.org/2004/02/skos/core#'),
        ('owl', 'http://www.w3.org/2002/07/owl#'),
        ('nrd', 'http://purl.org/net/nrd#'),
        ('void', 'http://rdfs.org/ns/void#'),
        ('foaf', 'http://xmlns.com/foaf/0.1/'),
        ('dcat', 'http://www.w3.org/ns/dcat#'),
        ('fp', 'http://downlode.org/Code/RDF/File_Properties/schema#'),
        ('arpfo', 'http://vocab.ox.ac.uk/projectfunding#'),
        ('org', 'http://www.w3.org/ns/org#'),
        ('lvont', 'http://lexvo.org/ontology#'),
        ('qb', 'http://purl.org/linked-data/cube#'),
        ('prov', 'http://www.w3.org/ns/prov#'),
]


def namespaced_name(name, namespaces):
        '''Substitutes a namespace prefix in a URL with its short form.

        :param name: the URL
        :type name: string
        :param namespaces: a list of (short prefix, long prefix) pairs
        :type namespaces: list of (string, string)
        :returns: the URL, with a short prefix
        :rtype: string
        '''
        for prefix, nsurl in namespaces + default_namespaces:
                if prefix is None: prefix = ''
                else: prefix += ':'
                if name.startswith(nsurl): return prefix + name[len(nsurl):]
                nsurl = '{%s}' % nsurl
                if name.startswith(nsurl): return prefix + name[len(nsurl):]
        return name


def namepath_for_element(prefix, name, indices, md):
        '''Helper function to form name paths

        This function takes a prefix and name and concatenates them into
        a "name path".  As a side effect, it also counts the elements with
        a same name path and gives them unique indices, and marks the
        count of such elements in the metadata dictionary.

        :param prefix: the namepath of the parent element
        :type prefix: string
        :param name: the name of the current element
        :type name: string
        :param indices: a hash to keep counts
        :type indices: a hash from strings to integers (inout)
        :param md: a dictionary of metadata keys (namepaths) and values
        :type md: a hash from strings to any type (inout)

        :returns: a new namepath with name appended to prefix
        :rtype: string
        '''
        index = indices.get(name, 0)
        indices[name] = index + 1
        if index != 0:
            return '%s/%s.%d' % (prefix, name, index)
        else:
            return '%s/%s' % (prefix, name)


def generic_xml_metadata_reader(xml_element):
        '''Transform XML documents into metadata dictionaries

        :param xml_element: XML document
        :type xml_element: lxml.etree.Element
        :returns: metadata dictionary with all the content of xml_element
        :rtype: oaipmh.common.Metadata
        '''
        def flatten_with(prefix, element, result):
                '''Recursive traversal of XML tree'''
                if element.text and element.text.strip():
                    result[prefix] = element.text.strip()
                for attr in element.attrib:
                        name = namespaced_name(attr, element.nsmap.items())
                        result['%s/@%s' % (prefix, name)] = element.attrib[attr]
                indices = {}
                for child in element:
                        name = namespaced_name(child.tag, child.nsmap.items())
                        child_path = namepath_for_element(
                            prefix, name, indices, result)
                        flatten_with(child_path, child, result)

        result = {}
        flatten_with(namespaced_name(xml_element.tag, xml_element.nsmap.items()),
                     xml_element, result)
        return oaipmh.common.Metadata(result)


def is_reverse_relation(rel1, rel2):
        '''Tells whether two elements are mutual reverses

        :param rel1: name of relation
        :type rel1: string
        :param rel2: name of relation
        :type rel2: string
        :returns: whether rel1 and rel2 are reverse relations
        :rtype: boolean
        '''
        try: rel1 = rel1[:rel1.rindex('.')]
        except ValueError: pass
        try: rel2 = rel2[:rel2.rindex('.')]
        except ValueError: pass
        return rel1 == 'rev:' + rel2 or rel2 == 'rev:' + rel1


def generic_rdf_metadata_reader(xml_element):
        '''Transform RDF/XML documents into metadata dictionaries

        This function takes an RDF document in XML format, transforms it
        into an RDF graph, and traverses that graph to find all nodes in
        the graph and give them namepaths.

        :param xml_element: RDF/XML document
        :type xml_element: lxml.etree.Element instance
        :returns: metadata dictionary
        :rtype: oaipmh.common.Metadata instance
        '''
        etree = lxml.etree
        g = rdflib.Graph()
        e = etree.ElementTree(xml_element[0])
        ns = dict((prefix, rdflib.Namespace(nsurl))
                        for prefix, nsurl in default_namespaces)
        # this is kinda stupid, but by far the easiest way:
        # rdflib uses xml.sax so it doesn't understand etree,
        # so text is the only common language spoken by lxml and rdflib
        f = cStringIO.StringIO(etree.tostring(e, xml_declaration=True,
                encoding='utf-8'))
        g.parse(f, format='xml') # publicID could be the metadata source URL
        # end stupid

        visited = set()
        def flatten_with(prefix, node, result):
                '''Recursive traversal of RDF graph'''
                path = prefix.split('/')
                if len(path) > 2 and is_reverse_relation(path[-1], path[-2]):
                        return
                result[prefix] = unicode(node)
                if node in visited: return
                visited.add(node)
                if hasattr(node, 'language') and node.language:
                        result[prefix + '/language'] = node.language
                indices = {}
                arcs = [(namespaced_name(str(p), list(g.namespaces())), o)
                                for p, o in g.predicate_objects(node)] + \
                        [('rev:' + namespaced_name(str(p),
                                list(g.namespaces())), s)
                                for s, p in g.subject_predicates(node)]
                for name, child in arcs:
                        child_path = namepath_for_element(prefix, name,
                                        indices, result)
                        flatten_with(child_path, child, result)

        datasets = list(g.subjects(ns['rdf']['type'], ns['nrd']['Dataset']))
        assert len(datasets) == 1
        root_node = datasets[0]
        result = {}
        flatten_with(u'dataset', root_node, result)
        return oaipmh.common.Metadata(result)


def dummy_metadata_reader(xml_element):
        '''A test metadata reader that always returns the same metadata

        :param xml_element: XML input
        :type xml_element: any
        :returns: metadata dictionary
        :rtype: oaipmh.common.Metadata instance
        '''
        return oaipmh.common.Metadata({'test': 'success'})
