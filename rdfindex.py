'''
Indexes up RDF data into a Whoosh index.
'''

import os.path, codecs, urllib
from pprint import pprint
from whoosh.index import create_in, open_dir
from whoosh.fields import *

import rdflib

# ----- CONFIG

indexpath = 'index'
rdffiles = ['/Users/lars.garshol/data/privat/trad-beer/references.ttl',
            '/Users/lars.garshol/data/privat/trad-beer/danmark/neu/metadata.ttl',
            '/Users/lars.garshol/data/privat/trad-beer/norge/neg/liste-35/metadata.ttl']

# ----- UTILITIES

def load_into_graph(graph, base_url, filename):
    try:
        graph.parse(location = base_url, file = open(filename), format = 'n3')
    except rdflib.exceptions.ParserError:
        print "Parse error in", filename
        return False

    return g

def tourl(filename):
    assert filename[0] == '/'
    return 'file://' + filename

def resource_is_link(sub):
    return False

def is_link(sub):
    return False

def is_http_uri(o):
    return isinstance(o, rdflib.URIRef) and (str(o).startswith('http://') or
                                             str(o).startswith('https://'))

def is_file_uri(o):
    return isinstance(o, rdflib.URIRef) and str(o).startswith('file://')

def add_value(obj, field, value):
    values = obj.get(field, [])
    values.append(value)
    obj[field] = values

def find_label(uri):
    return lookup(g, o, 'http://www.w3.org/2000/01/rdf-schema#label')

def lookup(graph, subject, predicate):
    for o in graph.objects(subject, rdflib.URIRef(predicate)):
        return o.value

def retrieve_content(url):
    if url.endswith(u'.txt'):
        return codecs.open(tofilename(o), 'r', 'utf-8').read()
    elif url.endswith(u'.pdf'):
        if os.path.exists('/tmp/pdf.txt'):
            os.unlink('/tmp/pdf.txt')
        os.system('pdftotext %s /tmp/pdf.txt' % tofilename(o))
        if os.path.exists('/tmp/pdf.txt'):
            return codecs.open('/tmp/pdf.txt', 'r', 'iso-8859-1').read()
    else:
        print "Can't extract content from %s" % o

def tofilename(url):
    if url.startswith('file://'):
        url = url[7 : ]

    return url.replace('%20', ' ')

def belongs_to_class(uri, klass):
    for k in g.objects(uri, rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')):
        if str(k) == klass:
            return True

def is_name_property(uri):
    return belongs_to_class(uri, 'http://psi.garshol.priv.no/2015/rdfsearch/NameProperty')

def is_description_property(uri):
    return belongs_to_class(uri, 'http://psi.garshol.priv.no/2015/rdfsearch/DescriptionProperty')

# ----- PREPARE

schema = Schema(url = ID(stored = True),
                name = TEXT(stored = True),
                content = TEXT,
                link = KEYWORD,
                description = TEXT(stored = True))

if not os.path.exists(indexpath):
    os.mkdir(indexpath)
ix = create_in(indexpath, schema)

writer = ix.writer()

# ----- LOAD RDF

g = rdflib.Graph()
for file in rdffiles:
    load_into_graph(g, tourl(file), file)

# ----- INDEX THE RDF

subjects = set(g.subjects())
for s in subjects:
    print s

    obj = {'url' : unicode(s)}
    if resource_is_link(s):
        set_value(obj, 'link', unicode(o))

    for p, o in g.predicate_objects(s):
        if is_link(p):
            set_value(obj, 'link', unicode(o))

        if is_http_uri(o):
            add_value(obj, 'content', find_label(o))
            # FIXME: this is for the faceting
            # p = mkprop(p, KEYWORD)
            # add_value(obj, p, unicode(o))
        elif is_file_uri(o):
            add_value(obj, 'content', retrieve_content(unicode(o)))
        elif isinstance(o, rdflib.Literal):
            add_value(obj, 'content', o.value)
            if is_name_property(p):
                add_value(obj, 'name', o.value)
            elif is_description_property(p):
                add_value(obj, 'description', o.value)
        else:
            print 'OOPS', p, o

    for prop, value in obj.items():
        if isinstance(value, list):
            v = ' '.join([unicode(v) for v in value if v])
            if v:
                obj[prop] = v
            else:
                del obj[prop]
    if not obj.has_key('name'):
        obj['name'] = unicode(s)

    writer.add_document(**obj)

writer.commit()
