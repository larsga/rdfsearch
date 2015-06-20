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
            '/Users/lars.garshol/data/privat/trad-beer/norge/neg/liste-35/metadata.ttl',
            '/Users/lars.garshol/data/privat/trad-beer/norge/neg/liste-35/herbs.ttl',
            '/Users/lars.garshol/data/privat/trad-beer/danmark/neu/topnr.ttl',
            '/Users/lars.garshol/data/privat/trad-beer/danmark/uff/metadata.ttl']

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

def get_classes(uri):
    return g.objects(uri, rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'))

def get_superclasses(klass):
    return g.objects(klass, rdflib.URIRef('http://www.w3.org/2000/01/rdf-schema#subclassOf'))

def resource_is_link(uri):
    for k in get_classes(uri):
        for s in get_superclasses(k):
            if str(s) == 'http://psi.garshol.priv.no/2015/rdfsearch/Resource':
                return True

def is_link(property):
    return belongs_to_class(property, 'http://psi.garshol.priv.no/2015/rdfsearch/LinkProperty')

def is_http_uri(o):
    return isinstance(o, rdflib.URIRef) and \
        (unicode(o).startswith(u'http://') or
         unicode(o).startswith(u'https://'))

def is_file_uri(o):
    return isinstance(o, rdflib.URIRef) and unicode(o).startswith(u'file://')

def add_value(obj, field, value):
    values = obj.get(field, [])
    values.append(value)
    obj[field] = values

def set_value(obj, field, value):
    obj[field] = value

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
    for k in get_classes(uri):
        if str(k) == klass:
            return True

def is_name_property(uri):
    return belongs_to_class(uri, 'http://psi.garshol.priv.no/2015/rdfsearch/NameProperty')

def is_description_property(uri):
    return belongs_to_class(uri, 'http://psi.garshol.priv.no/2015/rdfsearch/DescriptionProperty')

def get_property_name(uri):
    pos = max(uri.rfind('/'), uri.rfind('#'))
    name = uri[pos + 1 : ]
    if name in ('link', 'url', 'name', 'description'):
        name += '_'
    return name

def add_dynamic_property(obj, p, o, keyword):
    global writer
    prop = get_property_name(str(p))
    if keyword:
        add_value(obj, prop, unicode(o))
    else:
        add_value(obj, prop, o.value)
    if not prop in props:
        # must add the property to the schema
        writer.commit()
        writer = ix.writer()

        if keyword:
            config = KEYWORD(stored = True)
        else:
            config = TEXT(stored = True)

        writer.add_field(prop, config)
        props.add(prop)

# ----- PREPARE

props = set() # tells us whether we must create the property or not
schema = Schema(url = ID(stored = True),
                name = TEXT(stored = True),
                content = TEXT,
                link = KEYWORD(stored = True),
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
        set_value(obj, 'link', unicode(s))

    for p, o in g.predicate_objects(s):
        if is_link(p):
            set_value(obj, 'link', unicode(o))

        if is_http_uri(o):
            add_value(obj, 'content', find_label(o))
            add_dynamic_property(obj, p, o, True)
        elif is_file_uri(o):
            add_value(obj, 'content', retrieve_content(unicode(o)))
            add_dynamic_property(obj, p, o, True)
        elif isinstance(o, rdflib.Literal):
            add_value(obj, 'content', o.value)
            if is_name_property(p):
                add_value(obj, 'name', o.value)
            elif is_description_property(p):
                add_value(obj, 'description', o.value)
            else:
                add_dynamic_property(obj, p, o, False)
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
