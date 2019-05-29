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
PREFIX = '/Users/larsga/data/privat/trad-beer/'
OTHER = '/Users/larsga/cvs-co/fhdb/schema/'
rdffiles = [os.path.join(PREFIX, fname) for fname in [
    'references.ttl',
    'recipes.ttl',
    'danmark/neu/metadata.ttl',
    'norge/neg/liste-35/metadata.ttl',
    'norge/neg/liste-35/herbs.ttl',
    'danmark/neu/topnr.ttl',
    'danmark/uff/metadata.ttl',
    'norge/neg/liste-35/processes.ttl',
    'sverige/luf/metadata.ttl',
    'tyskland/voko/metadata.ttl',
    'finland/sls/sls-820/metadata.ttl',
    'finland/km/metadata.ttl',
    'pagerank.nt',
    'wood-types.ttl',
    'topics.ttl',
    'kveik.ttl',
    'sverige/eu/metadata.ttl',
    'cultures.ttl',
    'yeast-drying.ttl',
    OTHER + 'malt-drying.ttl',
    'dots.ttl',
    'sverige/eu/sp-98/metadata.ttl',
    'strainer-type.ttl',
    OTHER + 'cleaning-agents.ttl',
    OTHER + 'events.ttl',
    'litauen/siauliai-museum/metadata.ttl',
    'estland/erm/metadata.ttl',
]]

# ----- UTILITIES

def load_into_graph(graph, base_url, filename):
    try:
        graph.parse(location = base_url, format = 'n3')
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
    return g.objects(klass, rdflib.URIRef('http://www.w3.org/2000/01/rdf-schema#subClassOf'))

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
    try:
        if url.endswith(u'.txt'):
            return codecs.open(tofilename(url), 'r', 'utf-8').read()
        elif url.endswith(u'.pdf'):
            if os.path.exists('/tmp/pdf.txt'):
                os.unlink('/tmp/pdf.txt')
            os.system('pdftotext %s /tmp/pdf.txt' % tofilename(url))
            if os.path.exists('/tmp/pdf.txt'):
                return codecs.open('/tmp/pdf.txt', 'r', 'iso-8859-1').read()
        else:
            print "Can't extract content from %s" % url
    except IOError, e:
        print "Can't extract content from %s: %s" % (url, e)
    except UnicodeDecodeError, e:
        print "Bad UTF-8 encoding in %s: %s" % (url, e)

def tofilename(url):
    if url.startswith('file://'):
        url = url[7 : ]

    return url.replace('%20', ' ').encode('utf-8')

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

def exists(s):
    return bool(g.predicate_objects(s))

# ----- PREPARE

props = set() # tells us whether we must create the property or not
schema = Schema(url = ID(stored = True),
                name = TEXT(stored = True),
                content = TEXT,
                link = KEYWORD(stored = True),
                refers_to = KEYWORD, # references to other resources go here
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
        add_value(obj, 'content', retrieve_content(unicode(s)))

    for p, o in g.predicate_objects(s):
        if is_link(p):
            set_value(obj, 'link', unicode(o))

        if is_http_uri(o):
            add_value(obj, 'content', find_label(o))
            add_dynamic_property(obj, p, o, True)
            if exists(o):
                add_value(obj, 'refers_to', unicode(o))
        elif is_file_uri(o):
            add_value(obj, 'content', retrieve_content(unicode(o)))
            add_dynamic_property(obj, p, o, True)
            if exists(o):
                add_value(obj, 'refers_to', unicode(o))
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
