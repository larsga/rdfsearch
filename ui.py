'''
Runs the search interface. Run it with

  python ui.py [port number] [index directory]
'''

import os.path, sys
import web
from whoosh.index import open_dir
from whoosh.searching import Searcher
from whoosh.qparser import QueryParser
from whoosh.query import Every, Term, And

ix = open_dir(sys.argv[2])
searcher = ix.searcher()
qp = QueryParser('content', schema = ix.schema)

urls = (
    '/', 'Index',
    '/search', 'Search',
    '/download', 'Download',
    '/show', 'Show',
    )

PAGE_SIZE = 25

def nocache():
    web.header("Content-Type","text/html; charset=utf-8")
    web.header("Pragma", "no-cache");
    web.header("Cache-Control", "no-cache, no-store, must-revalidate, post-check=0, pre-check=0");
    web.header("Expires", "Tue, 25 Dec 1973 13:02:00 GMT");

def extract_name(key):
    pos = key.rfind('/')
    return key[pos + 1 : ].replace('_', ' ')

class DocumentSearcher:

    def __init__(self, ix):
        self._qp = QueryParser('url', schema = ix.schema)

    def __getitem__(self, url):
        q = self._qp.parse(url)
        r = list(searcher.search(q, limit = 1))
        if r:
            return r[0]['name']

# ----- PAGES

class Index:
    def GET(self):
        nocache()
        return render.index()

class Search:
    def GET(self):
        page = int(web.input().get('page', 1))

        query = web.input().get('query')
        q = make_query(query)

        results = searcher.search_page(q, page, pagelen = PAGE_SIZE)
        return render.search(results, query)

def make_query(query):
    return qp.parse(query)

mime = {
    'pdf' : 'application/pdf',
    'txt' : 'text/plain',
}

class Download:
    def GET(self):
        url = web.input().get('url')
        web.header('Content-type', mime[url.split('.')[-1]])
        return open(url[7 : ]).read()

class Show:
    def GET(self):
        url = web.input().get('url')

        qp = QueryParser('url', schema = ix.schema)
        q = qp.parse(url)
        r = searcher.search(q, limit = 1)
        doc = list(r)[0]

        qp = QueryParser('references', schema = ix.schema)
        q = qp.parse(url)
        refs = searcher.search(q, limit = 25)

        return render.show(doc, refs, DocumentSearcher(ix))

render = web.template.render(os.path.join('.', 'templates/'),
                             base = 'base')
app = web.application(urls, globals(), autoreload = False)
app.run()
