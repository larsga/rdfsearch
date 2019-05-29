'''
Runs the search interface. Run it with

  python ui.py [port number] [index directory]
'''

import os.path, sys, codecs
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
        extension = url.split('.')[-1]
        charset = ''
        if extension in ('html', 'txt'):
            charset = '; charset=utf-8'
            content = codecs.open(url[7 : ], 'r', 'utf-8').read()
        else:
            content = open(url[7 : ], 'r').read()

        web.header('Content-type', mime[extension] + charset)
        return content

class Show:
    def GET(self):
        url = web.input().get('url')
        page = int(web.input().get('page', 1))

        qp = QueryParser('url', schema = ix.schema)
        q = qp.parse(url)
        r = searcher.search(q, limit = 1)
        doc = list(r)[0]

        qp = QueryParser('refers_to', schema = ix.schema)
        q = qp.parse(url)
        refs = searcher.search_page(q, page, pagelen = PAGE_SIZE)
        refs = Pager(page, refs)

        return render.show(doc, refs, DocumentSearcher(ix), url)

class Pager:

    def __init__(self, page, results):
        self._page = page
        self._results = results

        if results.is_last_page():
            self._pages = page
        else:
            self._pages = page + 1

    def __getitem__(self, no):
        return self._results[no]

    def is_last_page(self):
        return self._page == self._pages

    def get_next_page(self):
        return self._page + 1

render = web.template.render(os.path.join('.', 'templates/'),
                             base = 'base')
app = web.application(urls, globals(), autoreload = False)
app.run()
