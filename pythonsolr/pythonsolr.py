from pysolr import *
import socket
import logging
log = logging.getLogger('solr')
from contextlib import contextmanager

class SolrResultsPaginator(object):
    """Offers an iterator on Solr results.  You simply provide a
    configured pysolr `Solr` instance, a query, and the default parameters.
    This class takes care of the rest.  Example use:

        >>> results = SolrResultsPaginator(Solr(SOLR_URL))
        >>> assert len(results) == len([result for result in results])
    """

    def __init__(self, solr, query="*:*", default_params=None, max_index=None):
        # store solr instance and query so that we can re-query for future pages
        self.solr = solr
        self.query = query
        default_params = dict() \
                if default_params is None \
                else default_params
        self.default_params = default_params

        # initialize paginator
        self.initialized = False
        self.exhausted = False
        self.index = 0
        self.max_index = max_index

    def _init_if_needed(self):
        if not self.initialized:
            self.move_to_next_page()
            self.initialized = True

    def _check_max_index(self):
        if self.max_index is None: 
            return
        if self.index > self.max_index:
            raise StopIteration()
        
    def __iter__(self):
        if self.exhausted:
            # reset
            return SolrResultsPaginator(self.solr, self.query, self.default_params)
        return self

    def __len__(self):
        # hits stores the total documents in the total result set, so it's what we want
        # there is no way to get the size of a single page; this entirely depends upon
        # your "rows" parameter, anyway.
        self._init_if_needed()
        return self.page.hits

    def next(self):
        self._check_max_index()
        self._init_if_needed()
        self._next()
        self.index += 1
        return self.cursor

    def _next(self):
        try:
            # advance the cursor forward
            self.cursor = self.item_iter.next()
        except StopIteration:
            # we are either at the end of the page or the result set
            try:
                # attempt to move to the next page
                self.move_to_next_page()
                # if we are at the end of the result set, page will come back empty
                assert len(self.page.docs) != 0
            except:
                # end of result set, so we stop the iteration
                self.exhausted = True
                raise StopIteration()
            # page has advanced successfully, so let's re-call next()
            self.cursor = self._next()
        # cursor has advanced, so we advance the index #
        return self.cursor

    def move_to_next_page(self):
        # use default parameters, and then overwrite "start" with the one calculated
        # by the paginator
        query_params = dict(self.default_params)
        # if first page, index is 0, otherwise we attempt to get the first doc on next page
        query_params["start"] = self.index
        # fire the solr search
        self.page = self.solr.search(self.query, **query_params)
        # use a generator that will yield each of the items in docs
        self.item_iter = (item for item in self.page.docs)

    def __unicode__(self):
        fmt = u"SolrResultsPaginator(hits={hits}, solr={solr}, query={query}, default_params={default_params})"
        return fmt.format(hits=len(self), **vars(self))

    __repr__ = __unicode__


class PythonSolr(Solr):
    def __init__(self, url='http://127.0.0.1:8983/solr/', decoder=None, timeout=60):
        super(PythonSolr, self).__init__(url, decoder, timeout)
        
    def search(self, q, **kwargs):
        results = super(PythonSolr, self).search(q, **kwargs)
        # replace results paginator with our custom paginator
        results.paginator = PythonSolrResults(self, query=q, default_params=kwargs)
        log.debug(u"ParselySolr: starting Solr search with query={query} and params={params}".format(
            query=q, params=kwargs)) 
        return results

class PythonSolrResults(SolrResultsPaginator):
    def __init__(self, solr=Solr('http://127.0.0.1:8983/solr/'), query="*:*", default_params=None, max_index=None):
        if default_params is None:
            default_params = {"rows": "100"} 
        else:
            if "rows" not in default_params:
                default_params["rows"] = 100
        super(PythonSolrResults, self).__init__(solr, query, default_params, max_index)

    def move_to_next_page(self):
        index = 0
        hits = "NA"
        if self.initialized:
            index = self.index
            hits = len(self)
        fmt = u"ParselySolrResults: moving to next page: "
        fmt += u"index={index} | total={hits} | query={query} | params={params}"
        log.debug(fmt.format(
            index=index, hits=hits, query=self.query, params=self.default_params))
        super(PythonSolrResults, self).move_to_next_page()

    def __unicode__(self):
        unicode_super = super(PythonSolrResults, self).__unicode__()
        return unicode_super.replace("SolrResultsPaginator", "ParselySolrResults")

    __repr__ = __unicode__

@contextmanager
def solr_batch_adder(solr, batch_size=500, auto_commit=False):
    """Meant to be used with a `with_statement`, so that you don't forget to flush the 
    `SolrBatchAdder` after adding a bunch of documents to it.  Example use:

    >>> with solr_batch_add(solr) as batcher:
            for document in documents:
                del document['unnecessary']
                batcher.add_one(document)

    The result of this will be one call of `batcher.add_one()` for each document, and, at the end,
    a call to `batcher.flush()` and `batcher.commit()`.  Since this context manager automatically
    commits at the end, we have `auto_commit` to false in our kwargs.
    """
    batcher = SolrBatchAdder(solr, batch_size, auto_commit)
    try:
        yield batcher
    finally:
        log.info("solr_batch_adder: flushing last few items in batch")
        batcher.flush()
        
class SolrBatchAdder(object):
    def __init__(self, solr, batch_size=100, auto_commit=True):
        """Provides an abstraction for batching commits to the Solr index when processing
        documents with pysolr.  `SolrBatchAdder` maintains an internal "batch" list, and
        when it reaches `batch_size`, it will commit the batch to Solr.  This allows for
        overall better performance when committing large numbers of documents.

        `batch_size` is 100 by default; different values may yield different performance 
        characteristics, and this of course depends upon your average document size and 
        Solr schema.  But 100 seems to improve performance significantly over single commits."""
        self.solr = solr
        self.batch = list()
        self.batch_len = 0
        self.batch_size = batch_size
        self.auto_commit = auto_commit

    def add_one(self, doc):
        """Adds a single document to the batch adder, committing only if we've reached batch_size."""
        self._append_commit(doc)

    def add_multi(self, docs_iter):
        """Iterates through `docs_iter`, appending each document to the batch adder, committing mid-way
        if batch_size is reached."""
        assert hasattr(docs_iter, "__iter__"), "docs_iter must be iterable"
        for doc in docs_iter:
            self._append_commit(doc)

    def flush(self):
        """Flushes the batch queue of the batch adder; necessary after 
        successive calls to `add_one` or `add_multi`."""
        batch_len = len(self.batch)
        auto_commit = self.auto_commit
        log.debug("SolrBatchAdder: flushing {batch_len} articles to Solr (auto_commit={auto_commit})".format(
            batch_len=batch_len, auto_commit=auto_commit))
        try:
            self.solr.add(self.batch, commit=auto_commit)
        except:
            log.exception("Exception encountered when committing batch, falling back on one-by-one commit")
            # one by one fall-back
            for item in self.batch:
                try:
                    self.solr.add([item], commit=False)
                except:
                    log.error(u"Could not add item to solr index")
                    log.exception(u"Exception stack trace for adding item")
            if auto_commit:
                self.commit()

        self.batch = list()
        self.batch_len = 0

    def commit(self):
        try:
            self.solr.commit()
        except socket.timeout:
            log.warning("SolrBatchAdder timed out when committing, but it's safe to ignore")

    def _append_commit(self, doc):
        if self.batch_len == self.batch_size:
            # flush first, because we are at our batch size
            self.flush()
        self._add_to_batch(doc)

    def _add_to_batch(self, doc):
        self.batch.append(doc)
        self.batch_len += 1

    def __unicode__(self):
        fmt = "SolrBatchAdder(batch_size={batch_size}, batch_len={batch_len}, solr={solr}"
        return fmt.format(**vars(self))

 
