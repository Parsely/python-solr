# -*- coding: utf-8 -*-
"""
All we need to create a Solr connection is a url.

>>> conn = Solr('http://127.0.0.1:8983/solr/single')

First, completely clear the index.

>>> conn.delete(q='*:*')

For now, we can only index python dictionaries. Each key in the dictionary
will correspond to a field in Solr.

>>> docs = [
...     {'id': 1,  'name': 'document 1', 'text': u'Paul Verlaine'},
...     {'id': 2,  'name': 'document 2', 'text': u'Владимир Маякoвский'},
...     {'id': 3,  'name': 'document 3', 'text': u'test'},
...     {'id': 4,  'name': 'document 4', 'text': u'test'}
... ]


We can add documents to the index by passing a list of docs to the connection's
add method.

>>> conn.add(docs)

>>> results = conn.search('Verlaine')
>>> len(results)
1

>>> results = conn.search(u'Владимир')
>>> len(results)
1


Simple tests for searching. We can optionally sort the results using Solr's
sort syntax, that is, the field name and either asc or desc.

>>> results = conn.search('test', sort='id asc')
>>> for result in results:
...     print result['name']
document 3
document 4

>>> results = conn.search('test', sort='id desc')
>>> for result in results:
...     print result['name']
document 4
document 3


To update documents, we just use the add method.

>>> docs = [
...     {'id': 4, 'name': 'document 4', 'text': u'blah'}
... ]
>>> conn.add(docs)

>>> len(conn.search('blah'))
1
>>> len(conn.search('test'))
1


We can delete documents from the index by id, or by supplying a query.

>>> conn.delete(id=1)
>>> conn.delete(q='name:"document 2"')

>>> results = conn.search('Verlaine')
>>> len(results)
0


Docs can also have multiple values for any particular key. This lets us use
Solr's multiValue fields.

>>> docs = [
...     {'id': 'testdoc.5', 'cat': ['poetry', 'science'], 'name': 'document 5', 'text': u''},
...     {'id': 'testdoc.6', 'cat': ['science-fiction',], 'name': 'document 6', 'text': u''},
... ]

>>> conn.add(docs)
>>> results = conn.search('cat:"poetry"')
>>> for result in results:
...     print result['name']
document 5

>>> results = conn.search('cat:"science-fiction"')
>>> for result in results:
...     print result['name']
document 6

>>> results = conn.search('cat:"science"')
>>> for result in results:
...     print result['name']
document 5

For faceted search, you need to be careful with the formatting as it
has a period in the needed parameters.  Also, you can limit the results
with a 'rows' command.

>>> results = conn.search(['hello','cat:"science"'],facet="on", **{'facet.field':['cat'],'rows':10,'start':0})
>>> print results.facets
{u'facet_fields': {u'cat': [u'poetry', 0, u'science', 0, u'science-fiction', 0]}, u'facet_dates': {}, u'facet_queries': {}}

"""

# TODO: unicode support is pretty sloppy. define it better.

from urllib import urlencode
from urlparse import urljoin, urlsplit
from datetime import datetime, date
import re

try:
    # for python 2.5
    from xml.etree import cElementTree as ET
except ImportError:
    try:
        # use etree from lxml if it is installed
        from lxml import etree as ET
    except ImportError:
        try:
            # use cElementTree if available
            import cElementTree as ET
        except ImportError:
            try:
                from elementtree import ElementTree as ET
            except ImportError:
                raise ImportError("No suitable ElementTree implementation was found.")

try:
    # For Python < 2.6 or people using a newer version of simplejson
    import simplejson as json
except ImportError:
    # For Python >= 2.6
    import json

try:
    # Desirable from a timeout perspective.
    from httplib2 import Http
    TIMEOUTS_AVAILABLE = True
except ImportError:
    from httplib import HTTPConnection
    TIMEOUTS_AVAILABLE = False

try:
    set
except NameError:
    from sets import Set as set

__author__ = 'Joseph Kocherhans, Jacob Kaplan-Moss, Daniel Lindsley'
__all__ = ['Solr']
__version__ = (2, 0, 9)

def get_version():
    return "%s.%s.%s" % __version__

DATETIME_REGEX = re.compile('^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})(\.\d+)?Z$')
ER_RE = re.compile ('<pre>(.|\n)*?</pre>')

class SolrError(Exception):
    pass

def list2dict(data):
    # convert : [u'tf', 1, u'df', 2, u'tf-idf', 0.5]
    # to a dict
    stop = len(data)
    keys = [data[i] for i in range(stop) if i%2==0]
    values = [data[i] for i in range(stop) if i%2==1]
    return dict(zip(keys,values))

# Solr/Lucene special characters: + - ! ( ) { } [ ] ^ " ~ * ? : \
# There are also operators && and ||, but we're just going to escape
# the individual ampersand and pipe chars.
# Also, we're not going to escape backslashes!
# http://lucene.apache.org/java/2_9_1/queryparsersyntax.html#Escaping+Special+Characters
ESCAPE_CHARS_RE = re.compile(r'(?<!\\)(?P<char>[&|+\-!(){}[\]^"~*?:])')

def solr_escape(value):
    r"""Escape un-escaped special characters and return escaped value.
    
    >>> solr_escape(r'foo+') == r'foo\+'
    True
    >>> solr_escape(r'foo\+') == r'foo\+'
    True
    >>> solr_escape(r'foo\\+') == r'foo\\+'
    True
    """
    return ESCAPE_CHARS_RE.sub(r'\\\g<char>', value)

class TermVectorResult(object):
    def __init__(self,field,response=None,decoder=None):
        self.decoder = decoder or json.JSONDecoder()
        result = self.decoder.decode(response)
    
        # term vectors from /tvrh
        if result.get('termVectors'):
            tv = result['termVectors'][1]
            tv = list2dict(tv)
            
            fields = tv.keys()
            if 'uniqueKey' in fields:
                fields.remove('uniqueKey')
            self.tv = {}
            for f in fields:
                res = list2dict(tv[f])
                res2 = {}
                for k,v in res.iteritems():
                    res3 = list2dict(v)
                    res3['field'] = f
                    res2[k] = res3
                self.tv.update(res2)
                        
        self.docs = result['response']['docs']

    def __len__(self):
        return len(self.docs)

    def __iter__(self):
        return iter(self.docs)

class Results(object):
    def __init__(self, response=None,decoder=None):
        self.decoder = decoder or json.JSONDecoder()
        if not response:
            self.result = {}
        else:
            self.result =  self.decoder.decode(response)
        
        self.highlighting = {}
        self.facets = {}
        self.spellcheck = {}
        self.matches = {}
        self.interesting_terms = {}
        self.response = response
            
        if self.result.get('highlighting'): # highlighting
            self.highlighting = self.result['highlighting']
        
        if self.result.get('facet_counts'):
            self.facets = self.result['facet_counts']
        
        if self.result.get('spellcheck'):
            self.spellcheck = self.result['spellcheck']
    
        if self.result.get('interestingTerms'):
            self.interesting_terms = self.result["interestingTerms"]
        
        if self.result.get('match',{}).get('docs'):
            self.matches = self.result['match']['docs']

        response = self.result.get('response')
        if response:
            self.docs = response['docs']
            self.hits = response['numFound']
        else:
            self.docs, self.hits = ([],0)
    
    def __len__(self):
        return len(self.docs)

    def __iter__(self):
        return iter(self.docs)

class GroupedResults(object):
    def __init__(self, response=None,decoder=None):
        self.decoder = decoder or json.JSONDecoder()
        self.result = self.decoder.decode(response)

        grouped_response = self.result.get('grouped')
        docs = {}
        for name,res in grouped_response.iteritems():
            r = Results()
            r.docs = res.get('doclist',{}).get('docs')
            r.hits = res.get('doclist',{}).get('numFound')
            docs[name] = r
                                       
        self.docs = docs
                    
    def __iter__(self):
        return iter(self.docs)

class Solr(object):
    """
    An object that makse http json requests to a Solr server.
    
    If you have httplib2 installed we will cache the responses we get from
    Solr in a directory called '.cache'. The cache can also be an object that subclases httplib2.FileCache
    Not safe to use if multiple threads or processes are going to be running on the same cache.
    """
    def __init__(self, url, decoder=None, timeout=60,result_class=Results,use_cache=None,cache=None):
        self.decoder = decoder or json.JSONDecoder()
        self.url = url
        self.scheme, netloc, path, query, fragment = urlsplit(url)
        netloc = netloc.split(':')
        self.host = netloc[0]
        if len(netloc) == 1:
            self.host, self.port = netloc[0], None
        else:
            self.host, self.port = netloc
        self.path = path.rstrip('/')
        self.timeout = timeout
        self.result_class = result_class
        if TIMEOUTS_AVAILABLE and use_cache:
            self.http = Http(cache=cache or ".cache",timeout=self.timeout)
        else:
            self.http = Http(timeout=self.timeout)
            
    def _send_request(self, method, path, body=None, headers=None):
        if TIMEOUTS_AVAILABLE:
            url = self.url.replace(self.path, '')
            headers, response = self.http.request(urljoin(url, path), method=method, body=body, headers=headers)
            
            if int(headers['status']) not in (200,304):
                raise SolrError(self._extract_error(headers, response))
            
            return response
        else:
            if headers is None:
                headers = {}
            
            conn = HTTPConnection(self.host, self.port)
            conn.request(method, path, body, headers)
            response = conn.getresponse()
                        
            if response.status != 200:
                raise SolrError(self._extract_error(dict(response.getheaders()), response.read()))
            
            return response.read()

    def _select(self, params):
        # encode the query as utf-8 so urlencode can handle it
        params['q'] = self._encode_q(params['q'])
        params['wt'] = 'json' # specify json encoding of results
        path = '%s/select/?%s' % (self.path, urlencode(params, True))
        return self._send_request('GET', path)

    def _select_post(self,params):
        """
        Send a query via HTTP POST. Useful when the query is long (> 1024 characters)
        """
        params['q'] = self._encode_q(params['q'])
        params['wt'] = 'json' 
        path = '%s/select?' % (self.path,)
        
        headers = {"Content-type": "application/x-www-form-urlencoded"}
        body = urlencode(params, False)
        return self._send_request('POST',path,body=body,headers=headers)
    
    def _mlt(self, params):
        # encode the query as utf-8 so urlencode can handle it
        params['q'] = self._encode_q(params['q'])
        params['wt'] = 'json' # specify json encoding of results
        path = '%s/mlt?%s' % (self.path, urlencode(params, True))
        return self._send_request('GET', path)

    def _tvrh(self, params):
        # encode the query as utf-8 so urlencode can handle it
        params['q'] = self._encode_q(params['q'])
        params['wt'] = 'json' # specify json encoding of results
        path = '%s/tvrh?%s' % (self.path, urlencode(params, True))
        return self._send_request('GET', path)

    def _encode_q(self,qarg):
        if type(qarg) == list:
            return [q.encode('utf-8') for q in qarg]
        else:
            return qarg.encode('utf-8')
        
    def _update(self, message, clean_ctrl_chars=True):
        """
        Posts the given xml message to http://<host>:<port>/solr/update and
        returns the result.
        
        Passing `sanitize` as False will prevent the message from being cleaned
        of control characters (default True). This is done by default because
        these characters would cause Solr to fail to parse the XML. Only pass
        False if you're positive your data is clean.
        """
        path = '%s/update/' % self.path
        
        # Clean the message of ctrl characters.
        if clean_ctrl_chars:
            message = sanitize(message)
        
        return self._send_request('POST', path, message, {'Content-type': 'text/xml'})

    def _extract_error(self, headers, response):
        """
        Extract the actual error message from a solr response. Unfortunately,
        this means scraping the html.
        """
        reason = ER_RE.search(response)
        if reason:
            reason = reason.group()
            reason = reason.replace('<pre>','')
            reason = reason.replace('</pre>','')
            return "Error: %s" % str(reason)
        return "Error: %s" % response

    # Conversion #############################################################

    def _from_python(self, value):
        """
        Converts python values to a form suitable for insertion into the xml
        we send to solr.
        """
        if isinstance(value, datetime):
            value = value.strftime('%Y-%m-%dT%H:%M:%SZ')
        elif isinstance(value, date):
            value = value.strftime('%Y-%m-%dT00:00:00Z')
        elif isinstance(value, bool):
            if value:
                value = 'true'
            else:
                value = 'false'
        else:
            value = unicode(value)
        return value
    
    def _to_python(self, value):
        """
        Converts values from Solr to native Python values.
        """
        if isinstance(value, (int, float, long, complex)):
            return value
        
        if isinstance(value, (list, tuple)):
            value = value[0]
        
        if value == 'true':
            return True
        elif value == 'false':
            return False
        
        if isinstance(value, basestring):
            possible_datetime = DATETIME_REGEX.search(value)
        
            if possible_datetime:
                date_values = possible_datetime.groupdict()
            
                for dk, dv in date_values.items():
                    date_values[dk] = int(dv)
            
                return datetime(date_values['year'], date_values['month'], date_values['day'], date_values['hour'], date_values['minute'], date_values['second'])
        
        try:
            # This is slightly gross but it's hard to tell otherwise what the
            # string's original type might have been. Be careful who you trust.
            converted_value = eval(value)
            
            # Try to handle most built-in types.
            if isinstance(converted_value, (list, tuple, set, dict, int, float, long, complex)):
                return converted_value
        except:
            # If it fails (SyntaxError or its ilk) or we don't trust it,
            # continue on.
            pass
        
        return value

    # API Methods ############################################################

    def search(self, q, **kwargs):
        """Performs a search and returns the results.  query input can be a list
        
        Examples::
            
            conn.search('ipod')
            
            conn.search(["ipod","category_id:1"],facet="on", 
                **{'facet.field':['text','tags','cat','manufacturer'],'rows':10})
        """
        params = {'q': q}
        params.update(kwargs)
        if len(q) < 1024:
            response = self._select(params)
        else:
            response = self._select_post(params)
        
        return self.result_class(response,decoder=self.decoder)
        
    
    def more_like_this(self, q, mltfl, **kwargs):
        """
        Finds and returns results similar to the provided query.
        
        Requires Solr 1.3+.
        """
        params = {
            'q': q,
            'mlt.fl': mltfl,
        }
        params.update(kwargs)
        response = self._mlt(params)
        result = self.decoder.decode(response)
        
        if result['response'] is None:
            result['response'] = {
                'docs': [],
                'numFound': 0,
            }
            
        return self.result_class(response,decoder=self.decoder)

    def term_vectors(self,q,field=None,**kwargs):
        params = {'q': q or '','tv.all':'true' }
        if field:
            params['tv.fl'] = field
        params.update(kwargs)

        response = self._tvrh(params)
        return TermVectorResult(field,response)

    def group(self,q,**kwargs):
        params = {'q': q or '',
                  'group':'true' }
        
        params.update(kwargs)
        response = self._select(params)
        return GroupedResults(response)

#######################################################
      
        
    def add(self, docs, commit=True):
        """Adds or updates documents. For now, docs is a list of dictionaies
        where each key is the field name and each value is the value to index.
        """
        message = ET.Element('add')
        for doc in docs:
            d = ET.Element('doc')
            for key, value in doc.items():
                # handle lists, tuples, and other iterabes
                if hasattr(value, '__iter__'):
                    for v in value:
                        f = ET.Element('field', name=key)
                        f.text = self._from_python(v)
                        d.append(f)
                # handle strings and unicode
                else:
                    f = ET.Element('field', name=key)
                    f.text = self._from_python(value)
                    d.append(f)
            message.append(d)
        m = ET.tostring(message)
        response = self._update(m)
        # TODO: Supposedly, we can put a <commit /> element in the same post body
        # as the add element. That isn't working for some reason, and it would save us
        # an extra trip to the server. This works for now.
        if commit:
            self.commit()

    def delete(self, id=None, q=None, commit=True, fromPending=True, fromCommitted=True):
        """Deletes documents."""
        if id is None and q is None:
            raise ValueError('You must specify "id" or "q".')
        elif id is not None and q is not None:
            raise ValueError('You many only specify "id" OR "q", not both.')
        elif id is not None:
            m = '<delete><id>%s</id></delete>' % id
        elif q is not None:
            m = '<delete><query>%s</query></delete>' % q
        response = self._update(m)
        # TODO: Supposedly, we can put a <commit /> element in the same post body
        # as the delete element. That isn't working for some reason, and it would save us
        # an extra trip to the server. This works for now.
        if commit:
            self.commit()

    def commit(self):
        response = self._update('<commit />')

    def optimize(self,waitFlush=False,waitSearcher=False,block=False):
        """
        Optimize index and optionally wait for the call to be completed before returning with `block=True`. Default
        is `False`
        """
        params = {'waitFlush':str(waitFlush).lower(),'waitSearcher':str(waitSearcher).lower(),'optimize':str(True).lower()}
        if block:
            import urllib
            import socket
            socket.setdefaulttimeout(None) # block until we get a response
            path = '/update?%s' % (urlencode(params))
            print self.url+path
            return urllib.urlopen(self.url+path).read()
        
        path = '%s/update?%s' % (self.path,urlencode(params))
        return self._send_request('GET',path)
        
# Using two-tuples to preserve order.
REPLACEMENTS = (
    # Nuke nasty control characters.
    ('\x00', ''), # Start of heading
    ('\x01', ''), # Start of heading
    ('\x02', ''), # Start of text
    ('\x03', ''), # End of text
    ('\x04', ''), # End of transmission
    ('\x05', ''), # Enquiry
    ('\x06', ''), # Acknowledge
    ('\x07', ''), # Ring terminal bell
    ('\x08', ''), # Backspace
    ('\x0b', ''), # Vertical tab
    ('\x0c', ''), # Form feed
    ('\x0e', ''), # Shift out
    ('\x0f', ''), # Shift in
    ('\x10', ''), # Data link escape
    ('\x11', ''), # Device control 1
    ('\x12', ''), # Device control 2
    ('\x13', ''), # Device control 3
    ('\x14', ''), # Device control 4
    ('\x15', ''), # Negative acknowledge
    ('\x16', ''), # Synchronous idle
    ('\x17', ''), # End of transmission block
    ('\x18', ''), # Cancel
    ('\x19', ''), # End of medium
    ('\x1a', ''), # Substitute character
    ('\x1b', ''), # Escape
    ('\x1c', ''), # File separator
    ('\x1d', ''), # Group separator
    ('\x1e', ''), # Record separator
    ('\x1f', ''), # Unit separator
)

def sanitize(data):
    fixed_string = data
    
    for bad, good in REPLACEMENTS:
        fixed_string = fixed_string.replace(bad, good)
    
    return fixed_string


#############################################################

class SolrJson(Solr):
    """
    a Solr client that uses json to to update operations
    """
    def _update(self, message):
        """
        Posts the given xml message to http://<host>:<port>/solr/update/json and
        returns the result.
        
        """
        path = '%s/update/json' % self.path
        
        return self._send_request('POST', path, message, {'Content-type': 'application/json'})

    def add(self, docs, commit=True):
        message = json.dumps(docs)
        response = self._update(message)
        return response

    def delete(self, id=None, q=None, commit=True, fromPending=True, fromCommitted=True):
        """Deletes documents."""
        if id is None and q is None:
            raise ValueError('You must specify "id" or "q".')
        elif id is not None and q is not None:
            raise ValueError('You many only specify "id" OR "q", not both.')
        elif id is not None:
            m = json.dumps({"delete":{"id":"%s" % id }}) 
        elif q is not None:
            m = json.dumps({"delete":{"query":"%s" % q }}) 
            
        response = self._update(m)
        if commit:
            self.commit()

    def commit(self):
        response = self._update('{"commit":{}}')

    def optimize(self,waitFlush=False,waitSearcher=False,block=False):
        """
        Optimize index and optionally wait for the call to be completed before returning with `block=True`. Default
        is `False`
        """
        params = {'waitFlush':str(waitFlush).lower(),'waitSearcher':str(waitSearcher).lower(),'optimize':str(True).lower()}
        if block:
            import urllib
            import socket
            socket.setdefaulttimeout(None) # block until we get a response
            path = '/update?%s' % (urlencode(params))
            print self.url+path
            return urllib.urlopen(self.url+path).read()
        
        path = '%s/update?%s' % (self.path,urlencode(params))
        return self._send_request('GET',path)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
