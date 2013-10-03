import httplib, urllib
import urllib2
import MySQLdb
import happybase
import DataList
import json

class Utilities():
    def __init__(self):
        self._json_decode = json.JSONDecoder().decode
        
    def _fetch_n_decode(self,url):
        "Return decoded json data from url."
        try:
            return self._json_decode(urllib2.urlopen(url).read())
        except (urllib2.HTTPError, ValueError):
            # HTTP Error 404: Not Found | No JSON object could be decoded 
            return []
        
    def _decode_gzipped(self,filename):
        "Return decoded json data from gzipped file."
        f = gzip.open(filename, 'rb')
        try:
            json_data = f.read()
        except IOError, e:
            if 'Not a gzipped file' in repr(e):
                # In the only instance of this error occuring, the file was json
                # data, but it was not gzipped.
                f.close()
                f = open(filename, 'r')
                json_data = f.read()
    
        try:
            data = self._json_decode(json_data)
        except ValueError, e:
            if 'No JSON object could be decoded' in repr(e):
                print "No JSON object could be decoded in '%s'." % filename
                data = []
        except OSError, e:
            if 'Resource temporarily unavailable' in repr(e):
                data = self._json_decode(json_data)
            else:
                raise
    
        f.close()
        return data