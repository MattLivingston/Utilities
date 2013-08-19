__all__= ['MySQL', 'Hbase', 'Solr']
"""    
    Matt Livingston
    Several Data Access Classes 
"""
import httplib, urllib
import urllib2
import MySQLdb
import happybase
import DataList

class MySQL:

        def __init__(self, host, user, passwd, db):
                """
                    host='localhost',user='root',passwd='Cision2013',db='BigData'
                """
                
                self.host = host
                self.user = user
                self.passwd = passwd
                self.db = db


        def open(self):
                try:
                        self.db = MySQLdb.connect(host=self.host,user=self.user,passwd=self.passwd,db=self.db)
                        self.cursor = self.db.cursor()
                except Exception, e:
                        print str(e)

        def insert(self, query):
                try:
                        self.open()
                        self.cursor.execute(query)
                        self.LastInsertedId = self.cursor.lastrowid
                        self.db.commit()
                        self.close()
                        return self.LastInsertedId
                except Exception, e:
                        print str(e)

        def insert_with_params(self, query, parameters):
                try:
                        self.open()
                        self.cursor.execute(query, parameters)
                        self.db.commit()
                        self.close()
                except Exception, e:
                        print str(e)

        def close(self):
                self.db.close()
                

class Hbase:
    
    def __init__(self, connString):
        self.connection = happybase.Connection(connString)
    
    def get_row_json(self, key):
        pass
    
    def get_rows_json(self):
        """
            Return a Json String of ros
        """
        pass
    def get_rows_dict(self, table, filter=""):
        """
            Return a dictionary of rows
            filter="ValueFilter( =, 'binaryprefix:LexisNexisDNF' )"
        """
        row_dict = {}
        tbl = self.connection.table(table)
        
        try:
            for key, data in tbl.scan(filter=filter):
                row_dict[key]= data
        except: 
            print 'Table was empty' 
            pass
                
    
    
class Solr:
    def __init__(self, server):
        """
            * server: location of the solr instance (e.g. http:mapr1-dev.qwestcolo.local:8080/solr/
        """
        assert server[-6:] == '/solr/', "Server must end in solr"
        self._server = server
        self._url_var = 'select/?q=%(query)s&start=0&rows=%(rows)d&wt=json'
        
    def _construct_url(self, core=''):
        """
            * core: name of core to perform queries on
        """
        if not core:
            return self._server + self._url_var
        else:
            return self._server + core + '/' + self._url_var
        
    def or_query_wrapper(self, core='', or_field_list=[], return_fields = None,
            sort_field=None, sort_direction='asc', rows=-1, count_only=False,
            verbose=False):
        """
        Due to the fact that the or_field_list needs to be limited to 100
        items, this wraper can receive an arbitrarily long or_field_list and
        call the query method on 100 item chunks of the list and merge the
        results together.
        
        Thanks to DataList.DataList, we can still return the aggregated results
        in a sorted order (if a sort_field is specified).

        Typically, if you have so many or_field_list items that you need to
        call this wrapper, you should probably have rows=-1 to receive all rows.
        """
        count = 0
        datalist = DataList.DataList()
        
        for i in xrange(0, len(or_field_list), 100):
            results = self.query(
                    core = core,
                    or_field_list=or_field_list[i:i+100],
                    return_fields=return_fields,
                    sort_field=sort_field,
                    sort_direction=sort_direction,
                    rows=rows,
                    count_only=count_only,
                    verbose=verbose)
        
        if count_only is True:
            count += results
        else:
            datalist.extend(results)
            
        if count_only is True:
            return count
        
        if sort_field:
            reverse = False
            if sort_direction == 'desc':
                reverse = True
                
            datalist.sort_by(key=sort_field, reverse=reverse)
                    
        if rows == -1:
            return datalist
        else:
            return datalist[:rows]
        
        
    def query(self, core='', field_dict={}, or_field_list=[],
            return_fields=None, sort_field=None, sort_direction='asc',
            rows=-1, count_only=False, verbose=False):
        """
        Return a DataList.DataList instance (awesome version of a list) or 
        just the number of results
            - core: solr core to query
            - field_dict: dictionary where keys are fields to query and the
              values are query arguments for field
            - or_field_list: list of 2-item tuples, where first tuple item is
              the name of the field and the second item is the value as a string
            - return_fields: list of solr fields to return
            - sort_field: field to sort on
            - sort_direction: 'asc' or 'desc'
            - rows: if -1, return all, otherwise, return at most 'rows' number
              of results
            - count_only: True/False... if True, only return the number of
              documents found
            - verbose: True/False... whether or not to display the constructed
              url and number of items found (regardless number of rows returned)
        """
        assert type(core) is types.StringType, \
            "core must be a string"
        assert type(field_dict) is types.DictType, \
            "field_dict must be a dict"
        assert type(or_field_list) is types.ListType \
            and len(or_field_list) <= 100, \
            "or_field_list must be a list with 100 or fewer 2-item tuples"
        assert not field_dict or not or_field_list, \
            "cannot specify both field_dict and or_field_list"
        assert type(return_fields) in [types.ListType, types.NoneType], \
            "return_fields must be a list or None"
        assert type(sort_field) in [types.StringType, types.NoneType], \
            "sort_field must be a string or None"
        assert sort_direction in ['asc', 'desc'], \
            "sort_direction must be 'asc' or 'desc'"
        assert type(rows) is types.IntType and (rows >= 0 or rows == -1), \
            "rows must be int >= 0 or == -1"
        assert type(count_only) is types.BooleanType, \
            "count_only must be a boolean (True/False)"
        assert type(verbose) is types.BooleanType, \
            "verbose must be a boolean (True/False)"

        url_extras = ''
        if return_fields:
            url_extras = url_extras + '&fl=%s' % ','.join(return_fields)

        if sort_field:
            url_extras = url_extras + '&sort=%s %s' % (sort_field, sort_direction)

        url_extras = url_extras.replace(' ', '%20')

        if field_dict:
            q_string = ' AND '.join(["%s:%s"%f for f in field_dict.items()])
        elif or_field_list:
            q_string = ' OR '.join(["%s:%s"%o for o in or_field_list])

        q_string = urllib2.quote(q_string)
        if not q_string:
            q_string = '*%3A*'      # query all "*:*"

        url_var = self._construct_url(core)
        url = None

        if count_only:
            rows = 0

        if rows == -1:
            url_tmp = url_var % {'query': q_string, 'rows': 0}
            rows = _fetch_n_decode(url_tmp)['response']['numFound']
            url = url_var % {'query': q_string, 'rows': rows} + url_extras
        else:
            url = url_var % {'query': q_string, 'rows': rows} + url_extras

        decoded = _fetch_n_decode(url)
        if verbose:
            print '\n', url
            print 'Found:', decoded['response']['numFound']

        if count_only:
            return decoded['response']['numFound']
        else:
            return DataList.DataList(decoded['response']['docs'])

    
    
    