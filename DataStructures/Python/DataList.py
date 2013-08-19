__all__ = ['DataList']
"""
    Original Code = Kenneth Wade
    Modified By: Matt Livingston
"""
import collections
import copy
import json

try:
    # NameError will be raised later if someone tries to use DataList's xml
    # creation feature if they don't have lxml installed
    from lxml import etree
except ImportError:
    pass

from sys import stdout


_nonprint = '\x00\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c\x0e'\
            '\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a'\
            '\x1b\x1c\x1d\x1e\x1f\x7f\x80\x81\x82\x83\x84\x85'\
            '\x86\x87\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f\x90\x91'\
            '\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c\x9d'\
            '\x9e\x9f\xa0\xa1\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9'\
            '\xaa\xab\xac\xad\xae\xaf\xb0\xb1\xb2\xb3\xb4\xb5'\
            '\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf\xc0\xc1'\
            '\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xcb\xcc\xcd'\
            '\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9'\
            '\xda\xdb\xdc\xdd\xde\xdf\xe0\xe1\xe2\xe3\xe4\xe5'\
            '\xe6\xe7\xe8\xe9\xea\xeb\xec\xed\xee\xef\xf0\xf1'\
            '\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc\xfd'\
            '\xfe\xff'


def strip_nonprint(s):
    "Remove all non-printable characters in string s."
    try:
        return s.translate(None, _nonprint)
    except TypeError:       # unicode translate act's weird
        s =  s.encode('ascii', 'ignore')
        s = s.translate(None, _nonprint)
        return unicode(s)


def isiterable(obj):
    try:
        return callable(obj.__iter__)
    except AttributeError:
        return False


class CustomError(Exception):
    def __init__(self, value=None):
        self.value = value
    def __str__(self):
        return repr(self.value)


class BreakLoop(CustomError):
    pass


class Data(dict):
    def __init__(self, data=None):
        """
            - data: a dictionary-like object with key-value pairs
        """
        super(Data, self).__init__()
        if data is not None:
            try:
                callable(data.keys)
                self.update(data)
            except AttributeError:          # data is not a dict type
                try:
                    callable(data.__iter__)
                    for item in data:
                        self[item] = None
                except AttributeError:      # data is not a sequence
                    try:
                        hash(data)
                        self[data] = None
                    except TypeError:       # data unhashable
                        self[repr(data)] = None


#   ############################################################
#   # Class methods 'inverted' and 'merged' aren't necessarily
#   # useful for anything right now, so I'll comment them out.
#   def inverted(self):
#       "Return a dict where the keys and values switch places."
#       D = collections.defaultdict(list)
#       for key in self.iterkeys():
#           newkey = self[key]
#           try:
#               hash(newkey)
#           except TypeError, e:
#               if 'unhashable' in repr(e):
#                   newkey = repr(newkey)     #h or tuple(newkey)
#
#           D[newkey].append(key)
#
#       d = {}
#       for key, val in D.iteritems():
#           if len(val) == 1:
#               d[key] = val[0]
#           else:
#               d[key] = tuple(val)
#
#       return d
#
#
#   def merged(self, *objs):
#       """
#       Return a dict where the all objs are merged with self.  The keys will be
#       the same, but the values will be a tuple of the UNIQUE values at all
#       objs (including self).
#       """
#       D = collections.defaultdict(set)
#       for d in [self] + list(objs):
#           for key, val in d.iteritems():
#               D[key].add(copy.deepcopy(val))
#
#       d = {}
#       for key, val in D.iteritems():
#           if len(val) == 1:
#               D[key] = list(val)[0]
#           else:
#               D[key] = tuple(val)
#
#       return d


class DataList(list):
    """
    The internal data of the DataList must be subscriptable, meaning it has
    the __getitem__ function defined.
        - ex: lists and dictionaries have __getitem__ defined 
        - ex: int does NOT have __getitem__ defined 

    For convenience, internal data of DataList is converted to type Data
    automatically upon insertion.
    """
    def __init__(self, iterable_obj=None):
        super(DataList, self).__init__()
        if iterable_obj:
            self.extend(iterable_obj)


    def insert(self, index, obj):
        super(DataList, self).insert(index, Data(obj))


    def append(self, obj):
        super(DataList, self).append(Data(obj))


    def extend(self, iterable_obj):
        iterable_obj = map(Data, iterable_obj)
        super(DataList, self).extend(iterable_obj)


    def sort_by(self, key, reverse=False):
        "Sort internal data by key (or index)"
        if reverse:
            super(DataList, self).sort(lambda x,y: cmp(y.get(key), x.get(key)))
        else:
            super(DataList, self).sort(lambda x,y: cmp(x.get(key), y.get(key)))


    def filter_by(self, key, mfunc=(lambda x: x)):
        """
        Return a DataList where items at key/index are true when mfunc
        is applied.
        """
        seq = []
        for item in self:
            try:
                if mfunc(item[key]): seq.append(item)
            except (KeyError, IndexError): pass

        return DataList(seq)


    def mapreduce(self, key, rfunc=(lambda x,y: x+y), mfunc=(lambda x: x),
            *mfunc_args ):
        """
        rfunc is a funciton of two arguments, cumulatively applied to the
        items of the sequence, after mfunc is applied to every item at key/index

        If mfunc is a funciton of more than one argument, its extra args must
        be passed.

        Default behavior accumulates items at key/index.

            - W A R N I N G: it is a SyntaxError for a non-keyword arg to be 
                             passed after a keyword arg, so when calling this
                             method, be sure you don't pass the arguments as
                             keyword args if you need to pass extra arguments
                             to mfunc

                ok:  myinstance.mapreduce(
                        key='a',
                        mfunc=(lambda x: x-5))
                     # works fine.. not passing any extra args to mfunc
                
                ok:  rfunc = lambda x,y: x-y
                     mfunc = lambda x,y,z: x*y-z
                     myinstance.mapreduce('a', rfunc, mfunc, 2, 3)
                     # works fine.. y in mfunc is 2, z is 3

                BAD: myinstance.mapreduce(
                        key='a',
                        mfunc=(lambda x,y: x-y),
                        4)
                     # DO NOT pass as keyword args if you need to pass
                     # other arguments to the mfunc
        """
        seq = []
        for item in self:
            try:
                seq.append(mfunc(item[key], *mfunc_args))
            except (KeyError, IndexError): pass

        #print 'seq', seq
        if seq: return reduce( rfunc, seq )


    #h make a version that accepts a map function that will return keys
    #h also make a version that you can specify which keys you want as values
    def make_dict(self, *keys):
        """
        Return a dictionary object where the keys of the dictionary are the 
        values of the internal data at the specified key(s); these 'keys' 
        are indicies in the cases where the internal data are list objects.

        The internal data of the DataList must be subscriptable, meaning it has
        the __getitem__ function defined.
            - ex: lists and dictionaries have __getitem__ defined 
            - ex: int does NOT have __getitem__ defined 
        """
        D = {}
        key_filters = []
        for k in keys:
            try:
                hash(k)
                key_filters.append((k, lambda x: x))
            except TypeError, e:
                if 'unhashable' in repr(e):
                    key_filters.append((k, lambda x: repr(x)))

        for item in self:
            keylist = [] 
            try:
                for k, kfilter in key_filters:
                    try:
                        keylist.append(kfilter(item[k]))
                    except (KeyError, IndexError):
                        raise BreakLoop
                        # also, if internal data is a list object and the key is
                        # not an integer, a TypeError will be raised automatically

                key = keylist[0] if len(keylist) == 1 else tuple(keylist)
                try:
                    hash(key)
                except TypeError, e:
                    if 'unhashable' in repr(e):
                        key = repr(key)

                try:
                    D[key].append(item)
                except KeyError:
                    D[key] = DataList([item])
            except BreakLoop:
                continue

        return D


    def dump_json(self, stream=stdout):
        stream.write(json.dumps(self, skipkeys=True, ensure_ascii=False).encode('utf-8'))


    def dump_xml2(self, stream=stdout, root_title='corpus', entity_title='doc',
            duplicate_field_names=False, pretty_print=False):
        """
            - stream: an open file-like object
            - root_title: name of the root element of the xml file
            - entity_title: the name of each entity element enclosing a chunk
              of data (such as a tweet)
            - duplicate_field_names: True/False
                * if True, any data for a field_name that is an array will
                  have the same field_name repeated for each item
                    - ex: <field name="tag">sometag1</field>
                          <field name="tag">sometag2</field>
                * if False, any data for a field_name that is an array will
                  have an incremented field_name under an an array element for
                  each item
                    - ex: <array name="tag">
                            <tag-0>sometag1</tag-0>
                            <tag-1>sometag2</tag-1>
                          </array>
            - pretty_print: True/False.. whether or not to output the xml
              with spaces and indenting
        
        If calling dump_xml to generate a file to be posted to a solr instance
        via post.jar, use the following parameters:
            root_title='add', entity_title='doc', duplicate_field_names=True
        """

        # Generate the make_leaf function, which when given a parent element and
        # a field_name, will return an etree.SubElement instance under the
        # parent with either the field title set to field_name, or a field
        # attribute set to field_name.
        make_leaf = None
        if duplicate_field_names:
            make_leaf = lambda parent, field_name: \
                    etree.SubElement(parent, 'field', name=field_name)
        else:
            make_leaf = lambda parent, field_name: \
                    etree.SubElement(parent, field_name)

        # Declare the set_field_values function, which when given an entity
        # element, field_name, and list of values, will call the make_leaf
        # function generated above to create a leaf element under the entity
        # element and set the values according to setting of the
        # duplicate_field_names flag.
        def set_field_values(entity, field_name, values):
            """
            entity: the element that will be the parent of the generated leaf
            """
            if len(values) == 0:
                raise TypeError, "VALUES IS EMPTY!!"
            if duplicate_field_names is True or len(values) <= 1:  # never len=0
                for value in values:
                    field = make_leaf(entity, field_name)
                    try:
                        assign_value(field, value)
                    except AttributeError:
                        print 'field_name:', field_name
                        print 'values:', values
                        raise

            else:
                arr_element = etree.SubElement(entity, field_name)
                for i, value in enumerate(values):
                    field = make_leaf(arr_element, "item-%d"%i)
                    assign_value(field, value)


        # Declare the assign_value function, which when given an element and a
        # value, set element.text to that value
        def assign_value(element, value):
            try:
                element.text = strip_nonprint(value)
            except TypeError:       # value not bytes or unicode
                element.text = strip_nonprint(repr(value))
            except AttributeError:
                print '-'*80 + '\n\n'
                print 'value:', value
                print '-'*80 + '\n\n'
                raise

        #-----------------------------------------------------------
        # Create root element and the xml document rooted at that root
        root = etree.Element(root_title)
        xml = etree.ElementTree(root)

        for data in self:
            # Create entity element under root
            entity = etree.SubElement(root, entity_title)

            for field_name, value in sorted(data.items()):
                value_array = [value] if not isiterable(value) else value
                value_array = [v for v in value_array if v]

                if value_array:
                    set_field_values(entity, field_name, value_array)

        xml.write(file=stream, encoding='utf-8', pretty_print=pretty_print)
        

    def dump_xml(self, stream=stdout, root_name='corpus', chunk_name='doc',
            arr_name='arr', pretty_print=False):
        """
            - stream: an open file-like object.
            - root_name: the name of the root element of the xml file
            - chunk_name: the name of the element enclosing each chunk of data
            - arr_name: the name of array elements, if needed
        """
        # create root element
        root = etree.Element(root_name)

        # create the xml doc rooted at root element
        xml = etree.ElementTree(root)

        for data in self:
            # create chunk element under root
            chunk = etree.SubElement(root, chunk_name)

            # create field elements for each chunk
            for key in sorted(data.keys()):
                if isiterable(data[key]):
                    # make "array"
                    field = etree.SubElement(chunk, arr_name, name=key)

                    for i, value in enumerate(data[key]):
                        item = etree.SubElement(field, "%s-%d" % (key, i))
                        try:
                            item.text = strip_nonprint(value)
                            #item.text = value
                        except AttributeError:          # non-string
                            item.text = strip_nonprint(repr(value))
                        except ValueError:              # invalid character
                            item.text = strip_nonprint(value)
                        except TypeError:               # non-string type
                            item.text = strip_nonprint(repr(value))
                        
                else:
                    # make a non-array field
                    field = etree.SubElement(chunk, key)
                    try:
                        field.text = strip_nonprint(data[key])
                        #field.text = data[key]
                    except AttributeError:          # non-string
                        field.text = strip_nonprint(repr(data[key]))
                    except ValueError:              # invalid character
                        field.text = strip_nonprint(data[key])
                    except TypeError:               # non-string type
                        field.text = strip_nonprint(repr(data[key]))

        xml.write(file=stream, encoding='utf-8', pretty_print=pretty_print)


def example():
    from pprint import pprint

    dlist = DataList([{'a':35, 'b':9}, {'a':27, 'b':12, 'c':10}])
    dlist.append('f')
    dlist.extend([{'a':1, 'b':9, 'c':10}, {'c':5, 'd':9}, {'b':2, 'c':49}])
    dlist.insert(2, {'c': [1,2,3]} )
    print '-'*80 + '\n' 'dlist'
    pprint(dlist)

    print '-'*80 + '\n' 'dlist sorted by descending "b" value'
    dlist.sort_by('b', reverse=True)
    pprint(dlist)

    print '-'*80 + '\n' 'dlist where "c" value less than 15'
    pprint(dlist.filter_by('c', mfunc=(lambda x: x < 15)))

    print '-'*80 + '\n' 'dict where keys are tuple of the values of "a" & "b"'
    pprint(dlist.make_dict('a', 'b'))
    
    print '-'*80 + '\n' 'dict where keys are value "c"'
    pprint(dlist.make_dict('c'))

    print '-'*80 + '\n' 'mapreduce example, looking at values of "a"'
    alist = [x['a'] for x in dlist.filter_by('a')]
    pprint(alist)
    pprint([x*2 for x in alist])
    pprint([x*2-3 for x in alist])
    pprint(dlist.mapreduce('a', lambda x,y: x-y, lambda x,y,z: x*y-z, 2, 3))

    print '-'*80 + '\n' 'dlist as xml'
    dlist.xml_to_stream(pretty_print=True)

    print '-'*80 + '\n' 'dlist as json'
    dlist.json_to_stream()

    return dlist

if __name__ == '__main__':
    #example()
    pass
