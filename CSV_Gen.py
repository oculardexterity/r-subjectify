"""
CSV_Gen
==========

# Richard Hadden 2018

Highly customisable generator object to output CSV files as Python dictionaries.


Features
--------

Optionally output only selected fields.
Optionally customise with functions to transform values on the fly.
Provide your own field names (or take them from the top row).

And so forth.


Setup
-----

    stream = CSV_Stream(file_path)

stream.stream() is a generator object which returns
a dict for each row of the CSV file



Configuration
-------------

stream.field_names = [] 
    # List of length N matching number of columns in CSV file.

stream.stream(n): returns first n rows from CSV file
(no proper slicing because it takes unpredictable lengths of time to do anything;
if you want, use itertools.islice)


Provides a decorator for function to transform a field value (or multiple): 

    @stream.transform_field_value('balls')
    def balls(value):
        return value.replace('a', 'Q')

Can also be set by providing a dict of transforms:

    stream.value_transformers.add({'field': lambda x: x.replace('a', 'Q')})


Rename fields by providing a dict of mappings:

    stream.field_renames = {
                            'title' : 'balls',
                            'author': lambda x: x.replace('a', 'Q'),
                            '^ALL': CSV_Stream.snakecase,
                            }

Select specific fields to return:

    stream.selected_fields = ('title',)


Access fields by original names, snakecased_names, or renamed versions
at any point in setup (applied before stream() runs).


"""


import csv
import functools
import itertools
from collections import namedtuple


class CSV_Error(Exception):
    pass

class CSV_Gen:
    def __init__(self, file_path=None, field_names='header'):
        
        self._value_transformers = {} # Obviously can set this directly; or not?
        self._field_names = field_names
        
        if file_path:
            self.file_path = file_path
        
        self._selected_fields = [] # initalise this as blank for easy iffing..
        self._field_name_mappings = {}

        self.ignore_first_row = field_names == 'header'
 
    @property
    def value_transformers(self):
        vt = namedtuple('vt', 'add')
        def add_func(value):
            try:
                for k, v in value.items():
                    self._value_transformers[k] = v
            except Exception:
                raise ValueError('Value transformers must be in form of dict or list of tuples')
        
        return vt(add_func)

    @property
    def file_path(self):
        return self._file_path

    @file_path.setter
    def file_path(self, path):
        self._file_path = path
        self.set_csv_dialect()
        if self._field_names == 'header':
            self.get_field_names_from_csv()
            

    def get_field_names_from_csv(self):
        with open(self.file_path, 'r', newline='') as csv_file:
            csv_reader = csv.reader(csv_file)
            self._field_names = next(csv_reader)

    @property
    def field_names(self):
        if self._field_names == 'header' and self.file_path:
            self.get_field_names_from_csv()
            
        elif self._field_names == 'header' and not self.file_path:
            raise CSV_Error('No file path to a CSV file provided.')
        return self._field_names

    @field_names.setter
    def field_names(self, names):
        self._field_names = names


    @property
    def field_renames(self):
        return self._field_name_mappings

    @field_renames.setter
    def field_renames(self, fnmap):
        # Lets you throw in such things as list of tuples
        try:
            self._field_name_mappings = dict(fnmap)
        except ValueError:
            raise ValueError('Provide mapping as dict or list of tuples.')

    @property
    def selected_fields(self):
        return self._selected_fields

    @selected_fields.setter
    def selected_fields(self, fields):
        try:
            assert type(fields) in (list, tuple, set) # Check valid argument passed in
        except AssertionError:
            raise ValueError('Selected fields must be an iterable.')

        self._selected_fields = set(fields) # make set so appendable


    def __len__(self):
        return functools.reduce(lambda acc, e: acc + 1, self._stream(), 0)


    def set_csv_dialect(self):
        with open(self.file_path, 'r', newline='') as csv_file:
            csv_dialect = csv.Sniffer().sniff(csv_file.read(1024))
            csv_file.seek(0) # Reset read position after sniff 
        self.csv_dialect = csv_dialect


    def check_file_path_and_field_names_compatible(self):
        if self._field_names == 'header':
            with open(self.file_path, 'r', newline='') as csv_file:
                if not csv.Sniffer().has_header(csv_file.read(1024)) :
                    raise KeyError('Header missing from CSV file and fields not provided.')
        elif self._field_names:
            with open(self.file_path, 'r', newline='') as csv_file:
                csv_reader = csv.reader(csv_file)
                if len(self._field_names) != len(next(csv_reader)):
                    raise KeyError('Fields provided do not match the CSV file columns.')

    @staticmethod
    def snakecase(string):
        return string.lower().replace(' ', '_')

    def _to_snakecase(self, string):
        return string.lower().replace(' ', '_')


    def _change_field_name(self, field_name):
        fnmap = {**self._field_name_mappings, 
            **{self._to_snakecase(k): v for k, v in self._field_name_mappings.items()}}
        if field_name in fnmap or self._to_snakecase(field_name) in fnmap:
            field_rename = fnmap.get(field_name) or fnmap.get(self._to_snakecase(field_name))
            if callable(field_rename):
                field_name = field_rename(field_name)
            else:
                field_name = field_rename
        map_all = self._field_name_mappings.get('^ALL')
        if callable(map_all):
            field_name = map_all(field_name)
        return field_name

    def to_dict(self, values):
        all_fields = True if not self._selected_fields else False
        row_dict = {k: (self._value_transformers.get(k) 
                            or self._value_transformers.get(self._to_snakecase(k)) 
                            or (lambda x: x))(v)
                        for k, v in zip(self._field_names, values) 
                        if all_fields 
                            or k in self._selected_fields
                            or self._to_snakecase(k) in self._selected_fields}
        return row_dict


    def apply_final_config(self):
        self.check_file_path_and_field_names_compatible()
        self._selected_fields = set([
                    *self._selected_fields, 
                    *[self._change_field_name(field) for field in self._selected_fields], 
        ])
        self._value_transformers = {
                    **self._value_transformers, 
                    **{self._change_field_name(k): v for k, v in self._value_transformers.items()}
        }

        self._field_names = [self._change_field_name(k) for k in self._field_names]


    def stream(self, n=0):
        if not n:
            yield from self._stream()
        else:
            yield from itertools.islice(self._stream(), n)

    def _stream(self):
        self.apply_final_config()

        with open(self.file_path, 'r', newline='') as csv_file:
            csv_reader = csv.reader(csv_file, self.csv_dialect)
            
            if self.ignore_first_row:
                next(csv_reader)

            for row in csv_reader:
                yield self.to_dict(row)


    def transform_field_value(self, field_to_transform):
        ''' Decorator function to register functions that transform a field's value'''

        def real_decorator(function):

            if type(field_to_transform) in (list, tuple):
                try:
                    for field in field_to_transform:
                        assert type(field) == str
                        self._value_transformers[field] = function
                except AssertionError:
                    raise ValueError('Must provide a string or list of strings as fields to transform.')

            elif type(field_to_transform) == str:
                self._value_transformers[field_to_transform] = function
            else:
                raise ValueError('Must provide a string or list of strings as fields to transform.')
            
            @functools.wraps(function)
            def wrapper(*args, **kwargs):
                function(*args, **kwargs)
            
            return wrapper
        return real_decorator


if __name__ == '__main__':

    stream = CSV_Gen('bnb_childrens_literature.csv')


    @stream.transform_field_value('balls')
    def balls(value):
        return value.replace('a', 'Z')


    stream.field_renames = {'title': 'balls'}
    stream.selected_fields = ('title',)
    stream.value_transformers.add({'balls': lambda x: x.replace('a', 'Q')})


    for book in stream.stream(200):
        for k, v in book.items():
            print(k, ':', v)

