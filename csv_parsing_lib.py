#!/usr/bin/env python2

"""
Classes and functions for parsing CSV's in PySpark RDDs
"""

print(__doc__)

import csv
import sys
import codecs
import cStringIO

class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, csv_file, encoding):
        self.reader = codecs.getreader(encoding)(csv_file)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV csv_file,
    which is encoded in the given encoding.
    """
    def __init__(self, csv_file, dialect=csv.excel, encoding="utf-8", **kwds):
        csv_file = UTF8Recoder(csv_file, encoding)
        self.reader = csv.reader(csv_file, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV csv_file "f",
    which is encoded in the given encoding.
    """
    def __init__(self, csv_file, dialect=csv.excel, encoding="utf-8", **kwds):
        """
        Docstring
        """
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = csv_file
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        """
        Docstring
        """
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        """
        Docstring
        """
        for row in rows:
            self.writerow(row)

def unicode_csv_reader(unicode_csv_data, delimiter=',', **kwargs):
    """
    Function which takes in a csv line as input, and returns a unicode csv row object
    """
    csv.field_size_limit(sys.maxsize)
    csv_reader = csv.reader(utf_8_encoder(unicode_csv_data), delimiter=delimiter, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8') for cell in row]

def utf_8_encoder(unicode_csv_data):
    """
    Docstring
    """
    for line in unicode_csv_data:
        yield line.encode('utf-8')

def add_header(unicode_csv_data, new_header):
    """
    Docstring
    """
    final_iterator = [",".join(new_header)]
    for row in unicode_csv_data:
        final_iterator.append(row)
    return iter(final_iterator)

def to_csv_line(row):
    """
    Given a list of strings, returns a properly csv formatted string
    """
    output = cStringIO.StringIO("")
    UnicodeWriter(output, quoting=csv.QUOTE_ALL).writerow(row)
    return output.getvalue().strip()
