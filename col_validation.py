import csv
import codecs
import StringIO
import cStringIO
import sys
import time
import argparse

from pyspark import SparkContext

parser = argparse.ArgumentParser(description='Count columns and lines existing in file')
parser.add_argument('-df','--DATAFILE', dest="DATAFILE", type=str,
                   help='the path for the file to be analyzed (a csv one)')
parser.add_argument('-orep','--SAVE_TO_REP', dest="SAVE_TO_REP", type=str,
                   help='the path for the repartitioned file to be analyzed')


class UnicodeWriter:
        """
        A csv writer which will write rows to CSV file "f",
        which is encoded in the given encoding
        """
        def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
                # Redirect output to a queue
                self.queue = cStringIO.StringIO()
                self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
                self.stream = f
                self.encoder = codecs.getincrementalencoder(encoding)()
        def writerow(self, row):
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
                for row in rows:
                        self.writerow(row)


def unicode_csv_reader(unicode_csv_data, **kwargs):
        csv.field_size_limit(sys.maxsize)
        csv_reader = csv.reader(utf_8_encoder(unicode_csv_data), delimiter='|', **kwargs)
        for row in csv_reader:
                yield [unicode(cell, 'utf-8') for cell in row]


def utf_8_encoder(unicode_csv_data):
        for line in unicode_csv_data:
                yield line.encode('utf-8')


def toCSVLine(row):
        # Given a list of strings, returns a properly csv formatted string
        output = StringIO.StringIO("")
        UnicodeWriter(output,quoting=csv.QUOTE_ALL).writerow(row)
        return output.getvalue().strip()


def get_fields_index(header):
        # Identify field positions from header and populate dictionary.
        return dict(zip(header, range(len(header))))


def add_header(unicode_csv_data, new_header):
        final_iterator = [",".join(new_header)]
        for row in unicode_csv_data:
                final_iterator.append(row)
        return iter(final_iterator)


args = parser.parse_args()
params = vars(args)

sc = SparkContext()

file_path_full=params['DATAFILE']

kwargs = {'escapechar': '\\', 'doublequote':False}
rdd =sc.textFile(file_path_full).map(lambda x: x.replace("\x00","")).mapPartitions(lambda x: unicode_csv_reader(x))

sample = rdd.take(2)
header = sample[0]
line = sample[1]

print(header)
print(line)

# Get field positions from header.
fields_index = get_fields_index(data_header)

a = rdd.map(lambda x: (len(x),1)).reduceByKey(lambda a,b: a+b).collect()

if params['SAVE_TO_REP']:
    rdd.map(toCSVLine).mapPartitions(lambda x: add_header(x,data_header)).saveAsTextFile(params['SAVE_TO_REP']);
