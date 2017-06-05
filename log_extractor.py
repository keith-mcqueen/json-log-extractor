import argparse
import json
import os

import pypred as pypred


def _byteify(data, ignore_dicts=False):
    # if the data is a unicode string, return its string representation
    if isinstance(data, unicode):
        return data.encode('utf-8')

    # if the data is a list of values, return list of byteified values
    if isinstance(data, list):
        return [_byteify(item, ignore_dicts=True) for item in data]

    # if the data is a dict, return dict of byteified keys and values (but only if we haven't already byteified it)
    if isinstance(data, dict) and not ignore_dicts:
        return {
            _byteify(key, ignore_dicts=True): _byteify(value, ignore_dicts=True) for key, value in data.iteritems()
        }

    # if the data is anything else, just return it as-is
    return data


def json_load_byteified(file_handle):
    return _byteify(
        json.load(file_handle, object_hook=_byteify),
        ignore_dicts=True
    )


def json_loads_byteified(text):
    return _byteify(
        json.loads(text, object_hook=_byteify),
        ignore_dicts=True)


class LogExtractor:
    def __init__(self):
        self.log_file_path = None
        self.json_file_path = None
        self.lines = None
        self.predicate = None
        # self.log_objects = []
        self.log_objects = set()
        self.requested_fields = []

        self.parse_args()

    def parse_args(self):
        # create the argument parser
        parser = argparse.ArgumentParser(description='This program will read a log file', add_help=True)

        # add an argument for the input file
        parser.add_argument('-i', '--input',
                            help='path to the log file (should be in JSON format, with one line per object)',
                            required=True,
                            action='store')

        # add an argument for the number of lines to read in from the file
        parser.add_argument('-l', '--lines',
                            help='number of lines to read from the file (optional)',
                            required=False,
                            action='store',
                            type=int)

        # add an argument for the output file
        parser.add_argument('-o', '--output',
                            help='path to the output file',
                            required=False,
                            action='store')

        # add an argument for a filter condition
        parser.add_argument('-c', '--condition',
                            help='Boolean expression to filter out unwanted data (only matches will be exported)',
                            required=False,
                            action='store')

        parser.add_argument('-f', '--fields',
                            help='comma-separated list of fields to be exported',
                            required=False,
                            action='store')

        # parse the args
        args = parser.parse_args()

        # get the input file path
        self.log_file_path = args.input
        if not os.path.exists(self.log_file_path):
            raise Exception('File not found: {}'.format(self.log_file_path))
        if os.path.isdir(self.log_file_path):
            raise Exception('Input file must be an actual file, not a directory: {}'.format(self.log_file_path))

        # get the number of lines to read from the file
        self.lines = args.lines

        # get the path to the output file
        self.json_file_path = args.output

        # get the predicate if there is one
        if args.condition is not None:
            self.predicate = pypred.Predicate(args.condition)

        # get the list of fields to export
        if args.fields is not None:
            self.requested_fields = args.fields.split(',')

    def extract(self):
        # print 'Reading {} lines from {}'.format('ALL' if self.lines is None else self.lines, self.log_file_path)

        with open(self.log_file_path) as f:
            for line in f:
                log_obj = json_loads_byteified(line)

                if self.predicate is None or self.predicate.evaluate(log_obj):
                    self.add_row(log_obj)

                if self.lines is None:
                    continue

                if self.lines > 0:
                    self.lines -= 1
                else:
                    break

    def add_row(self, row):
        if len(self.requested_fields) == 0:
            record = row
        else:
            # record = {k: row.get(k) for k in self.requested_fields}
            record = {k: self.resolve_value(row, k) for k in self.requested_fields}

        print record

        if self.json_file_path is not None:
            # self.log_objects.append(record)
            # self.log_objects.add(record)
            self.log_objects.add(json.dumps(record))

    def resolve_value(self, obj, key):
        # Treat anything that is quoted as a string literal
        if key[0] == key[-1] and key[0] in ("'", "\""):
            return key[1:-1]

        # Check for the identifier in the document
        if key in obj:
            return obj[key]

        # Allow the dot syntax for nested object lookup
        # i.e. req.sdk.version = req["sdk"]["version"]
        if "." in key:
            parts = key.split(".")
            found = True
            root = obj
            for p in parts:
                if p in root:
                    # root = root[p]
                    root = root.get(p)
                else:
                    found = False
                    break
            if found:
                return root

    def export(self):
        if self.json_file_path is not None:
            with open(self.json_file_path, 'w') as output:
                for obj in sorted(self.log_objects):
                    # output.write(json.dumps(obj) + '\n')
                    output.write(obj + '\n')


if __name__ == '__main__':
    extractor = LogExtractor()
    extractor.extract()
    extractor.export()
