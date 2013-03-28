# Gathers statistics from Sil character dumps that are optionally downloaded /
# scraped from angband.oook.cz's main ladder (i.e. non-competition dumps.)
#
# If you are running this for the first time, you probably want to use the -s
# option to actually fetch dumps from the oook website. Each character dump will
# be saved in DUMPDIR as <some random guid>.dmp.
#
# You can use the -c option to run the scraper in parallel with CONCURRENCY
# number of threads/processes. If the Python multiprocessing library is giving
# you a hard time, use -c 1 to avoid concurrency altogether, and all dumps will
# be downloaded using the main program thread.
#
# Regardless of whether scraping or not, the script will always look from *.dmp
# in the DUMPDIR and will parse the character dumps and export the results to
# either a csv file (default) or a sqlite3 database, depending on what you
# specify for the -f option.
#
# TODO: The regexps are really sloppy and still grabbing random giant globs of
# character dumps in some cases. Tighten that up.

import os
import sys


#-------------------------------------------------------------------------------
# SCRAPING
#-------------------------------------------------------------------------------
from bs4 import BeautifulSoup
import requests
import uuid

DUMP_EXT = "dmp"
OOOK_ROOT = "http://angband.oook.cz/"
LADDER_URL = OOOK_ROOT + "ladder-browse.php"


def puts(string):
    """
    Output a string without sticking a newline at the end. Used for hack
    progress notification.
    """
    sys.stdout.write(string)
    sys.stdout.flush()


def get_dump_links_from_index(index_html):
    """
    Given the HTML of one of the ladder pages, this will scrape out all the
    direct links to actual character dumps.
    """
    soup = BeautifulSoup(index_html)
    links = soup.find_all("a")
    links = [OOOK_ROOT + link.get('href') for link in links if "ladder-show" in
             link.get('href')]
    return links


def download_dumps(dump_links, config):
    """
    Given a list of direct links to character dumps, this will grab each dump
    and save it to disk in DUMPDIR as <some random guid>.dmp.
    """
    destination_dir = config.dumpdir

    def write_dump(dump):
        filename = str(uuid.uuid4()) + "." + DUMP_EXT
        f = open(os.path.join(destination_dir, filename), "w")
        f.write(dump)
        f.close()

    def download_dump(dump_link):
        dump_page = requests.get(dump_link).text
        soup = BeautifulSoup(dump_page)
        dump = soup.find("pre").text
        write_dump(dump)

    print "Dumping {0} files to {1} using {2} threads.".format(
        len(dump_links), destination_dir, config.concurrency
    )

    if config.concurrency > 1:
        from multiprocessing.pool import ThreadPool
        pool = ThreadPool(processes=config.concurrency)
        pool.map(download_dump, dump_links)
    else:
        map(download_dump, dump_links)


def scrape_dumps(config):
    """
    Grabs all characters dumps from the Sil oook ladder and saves each one to
    DUMPDIR as an individual file.
    """
    o = 0
    dump_links = []
    puts("Fetching links to dumps from oook ladder")

    while True:
        params = {
            'v': 'Sil',
            'o': o
        }

        dumps_index_response = requests.get(LADDER_URL, params=params)
        dumps_index_html = dumps_index_response.text

        these_links = get_dump_links_from_index(dumps_index_html)
        puts(".")

        if len(these_links) == 0:
            break

        dump_links.extend(these_links)
        o += 1

    print "\nDone! ({0} pages fetched.)".format(o)
    download_dumps(dump_links, config)


#-------------------------------------------------------------------------------
# PARSING
#-------------------------------------------------------------------------------
import glob
import re

CSV, SQLITE3 = ('csv', 'sqlite3')
STATS = (
    'Melee',
    'Archery',
    'Evasion',
    'Stealth',
    'Perception',
    'Will',
    'Smithing',
    'Song',
    'Str',
    'Dex',
    'Con',
    'Gra'
)


def get_regexp_map():
    """
    Creates a map of (stat -> regexp) pairs, where 'stat' is a label we're
    using to represent some quantity in a character dump, and 'regexp' is one
    of two things:

    1) A plain old regexp that you should apply to the entire sheet text in
    order to extract the stat in question; or

    2) a 2-tuple of (regexp, cleaner_func), where cleaner_func should be
    applied to the result of running the regexp on the sheet in order to get
    the correct quantity.
    """
    wipe_commas = lambda s: s.replace(",", "")
    convert_depth = lambda s: int(s.replace("'", "")) / 50
    # In cases where I was too lazy to config greedy matching to make it work,
    # I just clipped off the shit from the end I didn't want.
    fuck_this_regexp = lambda s: s.split(".")[0]

    # Convenient notation -- I hate writing re.compile(..., re.DOTALL) a
    # billion times. Especially if I later need to change the DOTALL :>
    rexp = lambda pattern: re.compile(pattern, re.DOTALL)

    def stat_pattern(stat, base=True):
        if base:
            pattern = r'.+{0}\s+[-0-9]+ =\s+([-0-9]+)'.format(stat)
        else:
            pattern = r'.+{0}\s+([-0-9]+)'.format(stat)

        return rexp(pattern)

    def fn_prot(index):
        return lambda s: s.split("-")[index]

    regexp_map = {
        'Version': rexp(r'.+\[Sil\s+([0-9\.]+) Character'),
        'Race': rexp(r'.+Race\s+(\w+)\s'),
        'House': rexp(r'.+House\s+(\w+)\s'),
        'Turns': (rexp(r'.+Game Turn\s+([,0-9]+)\s'), wipe_commas),
        'TotalXP': (rexp(r'.+Total Exp\s+([,0-9]+)\s'), wipe_commas),
        'Depth': (rexp(r".+Depth\s+(['0-9]+)\s+Voice"), convert_depth),
        'MinDepth': (rexp(r".+Min Depth\s+(['0-9]+)\s"), convert_depth),
        'KilledBy': (rexp(r'.+Slain by (.+).'), fuck_this_regexp),
        'ProtLow': (rexp(r'.+Armor\s+\[\+\d+,(.+?)\] '), fn_prot(0)),
        'ProtHigh': (rexp(r'.+Armor\s+\[\+\d+,(.+?)\] '), fn_prot(1)),
    }

    # For all stats and skills, create regexps to get the base + actual values.
    for stat in STATS:
        regexp_map[stat] = stat_pattern(stat, base=False)
        regexp_map[stat + "Base"] = stat_pattern(stat, base=True)

    return regexp_map

# Just create this once, since we're calling parse_dump a gazillion times.
REGEXP_MAP = get_regexp_map()


def parse_dump(dump, filename):
    """
    Given the text of the dump from 'filename', this will create a map of stat
    field -> quantity for that dump. 'filename' is included in the map as
    'Filename', for correctness-checking purposes.

    Most of the stats extracted come from the regexp map above, but a few are
    specifically computed here (e.g. how many sils were found, if V was killed,
    etc.)
    """
    fields = {'Filename': os.path.basename(filename)}

    for (field, processor) in REGEXP_MAP.items():
        if isinstance(processor, tuple):
            (pattern, cleaner) = processor
        else:
            (pattern, cleaner) = (processor, lambda x: x)

        m = pattern.match(dump)
        if m:
            val = cleaner(m.group(1))
        else:
            val = ""

        fields[field] = val

    fields["KilledV"] = "Slew Morgoth, Lord of Darkness" in dump
    fields["Won"] = "You escaped the Iron Hells" in dump

    if "You brought back all three Silmarils from" in dump:
        sil_count = 3
    elif "You brought back two Silmarils from" in dump:
        sil_count = 2
    elif "You brought back a Silmaril from" in dump:
        sil_count = 1
    else:
        sil_count = 0

    fields["SilCount"] = sil_count

    for stat in STATS:
        base_stat_key = stat + "Base"
        if not fields[base_stat_key]:
            fields[base_stat_key] = fields[stat]

    fuck_count = len(re.findall(r'(?i)fuck', dump))
    fields["FuckCount"] = fuck_count

    return fields


def parse_dumps(config):
    """
    Parses all dumps in config.dumpdir and returns a list of maps, where each
    map represents a parsed dump.
    """
    dump_filenames = glob.glob(os.path.join(config.dumpdir, "*." + DUMP_EXT))
    parsed_dumps = []

    for fn in dump_filenames:
        f = open(fn, "r")
        dump_text = f.read()
        fields = parse_dump(dump_text, fn)
        parsed_dumps.append(fields)

    return parsed_dumps


#-------------------------------------------------------------------------------
# EXPORT
#-------------------------------------------------------------------------------
class Exporter(object):
    """
    An abstract class that calls out to template methods to orchestrate an
    specific type of export.
    """

    def export(self, records, config):
        """
        Given a list of maps that represent parsed dumps, export the result to
        config.outputfile.
        """
        if not records:
            print "No records to export! Did you scrape (-s) to {0}?".format(
                config.dumpdir)
            return

        sample = records[0]
        header = sorted(sample.iterkeys())

        self._start_export(header, config)

        for record in records:
            sorted_rec = [record[field] for field in header]
            self._export_one(sorted_rec)

        self._end_export()


class CSVExporter(Exporter):
    """
    Orchestrates CSV export.
    """

    def _start_export(self, header, config):
        import csv
        self.filehandle = open(config.output_file, 'w')
        self.writer = csv.writer(self.filehandle)

    def _export_one(self, record):
        self.writer.writerow(record)

    def _end_export(self):
        self.filehandle.close()


class SqliteExporter(Exporter):
    """
    Orchestrates Sqlite export.
    """

    def _start_export(self, header, config):
        import sqlite3
        conn = sqlite3.connect(config.output_file)
        c = conn.cursor()

        create_stmt = "CREATE TABLE silchars ({0})".format(",".join(header))
        c.execute(create_stmt)

        conn.commit()

        self.conn = conn
        self.c = c

    def _export_one(self, record):
        insert_stmt = "INSERT INTO silchars VALUES ({0})".format(
            ",".join("?" * len(record)))
        self.c.execute(insert_stmt, record)

    def _end_export(self):
        self.conn.commit()
        self.conn.close()


#-------------------------------------------------------------------------------
# MAIN PROGRAM
#-------------------------------------------------------------------------------
import optparse

DESC = """Gathers statistics from Sil character dumps that are optionally downloaded /
scraped from angband.oook.cz's main ladder (i.e. non-competition dumps.)"""

USAGE = "%prog [options] dump_directory output_filename"


def parse_args():
    p = optparse.OptionParser(usage=USAGE, description=DESC)

    sg = optparse.OptionGroup(p, 'Scraping options')
    sg.add_option("-s", None, dest="scrape", default=False,
            action="store_true", help="Scrape oook for all Sil character dumps")
    sg.add_option("-c", None, dest="concurrency", type='int', default=1,
            action="store", help="How many threads to use for scraping?")
    p.add_option_group(sg)

    pg = optparse.OptionGroup(p, 'Parsing options')
    pg.add_option("-f", None, dest="fformat", default="csv",
            action="store", help="Format of output (csv or sqlite3)")
    p.add_option_group(pg)

    (opts, args) = p.parse_args()
    opts.fformat = opts.fformat.lower()

    if opts.concurrency < 1:
        print "Concurrency must be a positive integer."
        sys.exit()

    if opts.concurrency > 1 and not opts.scrape:
        print "Concurrency option only used when scraping; use -s to scrape."
        sys.exit()

    if opts.fformat not in (CSV, SQLITE3):
        print "Please specify 'csv' or 'sqlite3' as file format."
        sys.exit()

    if len(args) != 2:
        p.error("requires exactly 2 args.")

    (dumpdir, output_file) = args

    if not os.path.isdir(dumpdir):
        p.error("Directory '{0}' does not exist.".format(dumpdir))

    opts.dumpdir = dumpdir
    opts.output_file = output_file

    return opts


def main():
    config = parse_args()
    if config.scrape:
        scrape_dumps(config)
    parsed_dumps = parse_dumps(config)

    if config.fformat == CSV:
        exporter = CSVExporter()
    else:
        exporter = SqliteExporter()

    exporter.export(parsed_dumps, config)
    print "Exported stats from {0} dumps to {1}".format(len(parsed_dumps),
            config.output_file)

if __name__ == "__main__":
    main()
