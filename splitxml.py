"""
split an input xml stream from a MediaWiki dump into small output files,
each containing the specified number of pages as well as the standard
xml header and footer.
"""
import sys
import getopt
import gzip
import bz2


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''

    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """
Usage: splitxml.py --pages <pagecount> --ofile <prefix>
                  [-ifile <name>] [-compression <type>]| --help

Options:
  --ofile       (-o):  output filename prefix; output files will be named
                       <prefix>_<number>.xml with steadily increasing numbers;
                       file naumber will be zero padded to five spaces
  --pages       (-p):  number of pages to write per file

  --compression (-c):  use the specified compression type for the output
                       files (gzip or bzip2); in this case the output
                       filenames will have the corresponding suffix
                       '.gz' or .bz2' appended to them
  --ifile       (-i):  optional input filename; if not specified, content
                       will be read from stdin, if filename ends in .gz2
                       or .bz2 it will be read with decompression

  --help        (-h):  display this help message
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def get_opts():
    """
    read and parse command line options, returning
    the values for 'pages', 'ifile', 'ofile', 'compression' options
    """
    pages = None
    ifile = None
    ofile = None
    compression = None

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "c:i:o:p:h", ["compression=", "ifile=", "ofile=", "pages=", "help"])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-c", "--compression"]:
            compression = val
        elif opt in ["-o", "--ifile"]:
            ifile = val
        elif opt in ["-o", "--ofile"]:
            ofile = val
        elif opt in ["-p", "--pages"]:
            if not val.isdigit():
                usage("argument to pages option must be a number")
            pages = int(val)
        elif opt in ["-h", "--help"]:
            usage("Help for this script")

    if pages is None:
        usage("Mandatory argument 'pages' not specified")
    elif ofile is None:
        usage("Mandatory argument 'ofile' not specified")
    elif len(remainder) > 0:
        usage("Unknown option(s) specified: <%s>" % remainder[0])
    if compression is not None and compression not in ["gzip", "bzip2"]:
        usage("Uknown compression type")
    return pages, ifile, ofile, compression


class XmlWrapper(object):
    """
    manage MediaWiki xml header and footer for a file
    """
    def __init__(self, inputxml):
        self.inputxml = inputxml
        self.header = self.get_header()
        self.footer = self.get_footer()

    def get_header(self):
        """
        read and return the mediawiki and siteinfo header from the
        input xml stream
        """
        header = []
        while True:
            line = self.inputxml.readline()
            if not line:
                sys.stderr.write("failed to read header, abrupt end to file, giving up")
                sys.exit(1)
            header.append(line)
            if "</siteinfo>" in line:
                break
        return header

    @staticmethod
    def get_footer():
        """
        return the xml mediawiki footer that is appended to all
        xml dump files
        """
        footer = ["</mediawiki>"]
        return footer


class XmlFileSplitter(object):
    """
    split a MediaWiki xml dump file into smaller files
    containing a given number of pages
    """
    def __init__(self, inputfile, ofile, pages, compression):
        self.inputxml = self.input_open(inputfile)
        self.ofile = ofile
        self.numpages = pages
        self.wrapper = XmlWrapper(self.inputxml)
        self.compression = compression

    @staticmethod
    def input_open(inputfile):
        """
        open input stream if needed
        """
        if inputfile is None:
            return sys.stdin
        elif inputfile.endswith(".gz"):
            return gzip.open(inputfile, "r")
        elif inputfile.endswith(".bz2"):
            return bz2.BZ2File(inputfile, "r")
        else:
            return open(inputfile, "r")

    def input_close(self):
        """
        close input stream if needed
        """
        if self.inputxml != sys.stdin:
            self.inputxml.close()

    def write_page(self, fhandle):
        """
        write one page from input xml stream to output file handle

        returns:
            True on EOF of input file
            False otherwise
        """
        written = False
        while True:
            line = self.inputxml.readline()
            if not line or line == '</mediawiki>':
                self.input_close()
                if written:
                    # we are in the middle of a page and got EOF. whine.
                    sys.stderr.write("input file ended in middle of page, giving up")
                    sys.exit(1)
                else:
                    return True
            fhandle.write(line)
            written = True
            if "</page>" in line:
                break
        return False

    def output_open(self, file_index):
        """
        open a file for ouput with the appropriate name and compression type

        returns: file handle
        """
        filename = "{prefix}_{index}.xml".format(prefix=self.ofile, index=str(file_index).zfill(5))
        if self.compression is None:
            return open(filename, "w")
        elif self.compression == 'gzip':
            return gzip.open(filename + ".gz", "wb")
        elif self.compression == 'bzip2':
            return bz2.BZ2File(filename + ".bz2", "wb")

    def write_file(self, file_index):
        """
        write one file containing mediawiki header, footer,
        and the specified number of xml pages read from xmlstream
        on stdin.  if there are not enough pages in the input
        stream, the number of pages available will be written
        instead.

        returns:
        True on EOF of input file
        False otherwise
        """
        fhandle = self.output_open(file_index)
        if not fhandle:
            sys.stderr.write("failed to open file for output, giving up")
            sys.exit(1)
        for line in self.wrapper.header:
            fhandle.write(line)
        for _ in range(self.numpages):
            result = self.write_page(fhandle)
            if result:
                break
        for line in self.wrapper.footer:
            fhandle.write(line)
        fhandle.close()
        return result

    def write_pages(self):
        """
        write output xml files, reading content from input,
        writing pagenum pages to each output file, except
        possibly the last one, which may have fewer
        """
        input_eof = False
        file_index = 1
        while not input_eof:
            input_eof = self.write_file(file_index)
            file_index += 1


def do_main():
    """
    main entry point
    """
    pages, inputfile, ofile, compression = get_opts()
    writer = XmlFileSplitter(inputfile, ofile, pages, compression)
    writer.write_pages()


if __name__ == '__main__':
    do_main()
