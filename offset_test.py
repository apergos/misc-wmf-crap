#!/usr/bin/python3
import os
import sys
from subprocess import Popen


# is it worth it to have some sort of progress that shows the size
# of the written file in the real code?


GZIPMARKER = b'\x1f\x8b\x08\x00'


def get_header_offset(filename):
    with open(filename, "rb") as infile:
        # skip the first byte
        try:
            infile.seek(1, os.SEEK_SET)
            max_offset = 1000000
            buffer = infile.read(max_offset)
        except IOError:
            return None
        buffer_offset = buffer.find(GZIPMARKER)
        if buffer_offset >= 0:
            # because we skipped the first byte, add that here
            return buffer_offset + 1
    return None


def get_footer_offset(filename):
    with open(filename, "rb") as infile:
        # empty files or files with only a footer will return None
        # here (too short) and that's ok, we might as well fail out on them
        # by now they should have already been moved out of the way
        # by the previous job but, just in case...
        max_offset = 100
        try:
            filesize = infile.seek(0, os.SEEK_END)
            infile.seek(filesize - max_offset, os.SEEK_SET)
            buffer = infile.read()
        except IOError:
            return None
        buffer_offset = buffer.find(GZIPMARKER)
        if buffer_offset >= 0:
            return filesize - (len(buffer) - buffer_offset)
    return None


def dump_file(filename, outfile, header_offset, footer_offset, outfile_size):
    command = ['dd', 'if=' + filename, 'of=' + outfile,
               'skip=' + str(header_offset),
               'count=' + str(footer_offset - header_offset),
               'iflag=skip_bytes,count_bytes',
               'seek=' + str(outfile_size), 'oflag=seek_bytes']
    print("running command", " ".join(command))
    proc = Popen(command)
    output, error = proc.communicate()
    if output:
        print("here is some output", output)
    if error:
        print("here is an error message", error)
    if proc.returncode:
        print("command returned with error code", proc.returncode)


def get_file_size(filename):
    try:
        filesize = os.stat(filename).st_size
    except Exception:
        return None
    return filesize


def filter_one_stubfile(filename, outfile):
    header_offset = get_header_offset(filename)
    footer_offset = get_footer_offset(filename)
    outfile_size = get_file_size(outfile)
    if not outfile_size:
        # this is used to decide where to seek to when outfile is opened
        outfile_size = 0
    print("header offset:", header_offset, "footer_offset:", footer_offset,
          "output file current size:", outfile_size)
    dump_file(filename, outfile, header_offset, footer_offset, outfile_size)


def write_header(filename, outfile):
    header_offset = get_header_offset(filename)
    print("header offset:", header_offset)
    outfile_size = get_file_size(outfile)
    if not outfile_size:
        # this is used to decide where to seek to when outfile is opened
        outfile_size = 0
    dump_file(filename, outfile, 0, header_offset, outfile_size)


def write_footer(filename, outfile):
    footer_offset = get_footer_offset(filename)
    infile_size = get_file_size(filename)
    outfile_size = get_file_size(outfile)
    if not outfile_size:
        # this is used to decide where to seek to when outfile is opened
        outfile_size = 0
    print("footer_offset:", footer_offset,
          "output file current size:", outfile_size)
    dump_file(filename, outfile, footer_offset, infile_size, outfile_size)


def rewrite_stubs(infiles, outfile):
    if len(infiles) == 1:
        write_header(infiles[0], "header-" + outfile)
        filter_one_stubfile(infiles[0], outfile)
        write_footer(infiles[0], "footer-" + outfile)
    else:
        write_header(infiles[0], outfile)
        for infile in infiles:
            filter_one_stubfile(infile, outfile)
        write_footer(infiles[-1], outfile)


def do_main():
    filenames = sys.argv[1:]
    if len(filenames) != 2:
        message = """Usage: python3 offset_test.py infile,infile,... outfile
This script writes the header from the first file, the body from all files in order,
and then the footer from the last file, into the specified output file. This assumes
that the input files all consist of three gzipped bits of content: header, body, footer.

If only only one file is specified as an input file instead of a comma-separated list,
the header and footer are stripped from it and the result written to the specified
output file. Additionally the header will be written to header-<outfile> and the footer
to footer-<outfile>.

Example: python3 offset_test.py elwikt-20190108-stub-articles1.xml.gz,elwikt-20190108-stub-articles2.xml.gz,\\
elwikt-20190108-stub-articles3.xml.gz,elwikt-20190108-stub-articles4.xml.gz  \\
elwikt-20190108-stub-articles-all.xml.gz

Example with one input file: python3 offset_test.py fullstubfile.gz fullstubextracted.gz"""
        sys.stderr.write(message + "\n")
        sys.exit(1)
    infiles = filenames[0].split(',')
    rewrite_stubs(infiles, filenames[1])


if __name__ == '__main__':
    do_main()
