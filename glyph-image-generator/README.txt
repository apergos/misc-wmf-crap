This script is intended for use for mass generation of small images,
varying in a predictable way, with embedded PNG data that can be extracted
by standard utilities.

Eventually it will allow for the upload of those images to a Wikimedia wiki,
and the addition of structured data to those uploads, based on the embedded data.

For Wikimedia's development-prep platform, the testbed equivalent of commons
does not have nearly enough structured data for testing. Uploading things manually
is slow. Generating things manually is slow.

This script creates images based on characters from some unicode point range,
which can be specified by providing the start and end character as arguments,
or by passing hex arguments of the form \xnn\xnn\xnn...  which will be converted
for you.

You need to supply:
the author name (used for the embedded 'Author' field)
the background color value and name (defaults to a sort of golden shade)
the font face
the width of the image (images will be square because we are lazy)
the output path including the base file name for the images (the glyph name and
    a .png suffix will be added automatically)
the start glyph (if no end glyph is supplied, just the one image will be created)

You can put some of these values in a config file to make your life easier;
see glyphs.conf for an example. You can specify the name of the config file on
the command line.

If you provide -h then a usage message will be printed.

This script currently creates two files for each glyph, one unlabelled and one
(starting with 'new-') labelled with metadata. Expect that to get cleaned up in
future runs.


