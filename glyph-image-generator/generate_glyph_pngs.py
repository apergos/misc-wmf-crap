#!/usr/bin/python3
'''
to be used to generate a pile of images for import to deployment-prep commons
requires: pycairo, Pillow >= 6.0.0
'''
import configparser
import getopt
import sys
import cairo
from PIL import Image
from PIL.PngImagePlugin import PngInfo


def write_png(output_path, glyph, args):
    '''
    given an output file path, the glyph to be displayed, and a dict of args
    including the font face, the background color and the width of the output
    image (height will be the same as the width), create an image with a 2 pixel
    border, a background with the specified color and the centered glyph in black.
    '''
    font = args['font']
    canvas_width = int(args['canvas_width'])
    canvas_height = canvas_width

    # for bitmap output. all units in pixels
    surface = cairo.ImageSurface(cairo.FORMAT_RGB24, canvas_width, canvas_height)
    ctx = cairo.Context(surface)

    # background
    ctx.rectangle(0, 0, canvas_width - 1, canvas_height - 1)
    ctx.set_source_rgb(args['color_pycairo'][0], args['color_pycairo'][1], args['color_pycairo'][2])
    ctx.fill()
    # border
    ctx.set_line_width(2)
    ctx.set_source_rgb(0.1, 0.1, 0.1)
    ctx.stroke()

    # glyph
    ctx.set_source_rgb(0, 0, 0)
    ctx.set_font_size(canvas_width - 2)
    ctx.select_font_face(font, cairo.FONT_SLANT_NORMAL, cairo.FONT_WEIGHT_BOLD)
    (x_bearing, y_bearing, text_width, text_height, _dx, _dy) = ctx.text_extents(glyph)
    if args['verbose']:
        print("x_advance is", _dx, "and y_advance is", _dy)
        print("text_width is", text_width, "and text_height is", text_height)
        print("x_bearing is", x_bearing, "and y_bearing is", y_bearing)

    options = cairo.FontOptions()
    options.set_antialias(cairo.ANTIALIAS_DEFAULT)
    ctx.set_font_options(options)

    ctx.move_to(canvas_width/2 - text_width/2 - x_bearing,
                canvas_height/2 - text_height/2 - y_bearing)
    ctx.show_text(glyph)

    surface.write_to_png(output_path)


def get_config(configfile):
    '''read and parse config file entries'''
    config = {}
    if configfile is None:
        return config
    parser = configparser.ConfigParser()
    parser.read(configfile)
    if not parser.has_section('all'):
        parser.add_section('all')

    for setting in ['author', 'bgcolor', 'canvas_width', 'font', 'output_path']:
        if parser.has_option('all', setting):
            config[setting] = parser.get('all', setting)
    return config


def validate_args(args):
    '''
    check that args are valid, not missing, etc.
    and do any necessary arg conversions too
    '''
    if not args['canvas_width'].isdigit():
        usage('width must be the number of pixels')

    if ':' not in args['bgcolor']:
        usage('Bad bgcolor argument')
    args['color_rgb'] = convert_to_rgb(args['bgcolor'].split(':')[0])
    args['color_name'] = args['bgcolor'].split(':')[1]
    if not args['color_rgb'] or not args['color_name']:
        usage('Bad bgcolor argument')
    args['color_pycairo'] = convert_rgb_to_pycairo(args['color_rgb'])

    if args['author'] is None:
        usage("mandatory '--author' argument is missing")


def get_arg(opt, val, args):
    '''set one arg from opt/val'''
    if opt in ["-c", "--configfile"]:
        args['configfile'] = val
    elif opt in ["-a", "--author"]:
        args['author'] = val
    elif opt in ["-b", "--bgcolor"]:
        args['bgcolor'] = val
    elif opt in ["-f", "--font"]:
        args['font'] = val
    elif opt in ["-s", "--start"]:
        args['start_glyph'] = val
    elif opt in ["-e", "--end"]:
        args['end_glyph'] = val
    elif opt in ["-o", "--output"]:
        args['output_path'] = val
    elif opt in ["-w", "--width"]:
        args['canvas_width'] = val
    else:
        return False

    return True


def get_flag(opt, args):
    '''set one flag from opt'''
    if opt in ["-v", "--verbose"]:
        args['verbose'] = True
    elif opt in ["-h", "--help"]:
        usage('Help for this script\n')
    else:
        return False
    return True


def merge_args(conf, default_args, args):
    '''given a (possibly empty) config dict, a dict of default
    args, and a dict of args passed in from the command line,
    return a dict of args using command line values first, config
    settings second, and defaults as a last fallback.'''
    for setting in conf:
        if setting not in args or args[setting] is None:
            args[setting] = conf[setting]
    for default in default_args:
        if default not in args or args[default] is None:
            args[default] = default_args[default]


def get_default_args():
    '''return a dict of the default values for args'''
    args = {'font': None,
            'author': None,
            'bgcolor': '#E5CC99:golden',
            'canvas_width': '32',
            'start_glyph': None,
            'end_glyph': None,
            'output_path': None,
            'verbose': False,
            'help': False}
    return args


def parse_args():
    '''get args passed on the command line
    and return as a dict'''
    args = {'configfile': 'glyphs.conf'}

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "a:b:c:f:o:w:s:e:vh", ["author=", "bgcolor=", "configfile=", "font=",
                                                 "output=", "width=", "start=", "end=",
                                                 "verbose", "help"])

    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if not get_arg(opt, val, args) and not get_flag(opt, args):
            usage("Unknown option specified: <%s>" % opt)

    if remainder:
        usage("Unknown option(s) specified: {opt}".format(opt=remainder[0]))

    conf = get_config(args['configfile'])
    merge_args(conf, get_default_args(), args)
    validate_args(args)

    return args


def usage(message=None):
    '''display usage info about this script'''
    if message is not None:
        print(message)
    usage_message = """Usage: generate_glyph_pngs.py --font <font-name> --author <name>
         --output <path-to-output-file> --start <start-glyph> [--end <end-glyph>]
         [--bgcolor <hex:name>] [--width <canvas-width-in-px>] [--verbose] | --help

Arguments:

  --font    (-f):   name of font to be used; if there are spaces in the name, it should be quoted
                    default: none
  --author  (-a):   name of author to be added as Author field in PNG file
                    default: none
  --bgcolor (-b):   entry of the form <hex-color-value>:<color-name>
                    the hex color will be used for the background of the image
                    the color name will be used in the description of the image
                    default: #E5CC99:golden
  --output  (-o):   path to basename of file to output; the glyph name will be concatenated onto
                    the filename and the .png extension added. Example:
                    --output /path/to/fun-items will result in generation of files named
                    /path/to/fun-items-<glyph>.png
                    default:none
  --start   (-s):   first glyph for which to produce a file
                    default: none
  --end     (-e):   last glyph for which to produce a file
                    default: same as start glyph (only one file will be produced)
  --width   (-w):   width of canvas (of image) in pixels; height and width will be the same
                    default: 32
  --verbose (-v):   display messages about files as they are created
                    default: false
  --help    (-h):   display this usage message

Notes: font is always bold weight; border, background and text colors are fixed

Example uses:
   python3 generate_glyph_pngs.py -f 'Noto Serif CJK JP' -w 32 -o myfile -a ArielGlenn -s 見
   python3 generate_glyph_pngs.py -f 'Noto Serif CJK JP' -o myfile -a ArielGlenn \
                                  -s 0xe8a68b -e 0xe8a68f"""

    print(usage_message)
    sys.exit(1)


def convert_hex(text):
    '''
    if the text is a string of hex bytes, convert that to unicode
    and use it instead
    '''
    if not text.startswith('0x'):
        return text

    text = text[2:]
    hex_digits = ''.join([letter for letter in text if letter not in 'abcdefABCDEF1234567890'])
    if hex_digits:
        return text
    if len(text) % 2:
        return text

    # we have a valid hex string, let's convert it then
    return bytes.fromhex(text).decode('utf8')


def convert_path(path, glyph):
    '''
    add - + glyph to the filename, and make sure that the
    .png suffix is added afterwards
    '''
    if path.endswith('.png'):
        path = path[0:-4]
    return path + '-' + glyph + '.png'


def convert_to_rgb(hexcolor):
    '''
    convert a hex color value (#aabbcc) to rgb triplet (nnn, nnn, nnn)
    '''
    if not hexcolor.startswith('#'):
        return None
    if not len(hexcolor) == 7:
        # yeah that's right. six digits and noooo shortcuts.
        return None
    if [letter for letter in hexcolor if letter not in '#1234567890abcdefABCDEF']:
        return None
    return (int(hexcolor[1:3], 16), int(hexcolor[3:5], 16), int(hexcolor[5:7], 16))


def convert_rgb_to_pycairo(rgb_color):
    '''
    convert an rgb triplet (nnn, nnn, nnn) to a triplet suitable for pycairo (0.n, 0.n, 0.n)
    '''
    return (rgb_color[0]/255, rgb_color[1]/255, rgb_color[2]/255)


def add_png_metadata(path, glyph, metadata, args):
    '''
    given the path to a png file and the glyph it contains,
    write appropriate metadata fields into the file
    '''
    metadata['Description'] = metadata['_Description_tmpl'].format(glyph=glyph)
    metadata['Title'] = metadata['_Title_tmpl'].format(glyph=glyph, color=args['color_name'])

    with Image.open(path) as image:
        info = PngInfo()
        for entry in ["Author", "Description", "Title", "Software"]:
            if not entry.startswith('_'):
                info.add_itxt(entry, metadata[entry], "en", entry)
        image.save('new-' + path, pnginfo=info)

        if args['verbose']:
            with Image.open('new-' + path) as image:
                print("new image is", 'new-' + path)
                print(image.info)


def do_main():
    '''
    entry point
    '''
    args = parse_args()

    render_info = "rendered from {font} with black border on {color}".format(
        font=args['font'], color=args['color_name'])
    metadata = {
        'Author': args['author'],
        'Software': "pycairo and pillow",
        '_Description_tmpl': "Character {glyph} " + render_info,
        '_Title_tmpl': "Icon_for_char_{glyph}_black_{color}_32x32.png"
    }

    for glyph in range(ord(convert_hex(args['start_glyph'])),
                       ord(convert_hex(args['end_glyph'])) + 1):
        glyph = chr(glyph)
        file_path = convert_path(args['output_path'], glyph)
        write_png(file_path, glyph, args)
        add_png_metadata(file_path, glyph, metadata, args)


if __name__ == '__main__':
    do_main()
