#!/usr/bin/python3
# encoding: utf-8
'''
to be used to generate a pile of images for import to deployment-prep commons
requires: pycairo, Pillow >= 6.0.0
'''
import configparser
import getopt
import getpass
import os
import sys
import time
import base64

import cairo
import requests
from PIL import Image
from PIL.PngImagePlugin import PngInfo


# FIXME we need to respect replag and we don't.


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

    for setting in ['author', 'bgcolor', 'canvas_width', 'font', 'log',
                    'output_path', 'user', 'agent', 'wait', 'wiki_api_url']:
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

    if 'author' not in args or args['author'] is None:
        usage("mandatory '--author' argument is missing")
    if 'wiki_api_url' not in args or args['wiki_api_url'] is None:
        usage("mandatory 'apiurl' config setting is missing")
    if 'agent' not in args or args['agent'] is None:
        usage("mandatory 'agent' config setting is missing")
    if args['start_glyph'] is None:
        usage("mandatory 'start_glyph' argument is missing")
    if args['end_glyph'] is None:
        args['end_glyph'] = args['start_glyph']

    if not args['wait'].isdigit():
        usage("argument 'wait' must be a number")

    if args['wiki_user'] is None:
        args['wiki_user'] = args['author']

    args['jobs'] = args['jobs'].split(',')
    known_jobs = ['generate', 'upload', 'caption']
    for job in args['jobs']:
        if job not in known_jobs:
            usage("bad value to --jobs, known values are " + ",".join(known_jobs))


def get_arg(opt, val, args):
    '''set one arg from opt/val'''
    if opt in ["-c", "--config"]:
        args['config'] = val
    elif opt in ["-a", "--author"]:
        args['author'] = val
    elif opt in ["-b", "--bgcolor"]:
        args['bgcolor'] = val
    elif opt in ["-f", "--font"]:
        args['font'] = val
    elif opt in ["-j", "--jobs"]:
        args['jobs'] = val
    elif opt in ["-s", "--start"]:
        args['start_glyph'] = val
    elif opt in ["-e", "--end"]:
        args['end_glyph'] = val
    elif opt in ["-o", "--output"]:
        args['output_path'] = val
    elif opt in ["-u", "--user"]:
        args['wiki_user'] = val
    elif opt in ["-w", "--width"]:
        args['canvas_width'] = val
    elif opt in ["-W", "--wait"]:
        args['wait'] = val
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
            'log': None,
            'jobs': 'generate,upload,caption',
            'end_glyph': None,
            'output_path': None,
            'wiki_user': None,
            'wait': '15',
            'verbose': False,
            'help': False}
    return args


def parse_args():
    '''get args passed on the command line
    and return as a dict'''
    args = {'config': 'glyphs.conf'}

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "a:b:c:f:j:l:o:u:w:s:e:vh",
            ["author=", "bgcolor=", "config=", "font=",
             "output=", "width=", "start=", "end=", "log=",
             "user=", "jobs=", "verbose", "help"])

    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if not get_arg(opt, val, args) and not get_flag(opt, args):
            usage("Unknown option specified: <%s>" % opt)

    if remainder:
        usage("Unknown option(s) specified: {opt}".format(opt=remainder[0]))

    conf = get_config(args['config'])
    merge_args(conf, get_default_args(), args)
    validate_args(args)

    return args


def usage(message=None):
    '''display usage info about this script'''
    if message is not None:
        print(message)
    usage_message = """Usage: generate_glyph_pngs.py --font <font-name> --author <name>
         --output <path-to-output-file> --start <start-glyph> [--end <end-glyph>]
         [--config] <path> [--bgcolor <hex:name>] [--width <canvas-width-in-px>]
         [--wait <seconds>] [--verbose] | --help

Arguments:

  --font    (-f):   name of font to be used; if there are spaces in the name, it should be quoted
                    default: none
  --author  (-a):   name of author to be added as Author field in PNG file
                    default: name of author
  --user    (-u):   name of wiki user
                    default: name of author
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
  --config  (-c):   name of file with configuration settings
                    default: glyphs.conf in current working directory
  --width   (-w):   width of canvas (of image) in pixels; height and width will be the same
                    default: 32
  --wait    (-W):   number of seconds to wait between uploads or caption additions
                    default: 15
  --jobs    (-j):   comma separated list of jobs to do
                    default: generate,upload,caption  (i.e. all of them)
  --log     (-l):   log file for logging the http status code of uploads
                    default: none, messages are logged to stderr
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
        basename, filename = os.path.split(path)
        newname = os.path.join(basename, 'new-' + filename)
        image.save(newname, pnginfo=info)

        if args['verbose']:
            with Image.open(newname) as image:
                print("new image is", newname)
                print(image.info)
    os.unlink(path)
    os.rename(newname, path)


def wiki_login(args):
    '''
    log into the wiki given by the wiki api url in the config file,
    get a crsf token, return it
    '''
    # FIXME if we get an error reply we should retry a few times (but
    # only if the error is not 'bad password'

    password = getpass.getpass('Wiki user password: ')
    headers = {'User-Agent': args['agent']}
    params = {'action': 'query', 'format': 'json', 'utf8': '',
              'meta': 'tokens', 'type': 'login'}
    result = requests.post(args['wiki_api_url'], data=params, headers=headers)
    try:
        login_token = result.json()['query']['tokens']['logintoken']
    except Exception:  # pylint: disable=broad-except
        sys.stderr.write(result.text)
        sys.exit(-1)
    cookies = result.cookies.copy()
    params = {'action': 'login', 'format': 'json', 'utf8': '',
              'lgname': args['wiki_user'], 'lgpassword': password, 'lgtoken': login_token}
    result = requests.post(args['wiki_api_url'], data=params, cookies=cookies, headers=headers)
    # {'warnings': {'main': {'*': 'Unrecognized parameter: password.'}},
    #  'login': {'result': 'Failed',
    #            'reason': 'The supplied credentials could not be authenticated.'}}
    body = result.json()
    if 'login' not in body or body['login']['result'] == 'Failed':
        sys.stderr.write(result.text + "\n")
        sys.exit(1)
    cookies = result.cookies.copy()

    params = {'action': 'query', 'format': 'json', 'meta': 'tokens'}
    result = requests.post(args['wiki_api_url'], data=params, cookies=cookies)
    return cookies, result.json()['query']['tokens']['csrftoken']


def get_image_info(path):
    '''given the path to a png image, get and return the image info from it'''
    with Image.open(path) as image:
        image.load()
        info = image.text
    return info


def get_file_page_text(image_info, today):
    '''given the image info from a png image, concoct suitable file page
    text contents for it and return them'''
    # {'Author': 'ArielGlenn',
    #  'Description': 'Character 見 rendered from Noto Serif CJK JP with black border on yellow',
    #  'Title': 'Icon_for_char_見_black_yellow_32x32.png',
    #  'Software': 'pycairo and pillow'}
    description = image_info['Description']
    category = 'Chinese letters'
    author = image_info['Author']
    # double {{ for templates becomes {{{{ with format()
    page_text_tmpl = """=={{int:filedesc}}==
{{{{Information
|description={{{{en|1={description}}}}}
|date={today}
|source={{{{own}}}}
|author={author}
}}}}

=={{{{int:license-header}}}}==
{{{{self|cc-by-sa-4.0}}}}

[[Category:{category}]]
"""
    return page_text_tmpl.format(description=description,
                                 today=today,
                                 author=author,
                                 category=category)


def get_file_page_comment():
    '''given the image info from a png image, cobble together a reasonable
    upload edit summary and return it'''
    return 'batch upload of glyphs from font Noto Serif CJK JP'


def get_file_page_title(image_info):
    '''given the image info from a png image, grab the image title out
    of the image info and return it to be used as the File page title
    during upload
    '''
    return image_info['Title']


def log(item, status, success, args):
    '''
    log the results of an attempted upload of an image or
    addition of a caption to a log file; this can be used
    to figure out what uploads or captions to retry, where
    to restart if the script is interrupted or dies, etc.
    '''
    log_entry = "{status}: (success:{success}) {item}\n".format(
        status=status, success=success, item=item)
    if 'log' in args and args['log']:
        with open(args['log'], "a+") as logfile:
            logfile.write(log_entry)
    else:
        sys.stderr.write(log_entry)


def rfc2047_encode(text):
    '''convert utf8 string into something urllib3 likes via rfc2047, ugh'''
    return str('=?utf-8?B?{}?='.format(base64.b64encode(text.encode('utf-8'))))


def upload_image(path, token, cookies, args, today):
    '''
    given path to the image file, a crsf token, and general args,
    try to upload it, with some reasonable number of retries.
    '''
    image_info = get_image_info(path)
    text = get_file_page_text(image_info, today)
    comment = get_file_page_comment()
    title = get_file_page_title(image_info)

    # urllib3 doesn't like utf8 filenames, nor byte strings, so there you have it
    # see https://github.com/psf/requests/issues/4218
    encoded_title = rfc2047_encode(title)

    params = {'action': 'upload', 'format':'json', 'filename': title,
              'comment': comment, 'text': text, 'token': token, 'ignorewarnings': 1}
    files = {'file': (encoded_title, open(path, 'rb'), 'multipart/form-data')}

    response = requests.post(args['wiki_api_url'], data=params, files=files, cookies=cookies,
                             headers={'User-Agent': args['agent']})

    success = False
    if response.status_code == 200:
        results = response.json()
        if 'upload' in results:
            if results['upload']['result'] == 'Success':
                success = True
    if not success:
        sys.stderr.write(response.text + "\n")
    log(title, response.status_code, success, args)
    return success


def get_mediainfo_id(image_info, args):
    '''given info for an image, find the mediainfo id for the image
    on the specified wiki'''
    title = get_file_page_title(image_info)
    params = {'action': 'query',
              'prop': 'info',
              'titles': 'File:' + title,
              'format': 'json'}
    response = requests.post(args['wiki_api_url'], data=params,
                             headers={'User-Agent': args['agent']})
    success = False
    if response.status_code == 200:
        results = response.json()
        if 'query' in results:
            if list(results['query']['pages'].keys()):
                success = True
    if not success:
        sys.stderr.write(response.text + "\n")
        return None

    page_id = list(response.json()['query']['pages'].keys())[0]
    if page_id == '-1':
        sys.stderr.write(response.text + "\n")
        return None

    return 'M' + list(response.json()['query']['pages'].keys())[0]


def add_caption(path, token, cookies, args):
    '''construct caption text for the image, find the
    image page on the wiki, and add a caption to it
    via the MediaWiki api'''
    image_info = get_image_info(path)
    caption = image_info['Description']
    comment = 'add caption'
    minfo_id = get_mediainfo_id(image_info, args)
    if minfo_id is None:
        sys.stderr.write("Failed to retrieve mediainfo id for " + path + ", skipping\n")
        return False
    if args['verbose']:
        print("Going to add caption for entity id", minfo_id)
    params = {'action': 'wbeditentity',
              'format': 'json',
              'id': minfo_id,
              'data': '{"labels":{"en":{"language":"en","value":"' + caption + '"}}}',
              'summary': comment,
              'token': token}
    response = requests.post(args['wiki_api_url'], data=params, cookies=cookies,
                             headers={'User-Agent': args['agent']})

    success = False
    if response.status_code == 200:
        results = response.json()
        if 'success' in results:
            if results['success'] == 1:
                success = True
    if not success:
        sys.stderr.write(response.text + "\n")
    log(caption, response.status_code, success, args)
    return success


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

    if 'generate' in args['jobs']:
        # create the images
        for glyph in range(ord(convert_hex(args['start_glyph'])),
                           ord(convert_hex(args['end_glyph'])) + 1):
            glyph = chr(glyph)
            file_path = convert_path(args['output_path'], glyph)
            write_png(file_path, glyph, args)
            add_png_metadata(file_path, glyph, metadata, args)

    if 'upload' in args['jobs']:
        # upload the images
        cookies, crsf_token = wiki_login(args)
        # format we like in commons uploads: YYYY-MM-DD HH:MM:SS
        today = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        failed = 0
        for glyph in range(ord(convert_hex(args['start_glyph'])),
                           ord(convert_hex(args['end_glyph'])) + 1):
            glyph = chr(glyph)
            file_path = convert_path(args['output_path'], glyph)
            # wait between requests
            time.sleep(int(args['wait']))

            if not upload_image(file_path, crsf_token, cookies, args, today):
                failed += 1
            else:
                failed = 0
            if failed > 5:
                sys.stderr.write("Giving up on uploads after 5 consecutive failures, " +
                                 "check log for details")
                sys.exit(1)

    if 'caption' in args['jobs']:
        # add captions to the images
        cookies, crsf_token = wiki_login(args)
        failed = 0
        for glyph in range(ord(convert_hex(args['start_glyph'])),
                           ord(convert_hex(args['end_glyph'])) + 1):
            glyph = chr(glyph)
            file_path = convert_path(args['output_path'], glyph)
            # wait between requests
            time.sleep(int(args['wait']))
            if not add_caption(file_path, crsf_token, cookies, args):
                failed += 1
            else:
                failed = 0
            if failed > 5:
                sys.stderr.write("Giving up on captions after 5 consecutive failures, " +
                                 "check log for details")
                sys.exit(1)


if __name__ == '__main__':
    do_main()
