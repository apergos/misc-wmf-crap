#!/usr/bin/python3
# encoding: utf-8
'''
to be used to generate a pile of images for import to deployment-prep commons
and add captions to them depending on font, character and border/background
colors

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
# FIXME we need to make the waits happen in a nice way, right now we
#       call the wait before the routine that makes one -- or maybe two! -- edits
# FIXME if we get an error logging in we should retry a few times (but
#       only if the error is not 'bad password')


def usage(message=None):
    '''display usage info about this script'''
    if message is not None:
        print(message)
    usage_message = """Usage: generate_glyph_pngs.py --font <font-name> --author <name>
         --output <path-to-output-file> --start <start-glyph> [--end <end-glyph>]
         [--config] <path> [--bgcolor <hex:name>] [--bordercolor <hex:name>]
         [--width <canvas-width-in-px>] [--wait <seconds>] [--verbose] | --help

Arguments:

  --font        (-f):  name of font to be used; if there are spaces in the name, it should be quoted
                       default: none
  --author      (-a):  name of author to be added as Author field in PNG file
                       default: name of author
  --user        (-u):  name of wiki user
                       default: name of author
  --bgcolor     (-b):  entry of the form <hex-color-value>:<color-name>
                       the hex color will be used for the background of the image
                       the color name will be used in the description of the image
                       default: #E5CC99:golden
  --bordercolor (-B):  entry of the form <hex-color-value>:<color-name>
                       the hex color will be used for the border of the image
                       the color name will be used in the description of the image
                       default: #111111:black
  --output      (-o):  path to basename of file to output; the glyph name will be concatenated onto
                       the filename and the .png extension added. Example:
                       --output /path/to/fun-items will result in generation of files named
                       /path/to/fun-items-<glyph>.png
                       default:none
  --start       (-s):  first glyph for which to produce a file
                       default: none
  --end         (-e):  last glyph for which to produce a file
                       default: same as start glyph (only one file will be produced)
  --config      (-c):  name of file with configuration settings
                       default: glyphs.conf in current working directory
  --width       (-w):  width of canvas (of image) in pixels; height and width will be the same
                       default: 32
  --wait        (-W):  number of seconds to wait between uploads or caption additions
                       default: 15
  --jobs        (-j):  comma separated list of jobs to do
                       default: generate,upload,caption,additem,depicts  (i.e. all of them)
  --log         (-l):  log file for logging the http status code of uploads
                       default: none, messages are logged to stderr
  --verbose     (-v):  display messages about files as they are created
                       default: false
  --help        (-h):  display this usage message

Notes: font is always bold weight; text color is fixed

Example uses:
   python3 generate_glyph_pngs.py -f 'Noto Serif CJK JP' -w 32 -o myfile -a ArielGlenn -s 見
   python3 generate_glyph_pngs.py -f 'Noto Serif CJK JP' -o myfile -a ArielGlenn \
                                  -s 0xe8a68b -e 0xe8a68f"""

    print(usage_message)
    sys.exit(1)


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


class CairoHex():
    '''manage rgb and hex values for (py)cairo'''

    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def convert_rgb_to_pycairo(rgb_color):
        '''
        convert an rgb triplet (nnn, nnn, nnn) to a triplet suitable for pycairo (0.n, 0.n, 0.n)
        '''
        return (CairoHex.range_adjust(rgb_color[0]/255), CairoHex.range_adjust(rgb_color[1]/255),
                CairoHex.range_adjust(rgb_color[2]/255))

    @staticmethod
    def range_adjust(rgb_field):
        '''
        given a value that should be a floating point number between 0.1 and 0.9,
        adjust it so it is
        '''
        if rgb_field == 0:
            return 0.1
        if rgb_field == 1:
            return 0.9
        return rgb_field


class ImageInfoProvider():
    '''read and extract image info'''
    def __init__(self):
        self.page_text_tmpl = """=={{int:filedesc}}==
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

    @staticmethod
    def get_image_info(path):
        '''given the path to a png image, get and return the image info from it'''
        with Image.open(path) as image:
            image.load()
            info = image.text
        return info

    @staticmethod
    def get_file_page_title(image_info):
        '''given the image info from a png image, grab the image title out
        of the image info and return it to be used as the File page title
        during upload
        '''
        return image_info['Title']

    @staticmethod
    def get_file_page_comment():
        '''given the image info from a png image, cobble together a reasonable
        upload edit summary and return it'''
        return 'batch upload of glyphs from font Noto Serif CJK JP'

    @staticmethod
    def get_glyph_from_image_info(image_info):
        '''give image_info structure, find a string in which
        the glyph is embedded in a regular way we know, extract
        it and return it'''
        # Icon_for_char_見_...
        fields = image_info['Title'].split('_', 4)
        return fields[3]

    def get_file_page_text(self, image_info, today):
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

        return self.page_text_tmpl.format(description=description,
                                          today=today,
                                          author=author,
                                          category=category)


class ArgParser():
    '''parse args, validate them, etc'''
    def __init__(self):
        self.args = {'config': 'glyphs.conf'}

    def get_arg(self, opt, val):
        '''set one arg from opt/val'''
        if opt in ["-c", "--config"]:
            self.args['config'] = val
        elif opt in ["-a", "--author"]:
            self.args['author'] = val
        elif opt in ["-b", "--bgcolor"]:
            self.args['bgcolor'] = val
        elif opt in ["-B", "--bordercolor"]:
            self.args['bordercolor'] = val
        elif opt in ["-f", "--font"]:
            self.args['font'] = val
        elif opt in ["-j", "--jobs"]:
            self.args['jobs'] = val
        elif opt in ["-s", "--start"]:
            self.args['start_glyph'] = val
        elif opt in ["-e", "--end"]:
            self.args['end_glyph'] = val
        elif opt in ["-o", "--output"]:
            self.args['output_path'] = val
        elif opt in ["-u", "--user"]:
            self.args['wiki_user'] = val
        elif opt in ["-w", "--width"]:
            self.args['canvas_width'] = val
        elif opt in ["-W", "--wait"]:
            self.args['wait'] = val
        else:
            return False
        return True

    def get_flag(self, opt):
        '''set one flag from opt'''
        if opt in ["-v", "--verbose"]:
            self.args['verbose'] = True
        elif opt in ["-h", "--help"]:
            usage('Help for this script\n')
        else:
            return False
        return True

    def parse_args(self):
        '''get args passed on the command line
        and return as a dict'''
        try:
            (options, remainder) = getopt.gnu_getopt(
                sys.argv[1:], "a:b:B:c:f:j:l:o:u:w:s:e:vh",
                ["author=", "bgcolor=", "bordercolor=",
                 "config=", "font=",
                 "output=", "width=", "start=", "end=", "log=",
                 "user=", "jobs=", "verbose", "help"])

        except getopt.GetoptError as err:
            usage("Unknown option specified: " + str(err))

        for (opt, val) in options:
            if not self.get_arg(opt, val) and not self.get_flag(opt):
                usage("Unknown option specified: <%s>" % opt)

        if remainder:
            usage("Unknown option(s) specified: {opt}".format(opt=remainder[0]))

        conf = self.get_config()
        self.merge_args(conf, self.get_default_args())
        self.validate_args()

    def merge_args(self, conf, default_args):
        '''given a (possibly empty) config dict, a dict of default
        args, and a dict of args passed in from the command line,
        return a dict of args using command line values first, config
        settings second, and defaults as a last fallback.'''
        for setting in conf:
            if setting not in self.args or self.args[setting] is None:
                self.args[setting] = conf[setting]
        for default in default_args:
            if default not in self.args or self.args[default] is None:
                self.args[default] = default_args[default]

    def get_config(self):
        '''read and parse config file entries'''
        configfile = self.args['config']
        config = {}
        if configfile is None:
            return config
        parser = configparser.ConfigParser()
        parser.read(configfile)
        if not parser.has_section('all'):
            parser.add_section('all')

        for setting in ['author', 'bgcolor', 'bordercolor', 'canvas_width', 'font', 'log',
                        'output_path', 'user', 'agent', 'wait', 'wiki_api_url',
                        'wikidata_api_url']:
            if parser.has_option('all', setting):
                config[setting] = parser.get('all', setting)
        return config

    @staticmethod
    def get_default_args():
        '''return a dict of the default values for args'''
        args = {'font': None,
                'author': None,
                'bgcolor': '#E5CC99:golden',
                'bordercolor': '#111111:black',
                'canvas_width': '32',
                'start_glyph': None,
                'log': None,
                'jobs': 'generate,upload,caption,additem,depicts',
                'end_glyph': None,
                'output_path': None,
                'wiki_user': None,
                'wait': '15',
                'verbose': False,
                'help': False}
        return args

    def check_mandatory_args(self):
        '''whine about missing mandatory args'''
        if 'author' not in self.args or self.args['author'] is None:
            usage("mandatory '--author' argument is missing")
        if 'wiki_api_url' not in self.args or self.args['wiki_api_url'] is None:
            usage("mandatory 'wiki_api_url' config setting is missing")
        if 'wikidata_api_url' not in self.args or self.args['wikidata_api_url'] is None:
            usage("mandatory 'wikidata_api_url' config setting is missing")
        if 'agent' not in self.args or self.args['agent'] is None:
            usage("mandatory 'agent' config setting is missing")
        if self.args['start_glyph'] is None:
            usage("mandatory 'start_glyph' argument is missing")

    def validate_args(self):
        '''
        check that args are valid, not missing, etc.
        and do any necessary arg conversions too
        '''
        self.check_mandatory_args()

        if not self.args['canvas_width'].isdigit():
            usage('width must be the number of pixels')

        if ':' not in self.args['bgcolor']:
            usage('Bad bgcolor argument')
        self.args['bgcolor_rgb'] = CairoHex.convert_to_rgb(self.args['bgcolor'].split(':')[0])
        self.args['bgcolor_name'] = self.args['bgcolor'].split(':')[1]
        if not self.args['bgcolor_rgb'] or not self.args['bgcolor_name']:
            usage('Bad bgcolor argument')
        self.args['bgcolor_pycairo'] = CairoHex.convert_rgb_to_pycairo(self.args['bgcolor_rgb'])

        if ':' not in self.args['bordercolor']:
            usage('Bad bordercolor argument')
        self.args['bordercolor_rgb'] = CairoHex.convert_to_rgb(
            self.args['bordercolor'].split(':')[0])
        self.args['bordercolor_name'] = self.args['bordercolor'].split(':')[1]
        if not self.args['bordercolor_rgb'] or not self.args['bordercolor_name']:
            usage('Bad bordercolor argument')
        self.args['bordercolor_pycairo'] = CairoHex.convert_rgb_to_pycairo(
            self.args['bordercolor_rgb'])

        if not self.args['wait'].isdigit():
            usage("argument 'wait' must be a number")

        if self.args['end_glyph'] is None:
            self.args['end_glyph'] = self.args['start_glyph']

        if self.args['wiki_user'] is None:
            self.args['wiki_user'] = self.args['author']

        self.args['jobs'] = self.args['jobs'].split(',')
        known_jobs = ['generate', 'upload', 'caption', 'additem', 'depicts']
        for job in self.args['jobs']:
            if job not in known_jobs:
                usage("bad value to --jobs, known values are " + ",".join(known_jobs))


class PngWriter():
    '''write a png with metadata'''
    def __init__(self, args, metadata):
        self.args = args
        self.metadata = metadata

    def write_rectangle(self, canvas_width, canvas_height, ctx):
        '''write a rectangle with given background
        and border color'''
        # background
        ctx.rectangle(0, 0, canvas_width - 1, canvas_height - 1)
        ctx.set_source_rgb(self.args['bgcolor_pycairo'][0],
                           self.args['bgcolor_pycairo'][1], self.args['bgcolor_pycairo'][2])
        ctx.fill_preserve()

        # border
        ctx.set_line_width(2)
        ctx.set_source_rgb(self.args['bordercolor_pycairo'][0], self.args['bordercolor_pycairo'][1],
                           self.args['bordercolor_pycairo'][2])
        ctx.stroke()

    def write_glyph(self, canvas_width, canvas_height, glyph, ctx):
        '''write a glyph centered in the rectangle'''
        ctx.set_source_rgb(0, 0, 0)
        ctx.set_font_size(canvas_width - 2)
        ctx.select_font_face(self.args['font'], cairo.FONT_SLANT_NORMAL, cairo.FONT_WEIGHT_BOLD)
        (x_bearing, y_bearing, text_width, text_height, _dx, _dy) = ctx.text_extents(glyph)
        if self.args['verbose']:
            print("x_advance is", _dx, "and y_advance is", _dy)
            print("text_width is", text_width, "and text_height is", text_height)
            print("x_bearing is", x_bearing, "and y_bearing is", y_bearing)

        options = cairo.FontOptions()
        options.set_antialias(cairo.ANTIALIAS_DEFAULT)
        ctx.set_font_options(options)

        ctx.move_to(canvas_width/2 - text_width/2 - x_bearing,
                    canvas_height/2 - text_height/2 - y_bearing)
        ctx.show_text(glyph)

    def write_png(self, output_path, glyph):
        '''
        given an output file path, the glyph to be displayed, and a dict of args
        including the font face, the background color and the width of the output
        image (height will be the same as the width), create an image with a 2 pixel
        border of the desired color, a background with the specified color and the
        centered glyph in black.
        '''
        canvas_width = int(self.args['canvas_width'])
        canvas_height = canvas_width

        # for bitmap output. all units in pixels
        surface = cairo.ImageSurface(cairo.FORMAT_RGB24, canvas_width, canvas_height)
        ctx = cairo.Context(surface)

        self.write_rectangle(canvas_width, canvas_height, ctx)
        self.write_glyph(canvas_width, canvas_height, glyph, ctx)
        surface.write_to_png(output_path)

    def add_png_metadata(self, path, glyph):
        '''
        given the path to a png file and the glyph it contains,
        write appropriate metadata fields into the file
        '''
        self.metadata['Description'] = self.metadata['_Description_tmpl'].format(glyph=glyph)
        self.metadata['Title'] = self.metadata['_Title_tmpl'].format(
            glyph=glyph, border=self.args['bordercolor_name'], color=self.args['bgcolor_name'])

        with Image.open(path) as image:
            info = PngInfo()
            for entry in ["Author", "Description", "Title", "Software"]:
                if not entry.startswith('_'):
                    info.add_itxt(entry, self.metadata[entry], "en", entry)
            basename, filename = os.path.split(path)
            newname = os.path.join(basename, 'new-' + filename)
            image.save(newname, pnginfo=info)

            if self.args['verbose']:
                with Image.open(newname) as image:
                    print("new image is", newname)
                    print(image.info)
        os.unlink(path)
        os.rename(newname, path)


class MediaInfoJob():
    '''jobs of various sorts: creating images, uploading them,
    setting mediainfo labels ('captions'), creating items
    for use in depicts statements for these images, etc.'''
    def __init__(self, args):
        self.args = args
        # format we like in commons uploads: YYYY-MM-DD HH:MM:SS
        self.today = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        render_info = "rendered from {font} with {border} border on {color}".format(
            font=args['font'], border=args['bordercolor_name'], color=args['bgcolor_name'])
        self.metadata = {
            'Author': self.args['author'],
            'Software': "pycairo and pillow",
            '_Description_tmpl': "Character {glyph} " + render_info,
            '_Title_tmpl': "Icon_for_char_{glyph}_{border}_{color}_32x32.png"
        }
        args['creds'] = {}
        args['creds']['commons'] = {'cookies': None, 'token': None}
        args['creds']['wikidata'] = {'cookies': None, 'token': None}

    def get_url(self, wikiname):
        '''given a wikiname, return the right api url for it.'''
        if wikiname == 'commons':
            return self.args['wiki_api_url']
        if wikiname == 'wikidata':
            return self.args['wikidata_api_url']
        return None

    @staticmethod
    def check_success(response):
        '''check the success of a response from MW api,
        whine if there is an issue, return True on success
        or False otherwise'''
        success = False
        if response.status_code == 200:
            success = True
            if 'error' in response.json():
                success = False
        if not success:
            sys.stderr.write(response.text + "\n")
        return success

    def get_mediainfo_id(self, image_info):
        '''given info for an image, find the mediainfo id for the image
        on the specified wiki'''
        title = ImageInfoProvider.get_file_page_title(image_info)
        params = {'action': 'query',
                  'prop': 'info',
                  'titles': 'File:' + title,
                  'format': 'json'}
        response = requests.post(self.args['wiki_api_url'], data=params,
                                 headers={'User-Agent': self.args['agent']})
        success = self.check_success(response)
        if not success:
            return None
        try:
            page_id = list(response.json()['query']['pages'].keys())[0]
        except Exception:
            return None

        if page_id == '-1':
            sys.stderr.write(response.text + "\n")
            return None

        return 'M' + list(response.json()['query']['pages'].keys())[0]

    def get_mediainfo(self, minfo_id):
        '''given the mediainfo id, get the mediainfo content
        in json format and return it'''
        params = {'action': 'wbgetentities',
                  'ids': minfo_id,
                  'format': 'json'}
        response = requests.post(self.args['wiki_api_url'], data=params,
                                 headers={'User-Agent': self.args['agent']})
        if self.args['verbose']:
            print("response:", response.text)
        success = self.check_success(response)
        if not success:
            return None

        try:
            mediainfo = response.json()['entities'][minfo_id]
        except Exception:
            return None
        return mediainfo

    def wikidata_has_depict_item(self, image_info):
        '''given login tokens, the path to the image and the embedded image
        info, see if corresponding item(s) exist for the character on
        wikidata (via the wikidata api url, as configured), and if so, return
        the list of matching items by Q-id, otherwise return an empty list'''
        glyph = ImageInfoProvider.get_glyph_from_image_info(image_info)
        params = {'action': 'wbsearchentities',
                  'props': '',
                  'search': glyph,
                  'language': 'en',
                  'strictlanguage': 'true',
                  'type': 'item',
                  'limit': '50',
                  'format': 'json'}
        response = requests.post(self.args['wikidata_api_url'], data=params,
                                 headers={'User-Agent': self.args['agent']})
        success = self.check_success(response)
        if not success:
            return None

        possibles = []
        for _index, entry in enumerate(response.json()['search']):
            if entry['match']['type'] != 'label':
                continue
            if entry['match']['language'] != 'en':
                continue
            if entry['match']['text'] != glyph:
                continue
            # full id with the Q
            possibles.append(entry['id'])
        return possibles

    @staticmethod
    def convert_path(path, glyph):
        '''
        add - + glyph to the filename, and make sure that the
        .png suffix is added afterwards
        '''
        if path.endswith('.png'):
            path = path[0:-4]
        return path + '-' + glyph + '.png'

    def generate_image(self, glyph):
        '''generate an image and save in the specified file path'''
        path = self.convert_path(self.args['output_path'], glyph)
        png_writer = PngWriter(self.args, self.metadata)
        png_writer.write_png(path, glyph)
        png_writer.add_png_metadata(path, glyph)

    @staticmethod
    def rfc2047_encode(text):
        '''convert utf8 string into something urllib3 likes via rfc2047, ugh'''
        return str('=?utf-8?B?{}?='.format(base64.b64encode(text.encode('utf-8'))))

    def upload_image(self, glyph):
        '''
        given path to the image file, a crsf token, and general args,
        try to upload it, with some reasonable number of retries.
        '''
        path = self.convert_path(self.args['output_path'], glyph)
        comment = ImageInfoProvider.get_file_page_comment()
        image_info = ImageInfoProvider.get_image_info(path)
        provider = ImageInfoProvider()
        text = provider.get_file_page_text(image_info, self.today)
        title = ImageInfoProvider.get_file_page_title(image_info)

        # urllib3 doesn't like utf8 filenames, nor byte strings, so there you have it
        # see https://github.com/psf/requests/issues/4218
        encoded_title = self.rfc2047_encode(title)

        params = {'action': 'upload', 'format':'json', 'filename': title,
                  'comment': comment, 'text': text,
                  'token': self.args['creds']['commons']['token'], 'ignorewarnings': 1}
        files = {'file': (encoded_title, open(path, 'rb'), 'multipart/form-data')}

        response = requests.post(self.args['wiki_api_url'], data=params,
                                 files=files, cookies=self.args['creds']['commons']['cookies'],
                                 headers={'User-Agent': self.args['agent']})

        success = self.check_success(response)
        log(title, response.status_code, success, self.args)
        return success

    def add_caption(self, glyph):
        '''construct caption text for the image, find the
        image page on the wiki, and add a caption to it
        via the MediaWiki api'''
        path = self.convert_path(self.args['output_path'], glyph)
        image_info = ImageInfoProvider.get_image_info(path)
        caption = image_info['Description']
        comment = 'add caption'
        minfo_id = self.get_mediainfo_id(image_info)
        if minfo_id is None:
            sys.stderr.write("Failed to retrieve mediainfo id for " + path + ", skipping\n")
            return False
        if self.args['verbose']:
            print("Going to add caption for entity id", minfo_id)
        params = {'action': 'wbeditentity',
                  'format': 'json',
                  'id': minfo_id,
                  'data': '{"labels":{"en":{"language":"en","value":"' + caption + '"}}}',
                  'summary': comment,
                  'token': self.args['creds']['commons']['token']}
        response = requests.post(self.args['wiki_api_url'], data=params,
                                 cookies=self.args['creds']['commons']['cookies'],
                                 headers={'User-Agent': self.args['agent']})

        success = self.check_success(response)
        log(caption, response.status_code, success, self.args)
        return success

    def add_wikidata_item(self, image_info):
        '''given login tokens, the path to the image and the embedded image
        info, add an item for the character in the image to wikidata (via
        the wikidata api url, as configured) and return the numeric part
        of the Q-id on success, otherwise return None'''
        glyph = ImageInfoProvider.get_glyph_from_image_info(image_info)
        comment = 'new item for CKJ character'
        contents = (
            '{"labels":{"en":{"language":"en","value":"' + glyph + '"}},' +
            '"descriptions":{"en":{"language":"en","value":"CJK (hanzi/kanji/hanja) character"}}}')
        params = {'action': 'wbeditentity',
                  'new': 'item',
                  'format': 'json',
                  'data': contents,
                  'summary': comment,
                  'token': self.args['creds']['wikidata']['token']}
        response = requests.post(self.args['wikidata_api_url'], data=params,
                                 cookies=self.args['creds']['wikidata']['cookies'],
                                 headers={'User-Agent': self.args['agent']})
        success = self.check_success(response)
        log(glyph, response.status_code, success, self.args)
        if not success:
            return None
        return response.json()['entity']['id']

    def add_item(self, glyph):
        '''add item to wikidata if it's not already there, so
        it can be used in later depicts statement'''
        path = self.convert_path(self.args['output_path'], glyph)
        image_info = ImageInfoProvider.get_image_info(path)
        possible_ids = self.wikidata_has_depict_item(image_info)
        if possible_ids:
            if self.args['verbose']:
                print("Item already exists, not adding. Ids:", possible_ids)
                return True
        else:
            item_id = self.add_wikidata_item(image_info)
            if item_id is not None:
                if self.args['verbose']:
                    print("for use in depicts statements, created new item with id", item_id)
                    return True

    @staticmethod
    def get_depicts_from_content(mediainfo):
        '''given mediainfo content for a specific mediainfo id,
        get any depicts statements out of there and collect all the
        target Q items in a list and return it'''
        depicts_targets = []
        if 'P180' in mediainfo['statements']:
            for entry in mediainfo['statements']['P180']:
                try:
                    depicts_targets.append(entry['mainsnak']['datavalue']['value']['id'])
                except Exception:
                    # some unknown value probably, move on
                    continue
        return depicts_targets

    def add_depicts(self, glyph):
        '''construct depicts statement for the image, find the
        image page on the wiki, and add the statement to it
        via the MediaWiki api'''
        path = self.convert_path(self.args['output_path'], glyph)
        image_info = ImageInfoProvider.get_image_info(path)
        minfo_id = self.get_mediainfo_id(image_info)
        if minfo_id is None:
            sys.stderr.write("Failed to retrieve mediainfo id for " + path + ", skipping\n")
            return False

        possible_ids = self.wikidata_has_depict_item(image_info)
        if possible_ids:
            # take the first one and hope :-D
            depicts_id = possible_ids[0]
            if self.args['verbose']:
                print("for use in depicts, found existing item with id", depicts_id)
        else:
            depicts_id = None
        if depicts_id is None:
            sys.stderr.write("No depicts id for image " + path + ", moving on\n")
            return False

        mediainfo = self.get_mediainfo(minfo_id)
        if mediainfo:
            existing_depicts = self.get_depicts_from_content(mediainfo)
            if depicts_id in existing_depicts:
                # it's already there, skip
                if self.args['verbose']:
                    glyph = ImageInfoProvider.get_glyph_from_image_info(image_info)
                    print("Depicts statement for", glyph, "with mediainfo id", minfo_id,
                          "and depicted", depicts_id, "already present, moving on")
                return True

        # FIXME Will this lose any caption that exists? do I need to combine this stuff with
        # the existing mediainfo content? I think there is an 'add' parameter, right??
        depicts = ('{"statements":"{"P180":[{{"mainsnak":{"snaktype": "value","property":"P180",' +
                   '"datavalue":{"value":{"entity-type":"item","numeric-id":' +
                   depicts_id + ',"id":"' + depicts_id + '"},"type":"wikibase-entityid"}}]}}')
        comment = 'add depicts statement'
        params = {'action': 'wbeditentity',
                  'format': 'json',
                  'id': minfo_id,
                  'data': depicts,
                  'summary': comment,
                  'token': self.args['creds']['commons']['token']}
        response = requests.post(self.args['wiki_api_url'], data=params,
                                 cookies=self.args['creds']['commons']['cookies'],
                                 headers={'User-Agent': self.args['agent']})

        success = self.check_success(response)
        glyph = ImageInfoProvider.get_glyph_from_image_info(image_info)
        log(glyph, response.status_code, success, self.args)
        return success

    def wiki_login(self, prompt, api_url):
        '''
        log into the wiki given by the wiki api url in the config file,
        prompting with the name of the wiki, get a crsf token, return it
        and the associated cookies
        '''
        password = getpass.getpass('Wiki user password ({wiki}): '.format(wiki=prompt))
        headers = {'User-Agent': self.args['agent']}
        params = {'action': 'query', 'format': 'json', 'utf8': '',
                  'meta': 'tokens', 'type': 'login'}
        result = requests.post(api_url, data=params, headers=headers)
        try:
            login_token = result.json()['query']['tokens']['logintoken']
        except Exception:  # pylint: disable=broad-except
            sys.stderr.write(result.text)
            sys.exit(-1)
        cookies = result.cookies.copy()
        params = {'action': 'login', 'format': 'json', 'utf8': '',
                  'lgname': self.args['wiki_user'], 'lgpassword': password, 'lgtoken': login_token}
        result = requests.post(api_url, data=params,
                               cookies=cookies, headers=headers)
        # {'warnings': {'main': {'*': 'Unrecognized parameter: password.'}},
        #  'login': {'result': 'Failed',
        #            'reason': 'The supplied credentials could not be authenticated.'}}
        body = result.json()
        if 'login' not in body or body['login']['result'] == 'Failed':
            sys.stderr.write(result.text + "\n")
            sys.exit(1)
        cookies = result.cookies.copy()

        params = {'action': 'query', 'format': 'json', 'meta': 'tokens'}
        result = requests.post(api_url, data=params, cookies=cookies)
        return cookies, result.json()['query']['tokens']['csrftoken']

    def do_job(self, job_name, job, wait=True, login=None):
        '''login is a list of wikis for which to log in, if any,
        job_name is the name of the job that will appear in errors, job is the actual method to
        be called, wait is true if there should be a wait between glyphs'''

        for wiki in login:
            self.args['creds'][wiki]['cookies'], self.args['creds'][wiki]['token'] = self.wiki_login(
                wiki, self.get_url(wiki))
        failed = 0
        for glyph in range(ord(CairoHex.convert_hex(self.args['start_glyph'])),
                           ord(CairoHex.convert_hex(self.args['end_glyph'])) + 1):
            glyph = chr(glyph)
            if wait:
                # wait between requests
                time.sleep(int(self.args['wait']))

            if not job(glyph):
                failed += 1
            else:
                failed = 0
            if failed > 5:
                sys.stderr.write("Giving up on {job} ".format(job=job_name) +
                                 "after 5 consecutive failures, check log for details")
                sys.exit(1)

    def do_jobs(self):
        '''do all the jobs in order'''
        os.makedirs(os.path.dirname(self.args['output_path']), exist_ok=True)
        if 'generate' in self.args['jobs']:
            self.do_job('generating', self.generate_image, login=[], wait=False)
        if 'upload' in self.args['jobs']:
            self.do_job('uploads', self.upload_image, login=['commons'])
        if 'caption' in self.args['jobs']:
            self.do_job('captions', self.add_caption, login=['commons'])
        if 'additem' in self.args['jobs']:
            self.do_job('adding items', self.add_item, login=['wikidata'])
        if 'depicts' in self.args['jobs']:
            self.do_job('depicts', self.add_depicts, login=['commons'])


def do_main():
    '''
    entry point
    '''
    argparser = ArgParser()
    argparser.parse_args()
    minfo = MediaInfoJob(argparser.args)
    minfo.do_jobs()


if __name__ == '__main__':
    do_main()
