#!/usr/bin/python3
# encoding: utf-8
'''
Get mediainfo entry for an uploaded image

This is intended to be run against a wiki that has the MediaInfo extension
enabled, such as commons.wikimedia.org.

Currently MediaInfo items are wikibase entities of type 'item' that are
stored in the 'mediainfo' slot of a revision rather than in wikibase
tables.

MediaInfo items have identifiers of the form Mxxx rather than Qxxx or Pxxx.
This script relies on the fact that currently the Mxxx id is derived from
the page id of the uploaded image. THIS MAY CHANGE and will be broken in
some cases, see
https://phabricator.wikimedia.org/T232087
'''
import getopt
import sys
import getpass
import requests


def validate_args(args):
    '''
    check that args are valid, not missing, etc.
    and do any necessary arg conversions too
    '''
    if 'title' not in args or args['title'] is None:
        usage("mandatory '--title' argument is missing")
    if 'wiki' not in args or args['wiki'] is None:
        usage("mandatory '--wiki' argument is missing")
    if 'agent' not in args or args['agent'] is None:
        usage("mandatory '--agent' argument is missing")
    if 'user' not in args or args['user'] is None:
        usage("mandatory '--user' argument is missing")
    if 'caption' not in args or args['caption'] is None:
        usage("mandatory '--caption' argument is missing")


def get_arg(opt, val, args):
    '''set one arg from opt/val'''
    if opt in ["-t", "--title"]:
        args['title'] = val
    elif opt in ["-w", "--wiki"]:
        args['wiki'] = val
    elif opt in ["-a", "--agent"]:
        args['agent'] = val
    elif opt in ["-u", "--user"]:
        args['user'] = val
    elif opt in ["-c", "--caption"]:
        args['caption'] = val
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


def parse_args():
    '''get args passed on the command line
    and return as a dict'''
    args = {'title': None,
            'wiki': None,
            'agent': None,
            'user': None,
            'caption': None,
            'verbose': False,
            'help': False}

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "a:l:t:u:w:c:vh", [
                "agent=", "title=", "wiki=", "user=", "caption=",
                "verbose", "help"])

    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if not get_arg(opt, val, args) and not get_flag(opt, args):
            usage("Unknown option specified: <%s>" % opt)

    if remainder:
        usage("Unknown option(s) specified: {opt}".format(opt=remainder[0]))

    validate_args(args)
    return args


def usage(message=None):
    '''display usage info about this script'''
    if message is not None:
        print(message)
    usage_message = """Usage: get_mediainfo.py --title <title> --wiki <hostname>
  --agent <user agent> [--verbose]| --help

Arguments:

  --title   (-t):   title of image
                    default: None
  --wiki    (-w):   hostname of the wiki, for viewing web pages and so on
                    default: None
  --agent   (-a):   user agent string for web requests
                    default: None
  --user    (-a):   user name for wiki edits
                    default: None
  --caption (-c):   caption to add to image (will overwrite any existing caption,
                    and will be in English)
                    default: None
  --verbose (-v):   display messages about files as they are created
                    default: false
  --help    (-h):   display this usage message

Example uses:
   python3 get_mediainfo.py -t 'Straßenbahn Haltestelle Freizeit- und Erholungszentrum-3.jpg'
                            -w commons.wikimedia.org -a 'get_mediainfo.py/0.1 <your email addy here>'
                            -u TestUser -c "some street in daylight FIXME"
   python3 get_mediainfo.py -t 'File:Marionina_welchi_(YPM_IZ_072302).jpeg' -v
                            -w commons.wikimedia.org -a 'get_mediainfo.py/0.1 <your email addy here>'
                            -u TestUser -c "SOme plant I think FIXME"
"""
    print(usage_message)
    sys.exit(1)


def get_mediainfo_id(args):
    '''given info for an image, find the mediainfo id for the image
    on the specified wiki'''
    if args['title'].startswith('File:'):
        title = args['title']
    else:
        title = 'File:' + args['title']
    title = title.replace(' ', '_')
    params = {'action': 'query',
              'prop': 'info',
              'titles': title,
              'format': 'json'}
    url = 'https://' + args['wiki'] + '/w/api.php'
    response = requests.post(url, data=params,
                             headers={'User-Agent': args['agent']})
    if args['verbose']:
        print("response:", response.text)
    success = False
    if response.status_code == 200:
        results = response.json()
        if 'query' in results:
            if list(results['query']['pages'].keys()):
                success = True
    if not success:
        sys.stderr.write("Error! " + response.text + "\n")
        sys.exit(1)
    if args['verbose']:
        print("mediainfo id for page:", 'M' + list(response.json()['query']['pages'].keys())[0])
    return 'M' + list(response.json()['query']['pages'].keys())[0]


def display_mediainfo_info(mediainfo_id, args):
    '''given a mediainfo item id, get whatever info exists about it and display that'''
    params = {'action': 'wbgetentities',
              'ids': mediainfo_id,
              'format': 'json'}
    url = 'https://' + args['wiki'] + '/w/api.php'
    response = requests.post(url, data=params,
                             headers={'User-Agent': args['agent']})
    if args['verbose']:
        print("response:", response.text)
    success = False
    if response.status_code == 200:
        results = response.json()
        if 'entities' in results:
            if mediainfo_id in results['entities']:
                success = True
    if not success:
        sys.stderr.write("Error! " + response.text + "\n")
        sys.exit(1)
    print("response for id", mediainfo_id + ":")
    print(results['entities'][mediainfo_id])


def wiki_login(args):
    '''
    log into the wiki given by the wiki api url in the config file,
    get a crsf token, return it
    '''
    # fixme if we get an error reply we should retry a few times (but
    # only if the error is not 'bad password'

    api_url = 'https://' + args['wiki'] + '/w/api.php'
    password = getpass.getpass('Wiki user password: ')
    headers = {'User-Agent': args['agent']}
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
              'lgname': args['user'], 'lgpassword': password, 'lgtoken': login_token}
    result = requests.post(api_url, data=params, cookies=cookies, headers=headers)
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


def set_mediainfo(mid, args):
    '''set the caption for the mediainfo item
    as a test. it's easiest so we do that one.'''
    cookies, token = wiki_login(args)

    api_url = 'https://' + args['wiki'] + '/w/api.php'
    caption = args['caption']
    comment = 'add caption'

    params = {'action': 'wbeditentity',
              'format': 'json',
              'id': mid,
              'data': '{"labels":{"en":{"language":"en","value":"' + caption + '"}}}',
              'summary': comment,
              'token': token}
    response = requests.post(api_url, data=params, cookies=cookies,
                             headers={'User-Agent': args['agent']})

    success = False
    if response.status_code == 200:
        results = response.json()
        if 'success' in results:
            if results['success'] == 1:
                success = True
    if not success:
        sys.stderr.write("Error! " + response.text + "\n")
    return success


def do_main():
    '''
    entry point
    '''
    args = parse_args()
    print("Getting MediaInfo ID")
    mid = get_mediainfo_id(args)
    print("Displaying MediaInfo")
    display_mediainfo_info(mid, args)
    print("Setting caption")
    set_mediainfo(mid, args)
    print("AFTER: MediaInfo now")
    display_mediainfo_info(mid, args)


if __name__ == '__main__':
    do_main()
