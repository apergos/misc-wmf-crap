#!/usr/bin/python3
import json
import sys
import time
import requests
import getopt


def get_revinfo_url(domain, revid):
    '''
    return url and params that will let us get recentchanges info
    via the mediawiki api for the specified domain
    '''
    #https://en.wikipedia.org/w/api.php?action=query&list=recentchanges&format=json
    base = '/w/api.php'
    url = 'https://' + domain + base
    params = {'action': 'query', 'prop': 'revisions', 'revids': str(revid),
              'rvprop': 'timestamp', 'format': 'json'}
    return url, params


def get_session():
    '''
    get an open session for making requests
    '''
    sess = requests.Session()
    sess.headers.update(
        {"User-Agent": "wikidata_is_too_big.py/0.0 (atg: when dumps are going to be infeasible ;-)",
         "Accept": "application/json"})
    return sess


def get_revinfo_from_json(content):
    '''
    given json output from mediawiki api for recentchanges
    information, get the revid out of the top entry and
    return it
    if there's no revid in the content or it can't be parsed,
    return None
    '''
    try:
        revinfo = json.loads(content)
        return revinfo['query']['recentchanges'][0]['revid']
    except Exception:
        return None


def get_revinfo(domain, revid):
    '''
    get timestamp of a rev for a project given its domain,
    if we can't get a good response, return None
    '''
    sess = get_session()
    url, params = get_revinfo_url(domain, revid)
    response = sess.get(url, params=params, timeout=5)
    if response.status_code != 200:
        sys.stderr.write("failed to get revid for %s\n" % url)
        return None
    try:
        rev_info = json.loads(response.content)
        pages = list(rev_info['query']['pages'].keys())
        if len(pages) != 1:
            print("strange content for revision", response.content)
            return None
        for pageid in pages:
            timestamp = rev_info['query']['pages'][pageid]['revisions'][0]['timestamp']
            print("revid", revid, "at", timestamp)
    except Exception:
        print("no info available for this revision", revid)
        print(response.content)
        return None
    return True


def get_revinfo_with_retries(domain, revid, max_retries):
    '''
    try a few times to get the max revid, if we fail then
    return None
    '''
    retries_done = 0
    while retries_done < max_retries:
        res = get_revinfo(domain, revid)
        if not res:
            time.sleep(5*60)  # probably an error on the back end, give servers a break
            retries_done += 1
        else:
            return


def usage(message):
    '''
    display a nice usage message along with an optional message
    describing an error
    '''
    if message:
        sys.stderr.write(message + "\n")
    usage_message = """Usage: $0 --domain <hostname> --revids <revid[,revid...]
or: $0 --help

gets and dispays the timestamps of the given revision ids for the specified wiki

Arguments:

 --domain (-d):   hostname of the wiki, for example el.wiktionary.org
                  default: en.wikipedia.org
 --revids (-r):   comma-separated list of revids for which to retrieve
		  timestamp
                  default: None

 --help    (-h):  show this help message
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def parse_args():
    '''
    get command line args, validate and return them
    only two
    '''
    domain = 'en.wikipedia.org'
    revids = []
    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "d:r:h", ["domain=", "revids=", "help"])

    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-d", "--domain"]:
            domain = val
        elif opt in ["-r", "--revids"]:
            revids = val.split(',')
        elif opt in ["-h", "--help"]:
            usage('Help for this script\n')
        else:
            usage("Unknown option specified: <%s>" % opt)

    if remainder:
        usage("Unknown option(s) specified: {opt}".format(opt=remainder[0]))

    return domain, revids


def do_main():
    '''
    entry point
    '''
    domain, revids = parse_args()
    for revid in revids:
        get_revinfo_with_retries(domain, revid, 3)
        # be nice and sleep a little in between retrievals
        time.sleep(5)


if __name__ == '__main__':
    do_main()
