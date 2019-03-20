#!/usr/bin/python3
import json
import sys
import time
import requests


def get_last_revid_url(domain):
    '''
    return url and params that will let us get recentchanges info
    via the mediawiki api for the specified domain
    '''
    #https://en.wikipedia.org/w/api.php?action=query&list=recentchanges&format=json
    base = '/w/api.php'
    url = 'https://' + domain + base
    params = {'action': 'query', 'list': 'recentchanges',
              'rclimit': '1', 'rctype': 'new|edit', 'format': 'json'}
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


def get_revid_from_json(content):
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


def get_last_rev_id(domain):
    '''
    get the max revid for a project given its domain,
    by asking for info in the last entry from recentchanges
    and digging the revid out of that
    if we can't get a good response, return None
    '''
    sess = get_session()
    url, params = get_last_revid_url(domain)
    response = sess.get(url, params=params, timeout=5)
    if response.status_code != 200:
        sys.stderr.write("failed to get revid for %s\n" % url)
        return None
    return get_revid_from_json(response.content)


def do_alarm():
    '''
    print an alarm which should get my attention
    '''
    for _i in range(1, 10):
        time.sleep(1)
        print("")


def get_new_towait(diff):
    '''
    as the diff between revids gets smaller,
    start checking more often
    '''
    # default, wait an hour between queries
    towait = 1800
    if diff is None:
        return towait

    if abs(diff) < 1000:
        towait = 1
    elif abs(diff) < 10000:
        towait = 10
    elif abs(diff) < 100000:
        towait = 500
    return towait


def get_maxrevid_with_retries(domain, max_retries):
    '''
    try a few times to get the max revid, if we fail then
    return None
    '''
    retries_done = 0
    while retries_done < max_retries:
        max_revid = get_last_rev_id(domain)
        if not max_revid:
            time.sleep(5*60)  # probably an error on the back end, give servers a break
            retries_done += 1
        else:
            return max_revid


def compare_revids(towait):
    '''
    get max revid for both enwiki and wikidatawiki, display them
    and the difference between them
    if wikidatawiki maxrevid is bigger (at last!) than enwiki,
    print an alarm that should get my attention
    '''
    enwiki_revid = get_maxrevid_with_retries('en.wikipedia.org', 3)
    wikidata_revid = get_maxrevid_with_retries('www.wikidata.org', 3)
    if not enwiki_revid or not wikidata_revid:
        return False, get_new_towait(None)

    print("Last enwiki revid is", enwiki_revid, "and last wikidata revid is", wikidata_revid)
    diff = int(wikidata_revid) - int(enwiki_revid)
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print("{time}: diff is {diff}".format(time=now, diff=diff))
    if diff > 0:
        towait = get_new_towait(diff)
        do_alarm()
        return True, towait
    print()
    return False, towait


def do_main():
    '''
    entry point
    '''
    # get the default time to wait between attempts
    towait = get_new_towait(None)

    done = False
    while not done:
        done, towait = compare_revids(towait)
        time.sleep(towait)


if __name__ == '__main__':
    do_main()
