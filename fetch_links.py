#! /usr/bin/env python
import time
from time import mktime
from datetime import datetime, timedelta, date
import argparse
from pprint import pprint
import json
import csv
import os, pickle
from psaw import PushshiftAPI
import signal
import platform

pushshift_rate_limit_per_minute = 20
max_comments_per_query = 150
write_every = 10

link_fields = ['author', 'created_utc', 'domain', 'id', 'is_self',
    'num_comments', 'over_18', 'permalink', 'retrieved_on', 'score',
    'selftext', 'stickied', 'subreddit_id', 'title', 'url']
comment_fields = ['author', 'body', 'created_utc', 'id', 'link_id',
    'parent_id', 'score', 'stickied', 'subreddit_id']

subs_settings = {}
subs = []

def handler(signum, frame):
    print('Interrupt was triggered, exiting...')
    store_archive(args.archive)

def store_archive(fn):
    if isinstance(args.archive, str) and len(subs_settings) != 0:
        with open(fn, 'w') as f:
            f.write(json.dumps(subs_settings))
        os._exit(0)

def fetch_links(subreddit=None, date_start=None, date_stop=None, limit=None, score=None, self_only=False):
    if subreddit is None or date_start is None or date_stop is None:
        print('ERROR: missing required arguments')
        exit()

    api = PushshiftAPI(rate_limit_per_minute=pushshift_rate_limit_per_minute, detect_local_tz=False)

    # get links
    links = []
    print('fetching submissions %s to %s...' % (time.strftime('%Y-%m-%d', date_start), time.strftime('%Y-%m-%d', date_stop)))
    params = {
        'after': int(mktime(date_start)) - 86400, # make date inclusive, adjust for UTC
        'before': int(mktime(date_stop)) + 86400,
        'subreddit': subreddit,
        'filter': link_fields,
        'sort': 'asc',
        'sort_type': 'created_utc',
    }
    if limit:
        params['limit'] = int(limit)
    if score:
        params['score'] = score
    if self_only:
        params['is_self'] = True
    link_results = list(api.search_submissions(**params))
    print('processing %s links' % len(link_results))
    for s in link_results:
        # print('%s %s' % (datetime.utcfromtimestamp(int(s.d_['created_utc'])), s.d_['title']))
        # pprint(s)

        # get comment ids
        comments = []
        if s.d_['num_comments'] > 0 and not comment_data_exists(subreddit, s.d_['created_utc'], s.d_['id']):
            comment_ids = list(api._get_submission_comment_ids(s.d_['id']))
            # print('%s comment_ids: %s' % (data['id'], comment_ids))

            # get comments
            if (len(comment_ids) > 0):
                mychunks = []
                if len(comment_ids) > max_comments_per_query:
                    mychunks = chunks(comment_ids, max_comments_per_query)
                else:
                    mychunks = [comment_ids]
                for chunk in mychunks:
                    comment_params = {
                        'filter': comment_fields,
                        'ids': ','.join(chunk),
                        'limit': max_comments_per_query,
                    }
                    comments_results = list(api.search_comments(**comment_params))
                    print('%s fetch link %s comments %s/%s' % (datetime.utcfromtimestamp(int(s.d_['created_utc'])), s.d_['id'], len(comments_results), len(comment_ids)))
                    for c in comments_results:
                        comments.append(c.d_)

        s.d_['comments'] = comments
        links.append(s.d_)

        # write results
        if len(links) >= write_every:
            success = write_links(subreddit, links)
            if success:
                links = []

    # write remining results
    if len(links):
        write_links(subreddit, links)

# csvs are not guaranteed to be sorted by date but you can resume broken runs
# and change sort criteria later to add more posts without getting duplicates.
# delete csvs and re-run to update existing posts
def write_links(subreddit, links):
    if links and len(links) > 0:
        writing_day = None
        file = None
        writer = None
        existing_link_ids = []
        wrote_links = 0
        wrote_comments = 0

        for r in links:
            # print('%s link %s' % (r['id'], r['title']))

            # grab link comments
            existing_comment_ids = []
            comments = r['comments']
            # print('%s comments %s' % (r['id'], comments))

            created_ts = int(r['created_utc'])
            subs_settings[subreddit] = created_ts
            created = datetime.utcfromtimestamp(created_ts).strftime('%Y-%m-%d')
            created_path = datetime.utcfromtimestamp(created_ts).strftime('%Y/%m/%d')
            if created != writing_day:
                if file:
                    file.close()
                writing_day = created
                path = 'data/' + subreddit + '/' + created_path
                os.makedirs(path, exist_ok=True)

                # create and parse existing links
                filename = 'links.csv'
                filepath = path + '/' + filename
                if not os.path.isfile(filepath):
                    file = open(filepath, 'a', encoding='utf-8')
                    writer = csv.DictWriter(file, fieldnames=link_fields)
                    writer.writeheader()
                    # print('created %s' % filepath)
                else:
                    with open(filepath, 'r', encoding='utf-8') as file:
                        reader = csv.DictReader(file)
                        for row in reader:
                            existing_link_ids.append(row['id'])

                    file = open(filepath, 'a', encoding='utf-8')
                    writer = csv.DictWriter(file, fieldnames=link_fields)

            # create and parse existing comments
            # writing empty comments csvs resuming and comment_data_exists()
            filename = r['id'] + '.csv'
            filepath = path + '/' + filename
            if not os.path.isfile(filepath):
                comments_file = open(filepath, 'a', encoding='utf-8')
                comments_writer = csv.DictWriter(comments_file, fieldnames=comment_fields)
                comments_writer.writeheader()
                # print('created %s' % filepath)
            else:
                with open(filepath, 'r', encoding='utf-8') as comments_file:
                    reader = csv.DictReader(comments_file)
                    for row in reader:
                        existing_comment_ids.append(row['id'])

                comments_file = open(filepath, 'a', encoding='utf-8')
                comments_writer = csv.DictWriter(comments_file, fieldnames=comment_fields)

            # write link row
            if r['id'] not in existing_link_ids:
                for field in list(r):
                    if field not in link_fields:
                        del r[field]

                writer.writerow(r)
                wrote_links += 1

            # write comments
            for c in comments:
                if c['id'] not in existing_comment_ids:
                    for field in list(c):
                        if field not in comment_fields:
                            del c[field]
                    comments_writer.writerow(c)
                    wrote_comments += 1
            comments_file.close()


        print('got %s links, wrote %s and %s comments' % (len(links), wrote_links, wrote_comments))
    return True

def link_data_exists(subreddit, date):
    created_path = time.strftime('%Y/%m/%d', date)
    path = 'data/' + subreddit + '/' + created_path + '/links.csv'
    if not os.path.isfile(path):
        return False
    return True

def comment_data_exists(subreddit, link_created_utc, link_id):
    created_ts = int(link_created_utc)
    created_path = datetime.utcfromtimestamp(created_ts).strftime('%Y/%m/%d')
    path = 'data/' + subreddit + '/' + created_path + '/' + link_id + '.csv'
    if os.path.isfile(path):
        return True
    return False

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def mkdate(datestr):
  try:
    return time.strptime(datestr, '%Y-%m-%d')
  except ValueError:
    raise argparse.ArgumentTypeError(datestr + ' is not a proper date string')

if __name__ == '__main__':
    platos = platform.system()
    if platos == "Linux" or platos == "Darwin":
        signal.signal(signal.SIGSTP, handler)
        signal.signal(signal.SIGSTOP, handler)
    elif platos == "Windows":
        signal.signal(signal.SIGINT, handler)
    parser=argparse.ArgumentParser()
    parser.add_argument('subreddit', help='subreddit to archive')
    parser.add_argument('date_start', type=mkdate, help='start archiving at date, e.g. 2005-1-1')
    parser.add_argument('date_stop', type=mkdate, help='stop archiving at date, inclusive, cannot be date_start')
    parser.add_argument('--limit', default=None, help='pushshift api limit param, default None')
    parser.add_argument('--score', default=None, help='pushshift api score param, e.g. "> 10", default None')
    parser.add_argument('--self_only', action="store_true", help='only fetch selftext submissions, default False')
    parser.add_argument('--slist', default=None, type=str, help='Input a text file containing a list of subreddits')
    parser.add_argument('--archive', default=None, type=str, help='Keeps track of the subreddits on where you left off when you exited RHA')
    args=parser.parse_args()

    self_only = False
    if args.self_only:
        self_only = True

    args.subreddit = args.subreddit.lower()

    # If either arguments are present, prepare them
    if isinstance(args.slist, str):
        with open(args.slist, 'r') as M:
            l = M.readlines()
            for line in l:
                line = line.strip("\n") # Strip any newline characters if they are there
                i = line.split(' ')
                if len(i) < 2: 
                    print('{} has no starting date, skipping.'.format(i[0]))
                    continue
                else:
                    subs.append(i)
        
    elif isinstance(args.archive, str):
        # Load file
        if os.path.isfile(args.archive):
            with open(args.archive, 'r') as b:
                subs_settings = json.load(b)
                print(subs_settings)
                b.close()
        else:
            sub_settings[args.subreddit] = None


                
    if isinstance(args.slist, str):
        for e in subs:
            name = e[0]
            sd = None
            ed = None

            if name in subs_settings and isinstance(args.archive, str):
                n = subs_settings[name]
                sd = datetime.fromtimestamp(n).strftime('%Y-%m-%d')
            else:
                sd = e[1]
                
            if len(e) > 2:
                ed = datetime.fromtimestamp(e[2]).strftime('%Y-%m-%d')
            else:
                ed = datetime.today().strftime('%Y-%m-%d')
            fetch_links(name, mkdate(sd), mkdate(ed), args.limit, args.score, args.self_only)

    elif not isinstance(args.slist, str) and isinstance(args.archive, str):
        sd = mkdate(subs_settings[args.subreddit])
        if sd is None:
            sd = args.date_start
        fetch_links(args.subreddit, sd, args.date_stop, args.limit, args.score, args.self_only)

    else:
        fetch_links(args.subreddit, args.date_start, args.date_stop, args.limit, args.score, self_only)

    store_archive(args.archive)
