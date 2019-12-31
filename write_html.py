#! /usr/bin/env python
from datetime import datetime, date, timedelta
import argparse
import csv
import os
import re
import snudown
import psutil
import configparser, json, requests
import pdb, sys, traceback

url_project = 'https://github.com/libertysoft3/reddit-html-archiver'
links_per_page = 30
pager_skip = 10
pager_skip_long = 100
start_date = date(2005, 1, 1)
end_date = datetime.today().date() + timedelta(days=1)
source_data_links = 'links.csv'
max_comment_depth = 8 # mostly for mobile, which might be silly
removed_content_identifiers = ['[deleted]','deleted','[removed]','removed']
default_sort = 'score'
sort_indexes = {
    'score': {
        'default': 1,
        'slug': 'score'
    },
    'num_comments': {
        'default': 0,
        'slug': 'comments',
    },
    'created_utc': {
        'default': 1000198000,
        'slug': 'date',
    }
}
missing_comment_score_label = 'n/a'


template_index = ''
with open('templates/index.html', 'r', encoding='utf-8') as file:
    template_index = file.read()

template_subreddit = ''
with open('templates/subreddit.html', 'r', encoding='utf-8') as file:
    template_subreddit = file.read()

template_link = ''
with open('templates/link.html', 'r', encoding='utf-8') as file:
    template_link = file.read()

template_comment = ''
with open('templates/partial_comment.html', 'r', encoding='utf-8') as file:
    template_comment = file.read()

template_search = ''
with open('templates/search.html', 'r', encoding='utf-8') as file:
    template_search = file.read()

template_user = ''
with open('templates/user.html', 'r', encoding='utf-8') as file:
    template_user = file.read()

template_sub_link = ''
with open('templates/partial_menu_item.html', 'r', encoding='utf-8') as file:
    template_sub_link = file.read()

template_user_url = ''
with open('templates/partial_user.html', 'r', encoding='utf-8') as file:
    template_user_url = file.read()

template_link_url = ''
with open('templates/partial_link.html', 'r', encoding='utf-8') as file:
    template_link_url = file.read()

template_search_link = ''
with open('templates/partial_search_link.html', 'r', encoding='utf-8') as file:
    template_search_link = file.read()

template_index_sub = ''
with open('templates/partial_index_subreddit.html', 'r', encoding='utf-8') as file:
    template_index_sub = file.read()

template_index_pager_link = ''
with open('templates/partial_subreddit_pager_link.html', 'r', encoding='utf-8') as file:
    template_index_pager_link = file.read()

template_selftext = ''
with open('templates/partial_link_selftext.html', 'r', encoding='utf-8') as file:
    template_selftext = file.read()

template_user_page_link = ''
with open('templates/partial_user_link.html', 'r', encoding='utf-8') as file:
    template_user_page_link = file.read()

teplate_url = ''
with open('templates/partial_url.html', 'r', encoding='utf-8') as file:
    template_url = file.read()

process = psutil.Process(os.getpid())
isubs = []

def retrieve_media(URL):
    try:
        http = requests.get(URL)
    except requests.exceptions.RequestException as e:
        print("Error failed to retrieve %s" % URL)
        print(e)
        return None
    
    if http.status_code != 200: return None

    if 'Content-Type' not in http.headers:
        print("Error, \'Content-Type\' is not present in headers in URL of %s." % URL)
        return None
    
    headers = http.headers['Content-Type']
    
    content_type = headers.split("/")[0]
    if content_type != "image":
        return None
    
    extension = headers.split("/")[1]
    return (extension, http.content)

def get_imgur_credentials():
    if os.path.isfile("credentials.ini"):
        cfg = configparser.ConfigParser()
        cfg.read("credentials.ini")
        return str(cfg["MAIN"]['imgur_client_id'])
    else:
        return None

# Returns a pair of bools that determine whether or not the imgur link is an individual image or an album
def is_imgur(URL):
    return (("https://imgur.com/" in URL),("https://imgur.com/a/" in URL))

# Using the client id given in credentials.ini, retrieve the URL from the json of the imgur image provided
def get_imgur_image_link(iURL):
    cid = get_imgur_credentials()
    if cid is None:
        print("Warning: failed to retrieve imgur image link from %s" % iURL)
        print("Reason: No client id found")
        return None
    URL = "https://api.imgur.com/3/image/" + iURL.split('/')[-1]
    
    header = {"Authorization": str("CLIENT-ID " + cid)}
    r = requests.get(URL, headers=header)
    if r.status_code != 200:
        print("Error, %s on retrieving %s" % (r.status_code, URL))
        return None
    
    j = json.loads(r.text)['data']
    if 'error' in j:
        print(j['error'])
        return None
    return json.loads(r.text)['data']['link']
    

def generate_html(min_score=0, min_comments=0, hide_deleted_comments=False):
    delta = timedelta(days=1)
    subs = get_subs()
    user_index = {}
    processed_subs = []
    stat_links = 0
    stat_filtered_links = 0

    for sub in subs:
        # write link pages
        # print('generate_html() processing %s %s kb' % (sub, int(int(process.memory_info().rss) / 1024)))
        stat_sub_links = 0
        stat_sub_filtered_links = 0
        stat_sub_comments = 0
        d = start_date
        while d <= end_date:
            raw_links = load_links(d, sub, True)
            stat_links += len(raw_links)
            stat_sub_links += len(raw_links)
            for l in raw_links:
                if validate_link(l, min_score, min_comments):
                    write_link_page(subs, l, sub, hide_deleted_comments)
                    stat_filtered_links += 1
                    stat_sub_filtered_links += 1
                    if 'comments' in l:
                        stat_sub_comments += len(l['comments'])
            d += delta
        if stat_sub_filtered_links > 0:
            processed_subs.append({'name': sub, 'num_links': stat_sub_filtered_links})
        print('%s: %s links filtered to %s' % (sub, stat_sub_links, stat_sub_filtered_links))

        # write subreddit pages
        valid_sub_links = []
        d = start_date
        while d <= end_date:
            raw_links = load_links(d, sub)
            for l in raw_links:
                if validate_link(l, min_score, min_comments):
                    valid_sub_links.append(l)

                    # collect links for user pages
                    # TODO: this is the least performant bit. load and generate user pages user by user instead.
                    l['subreddit'] = sub
                    if l['author'] not in user_index.keys():
                        user_index[l['author']] = []
                    user_index[l['author']].append(l)
            d += delta
        write_subreddit_pages(sub, subs, valid_sub_links, stat_sub_filtered_links, stat_sub_comments)
        write_subreddit_search_page(sub, subs, valid_sub_links, stat_sub_filtered_links, stat_sub_comments)

    # write user pages
    write_user_page(processed_subs, user_index)

    # write index page
    write_index(processed_subs)
    print('all done. %s links filtered to %s' % (stat_links, stat_filtered_links))

def write_subreddit_pages(subreddit, subs, link_index, stat_sub_filtered_links, stat_sub_comments):
    if len(link_index) == 0:
        return True

    for sort in sort_indexes.keys():
        links = sorted(link_index, key=lambda k: (int(k[sort]) if k[sort] != '' else sort_indexes[sort]['default']), reverse=True)
        pages = list(chunks(links, links_per_page))
        page_num = 0

        sort_based_prefix = '../'
        if sort == default_sort:
            sort_based_prefix = ''

        # render subreddits list
        subs_menu_html = ''
        for sub in subs:
            sub_url = sort_based_prefix + '../' + sub + '/index.html'
            subs_menu_html += template_sub_link.replace('###URL_SUB###', sub_url).replace('###SUB###', sub)

        for page in pages:
            page_num += 1
            # print('%s page' % (page))

            links_html = ''
            for l in page:
                author_link_html = template_user_url
                author_url = sort_based_prefix + '../user/' + l['author'] + '.html'
                author_link_html = author_link_html.replace('###URL_AUTHOR###', author_url).replace('###AUTHOR###', l['author'])

                link_url = l['url']
                link_comments_url = sort_based_prefix + l['permalink'].lower().strip('/')
                link_comments_url = link_comments_url.replace('r/' + subreddit + '/', '')
                idpath = '/'.join(list(l['id']))
                link_comments_url = link_comments_url.replace(l['id'], idpath)
                link_comments_url += '.html'
                if l['is_self'] is True or l['is_self'] == 'True':
                    link_url = link_comments_url

                index_link_data_map = {
                    '###TITLE###':              l['title'],
                    '###URL###':                link_url,
                    '###URL_COMMENTS###':       link_comments_url,
                    '###SCORE###':              str(l['score']),
                    '###NUM_COMMENTS###':       l['num_comments'] if int(l['num_comments']) > 0 else str(0),
                    '###DATE###':               datetime.utcfromtimestamp(int(l['created_utc'])).strftime('%Y-%m-%d'),
                    '###LINK_DOMAIN###':        '(self.' + subreddit + ')' if l['is_self'] is True or l['is_self'] == 'True' else '',
                    '###HTML_AUTHOR_URL###':    author_link_html,
                }
                link_html = template_link_url
                for key, value in index_link_data_map.items():
                    link_html = link_html.replace(key, value)
                links_html += link_html + '\n'

            index_page_data_map = {
                '###INCLUDE_PATH###':           sort_based_prefix + '../',
                '###TITLE###':                  'by ' + sort_indexes[sort]['slug'] + ' page ' + str(page_num) + ' of ' + str(len(pages)),
                '###SUB###':                    subreddit,
                '###ARCH_NUM_POSTS###':         str(stat_sub_filtered_links),
                '###ARCH_NUM_COMMENTS###':      str(stat_sub_comments),
                '###URL_SUBS###':               sort_based_prefix + '../index.html',
                '###URL_PROJECT###':            url_project,
                '###URL_IDX_SCORE###':          sort_based_prefix + 'index.html',
                '###URL_IDX_CMNT###':           sort_based_prefix + 'index-' + sort_indexes['num_comments']['slug'] + '/index.html',
                '###URL_IDX_DATE###':           sort_based_prefix + 'index-' + sort_indexes['created_utc']['slug'] + '/index.html',
                '###URL_SEARCH###':             sort_based_prefix + 'search.html',
                '###URL_IDX_SCORE_CSS###':      'active' if sort == 'score' else '',
                '###URL_IDX_CMNT_CSS###':       'active' if sort == 'num_comments' else '',
                '###URL_IDX_DATE_CSS###':       'active' if sort == 'created_utc' else '',
                '###URL_SEARCH_CSS###':         '',
                '###HTML_LINKS###':             links_html,
                '###HTML_SUBS_MENU###':         subs_menu_html,
                '###HTML_PAGER###':             get_pager_html(page_num, len(pages)),
            }
            page_html = template_subreddit
            for key, value in index_page_data_map.items():
                page_html = page_html.replace(key, value)

            
            # write file
            suffix = '-' + str(page_num) + '.html'
            if page_num == 1:
                suffix = '.html'
            filename = 'index' + suffix
            if sort == default_sort:
                filepath = 'r/' + subreddit + '/' + filename
            else:
                filepath = 'r/' + subreddit + '/index-' + sort_indexes[sort]['slug'] + '/' + filename
            if not os.path.isfile(filepath):
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                with open(filepath, 'w', encoding='utf-8') as file:
                    file.write(page_html)
                    # print('wrote %s %s, %s links' % (sort, filepath, len(page)))

    return True

def write_link_page(subreddits, link, subreddit='', hide_deleted_comments=False):
    # reddit:  https://www.reddit.com/r/conspiracy/comments/8742iv/happening_now_classmate_former_friend_of/
    # archive: r/conspiracy/comments/8/7/4/2/i/v/happening_now_classmate_former_friend_of.html
    idpath = '/'.join(list(link['id']))
    filepath = link['permalink'].lower().strip('/') + '.html'
    filepath = filepath.replace(link['id'], idpath)
    if os.path.isfile(filepath):
        return True

    created = datetime.utcfromtimestamp(int(link['created_utc']))
    sorted_comments = []
    if len(link['comments']) > 0:
        sorted_comments = sort_comments(link['comments'], hide_deleted_comments)

    # traverse up to root dir, depends on id length
    static_include_path = ''
    for i in range(len(link['id']) + 2):
        static_include_path += '../'

    image = None
    if not args.noimages:
        i = is_imgur(link['url'])
        # if we have an imgur client id and the url in the loop is an imgur link then get the URL
        if i[0]:
            # Extract url from json and download the image itself
            imu = get_imgur_image_link(link['url']) 
            if imu is not None:
                image = retrieve_media(imu)
        elif i[1]:
            # TODO: Implement Imgur albums support 
            pass
        else:
            image = retrieve_media(link['url'])
        # Finally, if the image is downloaded then generate a path and attach it to the url entry in the link dict
        # so when it's used as an href link it will point to the path instead of the url itself
        # URL + /images/ + ID + . Image Extension
        if image is not None: link['url'] = subreddit + "/images/" + link['id'] + "." + image[0]
        
    # render comments
    comments_html = ''
    for c in sorted_comments:
        css_classes = 'ml-' + (str(c['depth']) if int(c['depth']) <= max_comment_depth else str(max_comment_depth))
        if c['author'] == link['author'] and c['author'] not in removed_content_identifiers:
            css_classes += ' op'
        if c['stickied'].lower() == 'true' or c['stickied'] is True:
            css_classes += ' stickied'

        # author link
        url = static_include_path + 'user/' + c['author'] + '.html'
        author_link_html = template_user_url.replace('###URL_AUTHOR###', url).replace('###AUTHOR###', c['author'])

        comment_data_map = {
            '###ID###':                 c['id'],
            '###PARENT_ID###':          c['parent_id'],
            '###DEPTH###':              str(c['depth']),
            '###DATE###':               created.strftime('%Y-%m-%d'),
            '###SCORE###':              str(c['score']) if len(str(c['score'])) > 0 else missing_comment_score_label,
            '###BODY###':               snudown.markdown(c['body'].replace('&gt;','>')),
            '###CSS_CLASSES###':        css_classes,
            '###CLASS_SCORE###':        'badge-danger' if len(c['score']) > 0 and int(c['score']) < 1 else 'badge-secondary',
            '###HTML_AUTHOR_URL###':    author_link_html,
        }
        comment_html = template_comment
        for key, value in comment_data_map.items():
            comment_html = comment_html.replace(key, value)
        comments_html += comment_html + '\n'

    # render subreddits list
    subs_menu_html = ''
    for sub in subreddits:
        sub_url = static_include_path + sub + '/index.html'
        subs_menu_html += template_sub_link.replace('###URL_SUB###', sub_url).replace('###SUB###', sub)

    # render selftext
    selftext_html = ''
    if len(link['selftext']) > 0:
        selftext_html = template_selftext.replace('###SELFTEXT###', snudown.markdown(link['selftext'].replace('&gt;','>')))

    # author link
    url = static_include_path + 'user/' + link['author'] + '.html'
    author_link_html = template_user_url.replace('###URL_AUTHOR###', url).replace('###AUTHOR###', link['author'])

    #html_title = template_url.replace('#HREF#', link['url']).replace('#INNER_HTML#', link['title'])
    if image is None:
        html_title = template_url.replace('#HREF#', link['url']).replace('#INNER_HTML#', link['title'])
    else:
        html_title = template_url.replace('#HREF#', static_include_path + link['url']).replace('#INNER_HTML#', link['title'])
    if link['is_self'] is True or link['is_self'].lower() == 'true':
        html_title = link['title']

    # render link page
    link_data_map = {
        '###INCLUDE_PATH###':       static_include_path,
        '###SUB###':                subreddit,
        '###TITLE###':              link['title'],
        '###ID###':                 link['id'],
        '###DATE###':               created.strftime('%Y-%m-%d'),
        '###ARCHIVE_DATE###':       datetime.utcfromtimestamp(int(link['retrieved_on'])).strftime('%Y-%m-%d') if link['retrieved_on'] != '' else 'n/a',
        '###SCORE###':              str(link['score']),
        '###NUM_COMMENTS###':       str(link['num_comments']),
        '###URL_PROJECT###':        url_project,
        '###URL_SUBS###':           static_include_path + 'index.html',
        '###URL_SUB###':            static_include_path + subreddit + '/index.html',
        '###URL_SUB_CMNT###':       static_include_path + subreddit + '/index-' + sort_indexes['num_comments']['slug'] + '/index.html',
        '###URL_SUB_DATE###':       static_include_path + subreddit + '/index-' + sort_indexes['created_utc']['slug'] + '/index.html',
        '###URL_SEARCH###':         static_include_path + subreddit + '/search.html',
        '###HTML_SUBS_MENU###':     subs_menu_html,
        '###HTML_SELFTEXT###':      selftext_html,
        '###HTML_COMMENTS###':      comments_html,
        '###HTML_AUTHOR_URL###':    author_link_html,
        '###HTML_TITLE###':         html_title,
    }
    html = template_link
    for key, value in link_data_map.items():
        html = html.replace(key, value)

    # write html
    # reddit:  https://www.reddit.com/r/conspiracy/comments/8742iv/happening_now_classmate_former_friend_of/
    # archive: r/conspiracy/comments/8/7/4/2/i/v/happening_now_classmate_former_friend_of.html
    idpath = '/'.join(list(link['id']))
    filepath = link['permalink'].lower().strip('/') + '.html'
    filepath = filepath.replace(link['id'], idpath)
    if not os.path.isfile(filepath):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write(html)
        # print('wrote %s %s' % (created.strftime('%Y-%m-%d'), filepath))
        
    if image is not None:
        if not os.path.isfile(link['url']):
            os.makedirs("r/" + subreddit + "/images/", exist_ok=True)
            open("r/" + link['url'], 'wb').write(image[1])
            print("Writing media: %s " % link['url'])
        # Add a '../' because we will reuse the file location for the index file
        link['url'] = "../" + link['url']
    return True

def write_subreddit_search_page(subreddit, subs, link_index, stat_sub_filtered_links, stat_sub_comments):
    if len(link_index) == 0:
        return True

    # name sort?
    links = sorted(link_index, key=lambda k: re.sub(r'\W+', '', k['title']).lower())

    # render subreddits list
    subs_menu_html = ''
    for sub in subs:
        sub_url = '../' + sub + '/index.html'
        subs_menu_html += template_sub_link.replace('###URL_SUB###', sub_url).replace('###SUB###', sub)

    links_html = ''
    for l in links:
        link_comments_url = l['permalink'].lower().strip('/').replace('r/' + subreddit + '/', '')
        idpath = '/'.join(list(l['id']))
        link_comments_url = link_comments_url.replace(l['id'], idpath)
        link_comments_url += '.html'
        index_link_data_map = {
            '###TITLE###':              l['title'],
            '###URL###':                link_comments_url,
        }
        link_html = template_search_link
        for key, value in index_link_data_map.items():
            link_html = link_html.replace(key, value)
        links_html += link_html + '\n'

    index_page_data_map = {
        '###INCLUDE_PATH###':           '../',
        '###TITLE###':                  'search',
        '###SUB###':                    subreddit,
        '###ARCH_NUM_POSTS###':         str(stat_sub_filtered_links),
        '###ARCH_NUM_COMMENTS###':      str(stat_sub_comments),
        '###URL_SUBS###':               '../index.html',
        '###URL_PROJECT###':            url_project,
        '###URL_IDX_SCORE###':          'index.html',
        '###URL_IDX_CMNT###':           'index-' + sort_indexes['num_comments']['slug'] + '/index.html',
        '###URL_IDX_DATE###':           'index-' + sort_indexes['created_utc']['slug'] + '/index.html',
        '###URL_SEARCH###':             'search.html',
        '###URL_IDX_SCORE_CSS###':      '',
        '###URL_IDX_CMNT_CSS###':       '',
        '###URL_IDX_DATE_CSS###':       '',
        '###URL_SEARCH_CSS###':         'active',
        '###HTML_LINKS###':             links_html,
        '###HTML_SUBS_MENU###':         subs_menu_html,
    }
    page_html = template_search
    for key, value in index_page_data_map.items():
        page_html = page_html.replace(key, value)

    # write file
    filename = 'search.html'
    filepath = 'r/' + subreddit + '/' + filename
    if not os.path.isfile(filepath):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write(page_html)
            # print('wrote %s, %s links' % (filepath, len(links)))
    return True

def write_user_page(subs, user_index):
    if len(user_index.keys()) == 0:
        return False

    # subreddits list
    subs_menu_html = ''
    for sub in subs:
        sub_url = '../' + sub['name'] + '/index.html'
        subs_menu_html += template_sub_link.replace('###URL_SUB###', sub_url).replace('###SUB###', sub['name'])

    for user in user_index.keys():
        links = user_index[user]
        links.sort(key=lambda k: (int(k['score']) if k['score'] != '' else sort_indexes['score']['default']), reverse=True)

        links_html = ''
        for l in links:

            author_link_html = template_user_url
            author_url = l['author'] + '.html'
            author_link_html = author_link_html.replace('###URL_AUTHOR###', author_url).replace('###AUTHOR###', l['author'])

            link_comments_url = l['permalink'].lower().replace('/r/', '').strip('/')
            link_comments_url = '../' + link_comments_url
            idpath = '/'.join(list(l['id']))
            link_comments_url = link_comments_url.replace(l['id'], idpath)
            link_comments_url += '.html'
            link_url = l['url']
            if l['is_self'] is True or l['is_self'] == 'True':
                link_url = link_comments_url

            link_data_map = {
                '###TITLE###':              l['title'],
                '###URL###':                link_url,
                '###URL_COMMENTS###':       link_comments_url,
                '###SCORE###':              str(l['score']),
                '###NUM_COMMENTS###':       str(l['num_comments']) if int(l['num_comments']) > 0 else str(0),
                '###DATE###':               datetime.utcfromtimestamp(int(l['created_utc'])).strftime('%Y-%m-%d'),
                '###SUB###':                l['subreddit'],
                '###SUB_URL###':            '../' + l['subreddit'] + '/index.html',
                '###HTML_AUTHOR_URL###':    author_link_html,
            }
            link_html = template_user_page_link
            for key, value in link_data_map.items():
                link_html = link_html.replace(key, value)
            links_html += link_html + '\n'

        page_data_map = {
            '###INCLUDE_PATH###':           '../',
            '###TITLE###':                  'user/' + user,
            '###ARCH_NUM_POSTS###':         str(len(links)),
            '###URL_USER###':               user + '.html',
            '###URL_SUBS###':               '../index.html',
            '###URL_PROJECT###':            url_project,
            '###HTML_LINKS###':             links_html,
            '###HTML_SUBS_MENU###':         subs_menu_html,
        }
        page_html = template_user
        for key, value in page_data_map.items():
            page_html = page_html.replace(key, value)

        filepath = 'r/user/' + user + '.html'
        if not os.path.isfile(filepath):
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as file:
                file.write(page_html)
            # print('wrote %s' % (filepath))

    return True

def write_index(subs):
    if len(isubs) == 0:
        return False
    isubs.sort(key=lambda k: k['name'].casefold())
    
    stat_num_links = 0
    links_html = ''
    subs_menu_html = ''
    for sub in isubs:
        sub_url = sub['name'] + '/index.html'
        links_html += template_index_sub.replace('#URL_SUB#', sub_url).replace('#SUB#', sub['name']).replace('#NUM_LINKS#', str(sub['num_links']))
        subs_menu_html += template_sub_link.replace('###URL_SUB###', sub_url).replace('###SUB###', sub['name'])
        stat_num_links += sub['num_links']

    index_page_data_map = {
        '###INCLUDE_PATH###':           '',
        '###TITLE###':                  'subreddits',
        '###URL_SUBS###':               'index.html',
        '###URL_PROJECT###':            url_project,
        '###ARCH_NUM_POSTS###':         str(stat_num_links),
        '###HTML_LINKS###':             links_html,
        '###HTML_SUBS_MENU###':         subs_menu_html,
    }
    page_html = template_index
    for key, value in index_page_data_map.items():
        page_html = page_html.replace(key, value)

    filepath = 'r/index.html'
    if not os.path.isfile(filepath):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write(page_html)
        # print('wrote %s' % (filepath))

    return True

# a 'top' comments sort with orphaned comments (incomplete data) rendered last
def sort_comments(comments, hide_deleted_comments=False):
    sorted_comments = []
    if len(comments) == 0:
        return sorted_comments
    parent_map = {}
    id_map = {}
    top_level_comments = []
    link_id = comments[0]['link_id']
    depth = 0

    try:
        for c in comments:
            c['depth'] = depth
            id_map[c['id']] = c
            parent_map[c['id']] = c['parent_id']
            # add stickied comments
            if c['stickied'].lower() == 'true':
                sorted_comments.append(c)
            # store top level comments      
            elif c['parent_id'] == c['link_id']:
                top_level_comments.append(c)
    except Exception as e:
       
        with open("comment_error.txt", 'w') as k:
            #k.write(str(e))
            #k.write("\n")
            #k.write(json.dumps(c))
            print("An error occured.")
            sys.exit(1)

    # sort non stickied top level comments
    if len(top_level_comments) > 0:
        top_level_comments = sorted(top_level_comments, key=lambda k: (int(k['score']) if k['score'] != '' else 1), reverse=True)
        sorted_comments += top_level_comments

    # add each top level comment's child comments
    sorted_linear_comments = []
    for c in sorted_comments:
        # only remove deleted comments if no children
        if hide_deleted_comments and c['body'] in removed_content_identifiers and 't1_' + c['id'] not in parent_map.values():
            pass
        else:
            sorted_linear_comments.append(c)
            child_comments = get_comment_tree_list([], depth + 1, c, id_map, parent_map, hide_deleted_comments)
            if len(child_comments) > 0:
                sorted_linear_comments += child_comments

    # add orphaned comments
    for c in comments:
        if c['parent_id'] != link_id and c['parent_id'].replace('t1_', '') not in id_map.keys():
            if hide_deleted_comments and c['body'] in removed_content_identifiers:
                continue
            sorted_linear_comments.append(c)

    # print('sort_comments() in %s out %s show deleted: %s' % (len(comments), len(sorted_comments), hide_deleted_comments))
    return sorted_linear_comments

def get_comment_tree_list(tree, depth, parent_comment, id_map, parent_map, hide_deleted_comments):
    parent_id = 't1_' + parent_comment['id']
    child_comments = []
    for key, value in parent_map.items():
        if value == parent_id:
            if hide_deleted_comments and id_map[key]['body'] in removed_content_identifiers and 't1_' + key not in parent_map.values():
                pass
            else:
                child_comments.append(id_map[key])

    # sort children by score
    # TODO: sort by score and # of child comments
    if len(child_comments) > 0:
        child_comments = sorted(child_comments, key=lambda k: (int(k['score']) if k['score'] != '' else 1), reverse=True)
        for child_comment in child_comments:
            child_comment['depth'] = depth
            tree.append(child_comment)
            tree = get_comment_tree_list(tree, depth + 1, child_comment, id_map, parent_map, hide_deleted_comments)
    return tree

def validate_link(link, min_score=0, min_comments=0):
    if not link:
        return False
    elif not 'id' in link.keys():
        return False
    # apply multiple conditions as an OR, keep high score low comments and high comment low score links/posts
    # TODO this should be configurable
    if min_score > 0 and min_comments > 0:
        if int(link['score']) < min_score and int(link['num_comments']) < min_comments:
            return False
    else:
        if min_score > 0 and int(link['score']) < min_score:
            return False
        if min_comments > 0 and int(link['num_comments']) < min_comments:
            return False

    return True

def load_links(date, subreddit, with_comments=False):
    links = []
    if not date or not subreddit:
        return links

    date_path = date.strftime("%Y/%m/%d")
    daily_path = 'data/' + subreddit + '/' + date_path
    daily_links_path = daily_path + '/' + source_data_links
    if os.path.isfile(daily_links_path):
        links = []
        with open(daily_links_path, 'r', encoding='utf-8') as links_file:
            #pdb.set_trace()
            reader = csv.DictReader(links_file)
            for link_row in reader:
                if with_comments:
                    comments = []
                    comments_file_path = daily_path + '/' + link_row['id'] + '.csv'
                    if os.path.isfile(comments_file_path):
                        with open(comments_file_path, 'r', encoding='utf-8') as comments_file:
                            reader = csv.DictReader(comments_file)
                            for comment_row in reader:
                                comments.append(comment_row)
                    link_row['comments'] = comments
                links.append(link_row)
    return links

def get_subs():
    subs = []
    if not os.path.isdir('data'):
        print('ERROR: no data, run fetch_links.py first')
        return subs
    for d in os.listdir('data'):
        if os.path.isdir('data' + '/' + d):
            if d != args.sub and args.sub != "-": 
                # Since we're not adding all subreddits to sub, we need
                # to a list to append to so we can process the subreddits into the index file
                isubs.append(d.lower())
                continue
            subs.append(d.lower())
    return subs

def get_pager_html(page_num=1, pages=1):
    html_pager = ''

    # previous
    css = ''
    if page_num == 1:
        css = 'disabled'
    url = 'index'
    if page_num  - 1 > 1:
        url += '-' + str(page_num - 1)
    url += '.html'
    html_pager += template_index_pager_link.replace('#URL#', url).replace('#TEXT#', '&lsaquo;').replace('#CSS_CLASS#', css)
    
    # skip back
    css = ''
    prev_skip = page_num - pager_skip
    if prev_skip < 1:
        prev_skip = 1
    if page_num == 1:
        css = 'disabled'
    url = 'index'
    if prev_skip > 1:
        url += '-' + str(prev_skip)
    url += '.html'
    html_pager += template_index_pager_link.replace('#URL#', url).replace('#TEXT#', '&lsaquo;&lsaquo;').replace('#CSS_CLASS#', css)
    
    # skip back far
    css = ''
    prev_skip = page_num - pager_skip_long
    if prev_skip < 1:
        prev_skip = 1
    if page_num == 1:
        css += ' disabled'
    url = 'index'
    if prev_skip > 1:
        url += '-' + str(prev_skip)
    url += '.html'
    html_pager += template_index_pager_link.replace('#URL#', url).replace('#TEXT#', '&lsaquo;&lsaquo;&lsaquo;').replace('#CSS_CLASS#', css)

    # n-1
    start = -2
    if page_num + 1 > pages:
        start -= 1
    if page_num + 2 > pages:
        start -= 1
    for prev_page_num in range(start,0):
        if page_num + prev_page_num > 0:
            css = ''
            url = 'index'
            if page_num + prev_page_num > 1:
                url += '-' + str(page_num + prev_page_num)
            url += '.html'
            if prev_page_num < -1:
                css = 'd-none d-sm-block'
            html_pager += template_index_pager_link.replace('#URL#', url).replace('#TEXT#', str(page_num + prev_page_num)).replace('#CSS_CLASS#', css)
    # n
    url = 'index'
    if page_num > 1:
        url += '-' + str(page_num)
    url += '.html'
    html_pager += template_index_pager_link.replace('#URL#', url).replace('#TEXT#', str(page_num)).replace('#CSS_CLASS#', 'active')
    # n + 1
    css = ''
    end = 3
    if page_num -1 < 1:
        end += 1
    if page_num - 2 < 1:
        end += 1
    for next_page_num in range(1,end):
        if page_num + next_page_num <= pages:
            if next_page_num > 1:
                css = 'd-none d-sm-block'
            html_pager += template_index_pager_link.replace('#URL#', 'index' + '-' + str(page_num + next_page_num) + '.html').replace('#TEXT#', str(page_num + next_page_num)).replace('#CSS_CLASS#', css)

    # skip forward far
    next_skip = page_num + pager_skip_long
    css = ''
    if page_num == pages:
        css += ' disabled'
    if next_skip > pages:
        next_skip = pages
    url = 'index'
    if next_skip > 1:
        url += '-' + str(next_skip)
    url += '.html'
    html_pager += template_index_pager_link.replace('#URL#', url).replace('#TEXT#', '&rsaquo;&rsaquo;&rsaquo;').replace('#CSS_CLASS#', css)
    
    # skip forward
    next_skip = page_num + pager_skip
    css = ''
    if page_num == pages:
        css = 'disabled'
    if next_skip > pages:
        next_skip = pages
    url = 'index'
    if next_skip > 1:
        url += '-' + str(next_skip)
    url += '.html'
    html_pager += template_index_pager_link.replace('#URL#', url).replace('#TEXT#', '&rsaquo;&rsaquo;').replace('#CSS_CLASS#', css)

    # next
    css = ''
    next_num = page_num + 1 
    if page_num == pages:
      css = 'disabled'
      next_num = pages
    html_pager += template_index_pager_link.replace('#URL#', 'index' + '-' + str(next_num) + '.html').replace('#TEXT#', '&rsaquo;').replace('#CSS_CLASS#', css)

    return html_pager

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--min-score', default=0, help='limit post rendering, default 0')
    parser.add_argument('--min-comments', default=0, help='limit post rendering, default 0')
    parser.add_argument('--hide-deleted-comments', action='store_true', help='exclude deleted and removed comments where possible')
    parser.add_argument('--noimages', help='Disable retrieving of images', action='store_true')
    parser.add_argument('--sub', default='-', help='Only write a specific subreddit', type=str)
    args=parser.parse_args()

    hide_deleted_comments = False
    if args.hide_deleted_comments:
        hide_deleted_comments = True

    args.min_score = int(args.min_score)
    args.min_comments = int(args.min_comments)

    generate_html(args.min_score, args.min_comments, hide_deleted_comments)
