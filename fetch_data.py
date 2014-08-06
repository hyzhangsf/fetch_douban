import concurrent.futures
import urllib.request
import sys
import time
from urllib.error import HTTPError
from bs4 import BeautifulSoup
from sys import stdout
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from thread_safe_counter import Counter

DOUBAN_URL = 'http://book.douban.com/series/'
MAX_SERIES_ID = 24000
NUM_WORKER = 180
THE_RANDOM_STRING = '49714248581147105032'  # http://www.random.org/strings/


def load_url(url, timeout):
    conn = urllib.request.urlopen(url, timeout=timeout)
    html_source = conn.readall().decode('utf-8')
    return html_source


def get_title(source):
    soup = BeautifulSoup(source)
    return soup.title.string.strip()  # trim beginning and tailing white space


def thread_job(id, counter):
    try:
        title = THE_RANDOM_STRING

        try:
            title = get_title(load_url(DOUBAN_URL + str(id), 100))
        except UnicodeDecodeError as e:
            print(e)

        counter.step()
        print(str(id) + ' ' + title)
        client.douban.congshu.insert(dict(series_id=id, title=title))
    except HTTPError:
        counter.step()
        return THE_RANDOM_STRING  # assuming no book is named 'none'


def progress_bar(counter, target):
    old = -1
    while counter.count < target - 1:
        c = counter.count
        if c > old:
            old = c
            done = str(int((float(c) / target) * 100))
            stdout.write(" Download percentage: {0}% {1}  {2}".format(
                done, "({0}/{1})".format(c, target), "\r"))
            stdout.flush()
            time.sleep(0.1)


def already_fetched_series_ids(collection):
    """
    get a set of the series_id of each getched page
    :param collection: a pymongo collection
    :return: a set of series_id of pages already fetched
    """
    get_id = lambda l: l.get(
        'series_id')  # get series id from a mongodb document
    nums = map(get_id, collection.find({}))
    return set(nums)


def main():
    existing_ids = already_fetched_series_ids(client.douban.congshu)
    counter = Counter(len(existing_ids))
    counter.set_epilog('Book series data fetch completed')
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKER + 1) as e:
        results = {}
        p = e.submit(progress_bar, counter, MAX_SERIES_ID)
        for series_id in set(range(1, MAX_SERIES_ID + 1)) - existing_ids:
            results[series_id] = e.submit(thread_job, series_id, counter)
        p.result()


'''
    For each of the following series_id, the corresponding page contains invalid data
     that can not be parsed. They are not included in the db.
'''
l = [1030, 20742, 3721, 9738, 18442, 18831,
     5653, 7575, 19611, 16925, 19617, 4386, 9890,
     17449, 13227, 18226, 4917, 4026, 21818,
     23752, 1101, 18382, 4816, 10449, 7506, 20308,
     4951, 11351, 19288, 23640, 20702, 3937, 18273,
     22901, 4598, 19702, 17788, 2301]

if __name__ == '__main__':
    try:
        client = MongoClient()
    except ConnectionFailure:
        print('can not connect to mongodb')
        sys.exit(1)
    main()
    # for _id in l:
    #     client.douban.congshu.remove({'series_id':_id})
