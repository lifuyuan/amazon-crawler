from bs4 import BeautifulSoup
from urllib.parse import urlparse
from datetime import datetime
import settings
import random
import requests
import os
import redis
from requests.exceptions import RequestException
num_requests = 0

redis = redis.StrictRedis(host=settings.redis_host, port=settings.redis_port, db=settings.redis_db)


def make_request(url, return_soup=True):
    # 全局的url request及response处理函数
    url = format_url(url)
    log("make_request")
    log(url)
    if "picassoRedirect" in url:
        return None, None  # 跳过重定向的url

    global num_requests
    if num_requests >= settings.max_requests:
        raise Exception("Reached the max number of requests: {}".format(settings.max_requests))

    headers = settings.headers
    headers["User-Agent"] = random.choice(settings.agents)
    # proxy_dict = {
    #     "https": random.choice(settings.proxies)
    # }
    try:
        r = requests.get(url, headers=headers)
    except RequestException as e:
        log("WARNING: Request for {} failed.".format(url))
        return None, None

    num_requests += 1
    if r.status_code != 200:
        os.system('say "Got non-200 Response"')
        log("WARNING: Got a {} status code for URL: {}".format(r.status_code, url))
        return None, None

    if return_soup:
        return BeautifulSoup(r.text, "lxml"), r.text
    return r


def format_url(url):
    u = urlparse(url)
    scheme = u.scheme or 'https'
    host = u.netloc or 'www.amazon.com'
    path = u.path

    if not u.query:
        query = ""
    else:
        query = "?"
        for piece in u.query.split('&'):
            k, v = piece.split("=")
            query += "{k}={v}&".format(**locals())
        query = query[:-1]

    return "{scheme}://{host}{path}{query}".format(**locals())


def log(msg):
    if settings.log_stdout:
        try:
            print("{}:{}".format(datetime.now(), msg))
        except UnicodeEncodeError:
            pass


def enqueue_categories_url(url):
    url = format_url(url)
    return redis.sadd("categories_queue", url)


def dequeue_categories_url():
    return redis.spop("categories_queue")


def enqueue_items_url(url):
    url = format_url(url)
    return redis.sadd("items_queue", url)


def dequeue_items_url():
    return redis.spop("items_queue")

