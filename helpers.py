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
    proxy_dict = get_proxy()
    try:
        if proxy_dict:
            r = requests.get(url, headers=headers, proxies=proxy_dict, timeout=20)
        else:
            r = requests.get(url, headers=headers, timeout=20)
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


def init_proxies():
    for url in settings.proxies:
        enqueue_proxy_url(url)


def get_proxy():
    url = dequeue_proxy_url()
    if not url:
        return None
    try:
        valid_url = "http://www.baidu.com"
        proxy_dict = {"https": url}
        re = requests.get(valid_url, proxies=proxy_dict, timeout=2)
    except Exception as e:
        log("url: {} is invalid".format(url))
        get_proxy()
    else:
        code = re.status_code
        if code >= 200 and code < 300:
            enqueue_proxy_url(url)
            return proxy_dict
        else:
            log("url: {} is invalid".format(url))
            get_proxy()


def enqueue_categories_url(url):
    url = format_url(url)
    return redis.sadd("categories_queue", url)


def dequeue_categories_url():
    url = redis.spop("categories_queue")
    if url:
        url = url.decode()
    return url


def enqueue_items_url(url):
    url = format_url(url)
    return redis.sadd("items_queue", url)


def dequeue_items_url():
    url = redis.spop("items_queue")
    if url:
        url = url.decode()
    return url


def enqueue_images_url(url, path):
    return redis.sadd("images_queue", "{}::::{}".format(path, url))


def dequeue_images_url():
    image = redis.spop("images_queue")
    if image:
        image = image.decode().split("::::")
        return image[0], image[1]
    return None, None


def enqueue_proxy_url(url):
    return redis.sadd("proxy_urls", url)


def dequeue_proxy_url():
    url = redis.spop("proxy_urls")
    if url:
        url = url.decode()
    return url

