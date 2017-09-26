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
    log("make_request:  "+url)
    trying_times = redis.spop(url)
    trying_times = int(trying_times.decode()) if trying_times else 0
    log("tring_times: {}".format(trying_times))
    if trying_times > settings.max_retrying_times:
        log("Tring too many times")
        return None, None  # 超过重试次数
    if "picassoRedirect" in url:
        return None, None  # 跳过重定向的url

    global num_requests
    if num_requests >= settings.max_requests:
        raise Exception("Reached the max number of requests: {}".format(settings.max_requests))

    headers = settings.headers
    headers["User-Agent"] = random.choice(settings.agents)
    proxy_dict = get_proxy()
    timeout = 20 if return_soup else 60
    try:
        if proxy_dict:
            r = requests.get(url, headers=headers, proxies=proxy_dict, timeout=timeout)
        else:
            r = requests.get(url, headers=headers, timeout=timeout)
    except RequestException as e:
        log("WARNING: Request for {} failed. Retrying.....{} times".format(url, trying_times+1))
        redis.sadd(url, str(trying_times+1))
        return make_request(url, return_soup)

    num_requests += 1
    if r.status_code != 200:
        os.system('say "Got non-200 Response"')
        log("WARNING: Got a {} status code for URL: {}".format(r.status_code, url))
        return None, None

    if return_soup:
        return BeautifulSoup(r.text, "lxml"), r.text
    return r, r


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
    url = "https://us-proxy.org/"
    page, html = make_request(url)
    if not page:
        return
    tr_tag = page.select("table.table tbody tr")
    if tr_tag:
        for tag in tr_tag:
            th_tag = tag.select("td")
            ip = th_tag[0].string
            port = th_tag[1].string
            ptype = "https" if th_tag[6].string == "yes" else "http"
            enqueue_proxy_url("{}://{}:{}".format(ptype, ip, port))


def get_proxy():
    url = dequeue_proxy_url()
    if not url:
        return None
    return {url.split("://")[0]: url}
    ''' 不在此处判断ip是否可用
    try:
        valid_url = "http://www.baidu.com"
        proxy_dict = {url.split("://")[0]: url}
        headers = settings.headers
        headers["User-Agent"] = random.choice(settings.agents)
        re = requests.get(valid_url, proxies=proxy_dict, headers=headers, timeout=6)
    except Exception as e:
        log("url: {} is invalid".format(url))
        return get_proxy()
    else:
        code = re.status_code
        if code >= 200 and code < 300:
            enqueue_proxy_url(url)
            return proxy_dict
        else:
            log("url: {} is invalid".format(url))
            return get_proxy()
    '''


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
    url = redis.srandmember("proxy_urls")
    if url:
        url = url.decode()
    return url


def backup_categories_url():
    with open("categories_url.txt", "w") as f:
        while True:
            url = dequeue_categories_url()
            if url:
                print(url, file=f)
            else:
                break


def backup_items_url():
    with open("items_url.txt", "w") as f:
        while True:
            url = dequeue_items_url()
            if url:
                print(url, file=f)
            else:
                break
