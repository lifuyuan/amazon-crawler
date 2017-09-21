import helpers
import eventlet
from datetime import datetime
import extractors
import settings
from models import Product, conn, cursor
import re
import requests

crawl_time = datetime.now()

pool = eventlet.GreenPool(settings.max_threads)
pile = eventlet.GreenPile(pool)


# 初始化爬虫
def init_crawl():
    start_url = "https://www.amazon.com/gp/site-directory/ref=nav_shopall_btn"
    page, html = helpers.make_request(start_url)
    if not page:
        return
    for tag in page.select('.fsdDeptCol a'):
        helpers.enqueue_categories_url(tag['href'])


# 目录爬取
def crawl_categories():
    url = helpers.dequeue_categories_url()
    if not url:
        helpers.log("WARNING: No URLs found in the queue. Retrying...")
        pile.spawn(crawl_categories)
        return
    page, html = helpers.make_request(url)
    if not page:
        return
    items = page.select('.s-result-list li.s-result-item')
    if len(items) > 0:
        helpers.enqueue_items_url(url)
    text_tag = page.find(text=re.compile('Show results for'))
    subcategories = page.select('.a-carousel li  .list-item__category-link')
    if text_tag:
        div_tag = text_tag.find_parent().find_parent()
        subcategories.extend(div_tag.select('.s-ref-indent-one li a'))
        subcategories.extend(div_tag.select('.s-ref-indent-two li a'))
    for tag in subcategories:
        helpers.enqueue_categories_url(tag['href'])
    pile.spawn(crawl_categories)


# 商品信息爬取
def crawl_items():
    url = helpers.dequeue_items_url()
    if not url:
        helpers.log("WARNING: No URLs found in the queue. Retrying...")
        pile.spawn(crawl_items)
        return
    page, html = helpers.make_request(url)
    if not page:
        return
    items = page.select('.s-result-list li.s-result-item')
    category = extractors.get_category(page)
    for item in items:
        asin = extractors.get_asin(item)
        title = extractors.get_title(item)
        product_url = extractors.get_url(item)
        list_url = url
        price = extractors.get_price(item)
        img_url = extractors.get_primary_img(item)
        img_path = extractors.download_img(img_url, category.split(":::")[-1], asin)
        product = Product(
            category=category,
            asin=asin,
            title=title,
            product_url=product_url,
            list_url=list_url,
            price=price,
            img_url=img_url,
            img_path=img_path,
            crawl_time=datetime.now()
        )
        product.save()
    next_link_tag = page.select("a#pagnNextLink")
    if next_link_tag:
        helpers.log(" Found 'Next' link on {}: {}".format(url, next_link_tag[0]["href"]))
        helpers.enqueue_items_url(next_link_tag[0]["href"])
        pile.spawn(crawl_items)


# 商品图片获取
def crawl_images():
    path, url = helpers.dequeue_images_url()
    if not url:
        helpers.log("WARNING: No URLs found in the queue. Retrying...")
        pile.spawn(crawl_images)
        return
    proxy_dict = helpers.get_proxy()
    try:
        if proxy_dict:
            content = requests.get(url, proxies=proxy_dict, timeout=10).content
        else:
            content = requests.get(url, timeout=10).content
        with open(path, "wb") as f:
            f.write(content)
    except Exception as e:
        helpers.log(e)
    pile.spawn(crawl_images)

if __name__ == "__main__":
    # init crawl
    #init_crawl()
    # 爬取目录
    helpers.log("Beginning crawl at {}".format(crawl_time))
    #[pile.spawn(crawl_categories) for _ in range(settings.max_threads)]
    #pool.waitall()

    [pile.spawn(crawl_images) for _ in range(settings.max_threads)]
    pool.waitall()
    #cursor.close()
    #conn.close()

