import helpers
import eventlet
from datetime import datetime
import settings

crawl_time = datetime.now()

pool = eventlet.GreenPool(settings.max_threads)
pile = eventlet.GreenPile(pool)


def begin_crawl_categories():
    start_url = "https://www.amazon.com/gp/site-directory/ref=nav_shopall_btn"
    page, html = helpers.make_request(start_url)
    if not page:
        return
    for tag in page.select('.fsdDeptCol a'):
        helpers.enqueue_categories_url(tag['href'])


def crawl_categories():
    url = helpers.dequeue_categories_url()
    if not url:
        helpers.log("WARNING: No URLs found in the queue. Retrying...")
        pile.spawn(crawl_categories)
        return
    page, html = helpers.make_request(url.decode())
    if not page:
        return
    items = page.select('.s-result-list li.s-result-item')
    if len(items) > 0:
        helpers.enqueue_items_url(url.decode())
    subcategories = page.select('.s-ref-indent-one li a')
    subcategories.extend(page.select('.s-ref-indent-two li a'))
    subcategories.extend(page.select('.a-carousel li  .list-item__category-link'))
    for tag in subcategories:
        helpers.enqueue_categories_url(tag['href'])
    pile.spawn(crawl_categories)


if __name__ == "__main__":
    begin_crawl_categories()
    helpers.log("Beginning crawl at {}".format(crawl_time))
    [pile.spawn(crawl_categories) for _ in range(settings.max_threads)]
    pool.waitall()

