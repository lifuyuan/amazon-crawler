from twisted.enterprise import adbapi
import psycopg2

import settings

class Product(object):
    def __init__(self, category, asin, title, product_url, list_url, price, img_url, img_path, crawl_time):
        super(Product, self).__init__()
        self.category = category
        self.asin = asin
        self.title = title
        self.product_url = product_url
        self.list_url = list_url
        self.price = price
        self.img_url = img_url
        self.img_path = img_path
        self.crawl_time = crawl_time
        dbparams = dict(
            database=settings.pgs_database,
            user=settings.pgs_user,
            password=settings.pgs_password,
            host=settings.pgs_host,
            port=settings.pgs_port
        )
        self.dbpool = adbapi.ConnectionPool("psycopg2", **dbparams)

    def save(self):
        yield self.dbpool.runOperation("INSERT INTO products(category, asin, title, product_url, list_url, price,\
        img_url, img_path, crawl_time) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)  RETURNING id", (self.category,
        self.asin, self.title, self.product_url, self.list_url, self.price, self.img_url, self.img_path, self.crawl_time))
