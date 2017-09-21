from twisted.enterprise import adbapi
import psycopg2

import settings
import helpers

''' 异步参数
dbparams = dict(
    database=settings.pgs_database,
    user=settings.pgs_user,
    password=settings.pgs_password,
    host=settings.pgs_host,
    port=settings.pgs_port
)
dbpool = adbapi.ConnectionPool("psycopg2", **dbparams)
'''
try:
    conn = psycopg2.connect(database=settings.pgs_database, user=settings.pgs_user,
                            password=settings.pgs_password, host=settings.pgs_host, port=settings.pgs_port)
    cursor = conn.cursor()
except Exception as e:
    helpers.log(e)


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

    def save(self):
        insert_sql = "INSERT INTO products(category, asin, title, product_url, list_url, price,\
                img_url, img_path, crawl_time) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        # helpers.log(insert_sql)
        try:
            data = (self.category, self.asin, self.title, self.product_url, self.list_url, self.price, self.img_url,
                    self.img_path, self.crawl_time)
            cursor.execute(insert_sql, data)
            conn.commit()
        except Exception as err:
            conn.rollback()
            helpers.log(err)

'''
    def save(self):
        # 使用twisted将postgresql插入变成异步执行
        helpers.log("save")
        query = dbpool.runInteraction(self.do_insert)
        query.addErrback(self.handle_error)

    def do_insert(self, cursor):
        insert_sql = "INSERT INTO products(category, asin, title, product_url, list_url, price,\
                img_url, img_path, crawl_time) values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(
            self.category, self.asin, self.title, self.product_url, self.list_url, self.price, self.img_url,
            self.img_path, self.crawl_time)
        helpers.log(insert_sql)
        cursor.execute(insert_sql)

    def handle_error(self, failure):
        helpers.log(failure)
'''