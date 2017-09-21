from html.parser import HTMLParser
import html
from helpers import format_url, enqueue_images_url
import requests
import os

htmlparser = HTMLParser()


def get_category(page):
    category = ""
    for tag in page.select("#s-result-count span a"):
        category = "{}{}:::".format(category, tag.string)
    last_tag = page.select("#s-result-count span")[-1]
    if last_tag:
        category = "{}{}".format(category, last_tag.string)

    return category


def get_title(item):
    try:
        title_tag = item.select("h2.s-access-title")[0]
        if title_tag:
            return html.unescape(title_tag.string)
        else:
            return "<missing product title>"
    except IndexError as err:
        return "<missing product title>"
    except Exception as e:
        return "<missing product title>"


def get_url(item):
    try:
        link_tag = item.select("a.s-access-detail-page")[0]
        if link_tag:
            return format_url(link_tag['href'])
        else:
            return "<missing product url>"
    except IndexError as err:
        return "<missing product url>"
    except Exception as e:
        return "<missing product url>"


def get_asin(item):
    asin = item['data-asin']
    if asin:
        return asin
    else:
        return "<missing product asin>"


def get_price(item):
    try:
        price_tag = item.select(".sx-price")[0]
        if price_tag:
            currency = ""
            whole = ""
            fractional = ""
            currency_tag = price_tag.select(".sx-price-currency")[0]
            if currency_tag:
                currency = currency_tag.string
            whole_tag = price_tag.select(".sx-price-whole")[0]
            if whole_tag:
                whole = whole_tag.string
            fractional_tag = price_tag.select(".sx-price-fractional")[0]
            if fractional_tag:
                fractional = fractional_tag.string
            return "{}{}.{}".format(currency, whole, fractional)
        else:
            return "<missing product price>"
    except IndexError as err:
        return "<missing product price>"
    except Exception as e:
        return "<missing product price>"


def get_primary_img(item):
    try:
        asin = get_asin(item)
        thumb_tag = item.select(".s-access-image")[0]
        if thumb_tag:
            return thumb_tag['src']
        else:
            return "<missing product img>"
    except IndexError as err:
        return "<missing product img>"
    except Exception as e:
        return "<missing product img>"


def download_img(url, category, asin):
    dir_name = "images/{}".format(category)
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)
    path = "{}/{}.jpg".format(dir_name, asin)
    #with open(path, "wb") as f:
    #    f.write(requests.get(url).content)
    # 异步获取图片
    enqueue_images_url(url, path)
    return path
