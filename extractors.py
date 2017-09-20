from html.parser import HTMLParser

htmlparser = HTMLParser()


def get_category(page):
    category = ""
    for tag in page.select("#s-result-count span a"):
        category = "{}{}:::".format(category, tag.string)
    last_tag = page.select("#s-result-count span")[-1]
    if last_tag:
        category = "{}{}".format(category, last_tag.string)

    return category
