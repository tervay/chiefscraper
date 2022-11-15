import json
import os
import re
import unicodedata

from bs4 import BeautifulSoup, SoupStrainer
from pyrate_limiter import Duration, Limiter, RequestRate
from requests.adapters import HTTPAdapter
from requests_ratelimiter import LimiterSession
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map

BASE_URL = "https://www.chiefdelphi.com"
PATH = os.path.join(os.getcwd(), "data")

TOPIC_PATH = "/latest.json?no_definitions=true&page="
BASE_TOPIC_URL = BASE_URL + TOPIC_PATH

session = LimiterSession(limiter=Limiter(RequestRate(200, Duration.MINUTE)))
session.mount(BASE_URL, HTTPAdapter(max_retries=5))

already_saved = set()
for f in os.listdir(os.path.join(os.getcwd(), "media_2")):
    already_saved.add(f)


def slugify(value, allow_unicode=False):
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    ext = value[value.rfind(".") + 1 :]
    value = value[: value.rfind(".")]

    if allow_unicode:
        value = unicodedata.normalize("NFKC", value)
    else:
        value = (
            unicodedata.normalize("NFKD", value)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
    value = re.sub(r"[^\w\s-]", "", value.lower())
    return re.sub(r"[-\s]+", "-", value).strip("-_") + "." + ext


def download(params):
    url, filename = params
    if filename in already_saved:
        return

    resp = session.get(url, stream=True)
    with open(f"media_2/{filename}", "wb+") as f:
        for data in resp:
            f.write(data)

    already_saved.add(filename)


def main2():
    with open("to_save.json", "r") as f:
        to_save = json.load(f)

    thread_map(download, list(to_save.items()))


def main3():
    regex = re.compile("^https://www.chiefdelphi.com/uploads/default/original/")
    url_to_filename = {}
    for f in (pbar := tqdm(os.listdir(PATH))):
        with open(os.path.join(PATH, f), "r") as fp:
            data = json.load(fp)
            topic_id = data["id"]
            pbar.set_description(str(topic_id).rjust(10))

        for post in data["post_stream"]["posts"]:
            if "link_counts" in post and len(post["link_counts"]) > 0:
                if any(
                    [
                        str(link["url"]).startswith(
                            "https://www.chiefdelphi.com/uploads/default/"
                        )
                        for link in post["link_counts"]
                    ]
                ):
                    post_id = post["id"]
                    soup = BeautifulSoup(
                        post["cooked"],
                        parse_only=SoupStrainer("a"),
                        features="html.parser",
                    )

                    # Check for child tag, which is used in inline image uploads
                    if soup.find("img"):
                        for tag in soup.findAll("a", href=regex):
                            url = tag["href"]
                            hashed_name = url[56:]  # trim through default/original/3X/
                            hashed_name = str(hashed_name).replace("/", "_")
                            local_filename = (
                                str(topic_id)
                                + "__--__"
                                + str(post_id)
                                + "__--__"
                                + hashed_name
                            )
                            url_to_filename[url] = local_filename
                    else:
                        for tag in soup.findAll("a", href=regex):
                            display_name = slugify(tag.get_text())
                            url = tag["href"]
                            hashed_name = url[56:]  # trim through default/original/3X/
                            hashed_name = str(hashed_name).replace("/", "_")
                            # now get rid of the file extension
                            hashed_name = ".".join(hashed_name.split(".")[:-1])
                            local_filename = (
                                str(topic_id)
                                + "__--__"
                                + str(post_id)
                                + "__--__"
                                + hashed_name
                                + "__--__"
                                + display_name
                            )
                            url_to_filename[url] = local_filename

    with open("to_save.json", "w+") as f:
        json.dump(url_to_filename, fp=f, indent=2)


if __name__ == "__main__":
    main2()
