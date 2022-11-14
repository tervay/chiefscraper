import os
import requests
from requests.adapters import HTTPAdapter
from requests.cookies import RequestsCookieJar
from urllib.parse import urlparse
from pprint import pprint
import json
from tqdm import tqdm
from ratelimit import limits, sleep_and_retry
from requests_ratelimiter import LimiterSession, LimiterAdapter

COOKIE_NAME = "_t"
COOKIE_VALUE = "bc203e0521fcd8d928b9ba67ee72ee2e"
BASE_URL = "https://www.chiefdelphi.com"
PATH = os.path.join(os.getcwd(), "data")

BASE_SCHEME = urlparse(BASE_URL).scheme
MAX_MORE_TOPICS = 99
TOPIC_PATH = "/latest.json?no_definitions=true&page="
BASE_TOPIC_URL = BASE_URL + TOPIC_PATH

session = LimiterSession(per_second=5)
session.mount(BASE_URL, HTTPAdapter(max_retries=5))
cookie_jar = RequestsCookieJar()
cookie_jar.set(COOKIE_NAME, COOKIE_VALUE, domain=urlparse(BASE_URL).hostname, path="/")

already_saved = set()
for f in os.listdir(PATH):
    post_id = int(f.split(".json")[0])
    already_saved.add(post_id)


def is_up_to_date(topic_json):
    post_count = topic_json["highest_post_number"]
    with open(os.path.join(PATH, f'{topic_json["id"]}.json'), "r") as f:
        data = json.load(fp=f)
        return data["highest_post_number"] == post_count


def write_topic(topic_json):
    if topic_json["id"] == 69972:
        return

    filename = f'{topic_json["id"]}.json'
    if topic_json["id"] in already_saved:
        if is_up_to_date(topic_json):
            return

    detailed_json = get_full_topic_details(topic_json)
    with open(os.path.join(PATH, filename), "w+") as f:
        json.dump(detailed_json, fp=f, ensure_ascii=False)
        already_saved.add(topic_json["id"])


def get_full_topic_details(topic_json):
    topic_relative_url = "t/" + topic_json["slug"] + "/" + str(topic_json["id"])
    topic_download_url = BASE_URL + "/" + topic_relative_url + ".json"
    topic_detailed_resp = session.get(topic_download_url, cookies=cookie_jar)
    topic_detailed_json = topic_detailed_resp.json()

    posts_json = topic_detailed_json["post_stream"]["posts"]
    posts_stream = topic_detailed_json["post_stream"]["stream"][20:]

    n = 20
    chunked_posts_stream = [
        posts_stream[i * n : (i + 1) * n]
        for i in range((len(posts_stream) + n - 1) // n)
    ]
    posts_download_url = BASE_URL + "/t/" + str(topic_json["id"]) + "/posts.json?"
    for chunk in chunked_posts_stream:
        formatted_posts_list = ""
        for post_id in chunk:
            formatted_posts_list = (
                formatted_posts_list + "post_ids[]=" + str(post_id) + "&"
            )
        chunked_resp = session.get(
            posts_download_url + formatted_posts_list, cookies=cookie_jar
        )
        chunked_json = chunked_resp.json()
        posts_json.extend(chunked_json["post_stream"]["posts"])

    topic_detailed_json["post_stream"]["posts"] = posts_json
    return topic_detailed_json


def main():
    count = 0
    url = BASE_TOPIC_URL + str(count)
    current_resp = session.get(url, cookies=cookie_jar)
    current_json = current_resp.json()

    while "more_topics_url" in current_json["topic_list"].keys():
        for topic in (pbar := tqdm(current_json["topic_list"]["topics"], leave=False)):
            pbar.set_description(f'{count} | {topic["id"]}')
            write_topic(topic)

        count += 1
        url = BASE_TOPIC_URL + str(count)
        current_resp = session.get(url, cookies=cookie_jar)
        current_json = current_resp.json()


if __name__ == "__main__":
    main()
