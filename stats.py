from dataclasses import dataclass
import os
import json
from typing import List
from mpire import WorkerPool
from statistics import stdev, median

PATH = os.path.join(os.getcwd(), "data")

filenames = [x for x in os.listdir(PATH)]


@dataclass
class PostStats:
    post_number: int
    total_reacts: int


@dataclass
class ThreadStats:
    url: str
    total_comments: int
    post_stats: PostStats


def get_stats(filename: List[str]) -> ThreadStats:
    with open(os.path.join(PATH, filename), "r") as f:
        data = json.load(f)

    p0 = data["post_stream"]["posts"][0]

    return ThreadStats(
        url=f'https://www.chiefdelphi.com/t/{p0["topic_slug"]}/{p0["topic_id"]}',
        total_comments=len(data["post_stream"]["posts"]),
        post_stats=[
            PostStats(
                total_reacts=post["reaction_users_count"],
                post_number=post["post_number"],
            )
            for post in data["post_stream"]["posts"]
        ],
    )


with WorkerPool() as pool:
    ts = pool.map(get_stats, filenames, progress_bar=True)
    num_comments = [t.total_comments for t in ts]

    print(f"Total: {sum(num_comments):,}")
    print(f"Avg: {round(sum(num_comments) / len(filenames), 2):,}")
    print(f"StDev: {round(stdev(num_comments), 2):,}")
    print(f"Med: {round(median(num_comments), 2):,}")

    print("===")

    print("Top 10 most reacted posts")
    x = sorted(ts, key=lambda ts: -max([ps.total_reacts for ps in ts.post_stats]))
    for ts in x[:10]:
        most_reacted = max(ts.post_stats, key=lambda ps: ps.total_reacts)
        print(f"{most_reacted.total_reacts} - {ts.url}/{most_reacted.post_number}")
