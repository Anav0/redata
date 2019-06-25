import json
import ijson
from datetime import datetime
from pymongo import MongoClient
import click
import time
import math

__author__ = "Igor Motyka"

new_line_replacement = " nowaliniaigora "


@click.command()
@click.argument('path', type=click.Path(exists=True))
@click.option('--host', '-h', default="localhost", help="IP address of server hosting MongoDb.")
@click.option('--port', '-p', type=click.IntRange(0, 65535), default=27017, help="Port on which MongoDb listens.")
@click.option('--limit', '-l', type=click.IntRange(-1), default=-1, help="Maximum number of comments to save into database. By default all comments are saved.")
def main(path, host, port, limit):
    """Cleans reddit comments and save them into Mongo database"""
    try:
        startTime = time.time()
        client = MongoClient(host, port)
        db = client.gymba
        posts_collection = db.posts
        posts = retrive_posts_from_file(path, limit)
        insert_posts(posts, posts_collection)
        endTime = time.time()
        elapsed = endTime-startTime
        print("Execution took {} seconds".format(math.ceil(elapsed)))
    except Exception as err:
        print(str(err))


def normalize_post_body(body):
    return body.replace("\n", new_line_replacement).replace("\r", new_line_replacement).replace('"', "'")


def is_post_valid(post):
    if post['score'] < 2:
        return False

    if post['body'] == "[deleted]" or post['body'] == "[removed]":
        return False

    if len(post['body']) < 1 or len(post['body'].split(' ')) > 75:
        return False

    return True


def insert_posts(posts, posts_collection):
    print("Inserting posts into database...")
    posts_collection.insert_many(posts)
    print("Inserted posts into the database")


def retrive_posts_from_file(path, max_number_of_posts):

    posts = []
    posts_counter = 0

    try:
        with open(path, 'r') as file:
            with click.progressbar(iterable=file, label="Retriving posts from file...") as comments:
                for row in comments:
                    row = json.loads(row)
                    if is_post_valid(row) is False:
                        continue
                    post = {
                        'post_id': row['link_id'],
                        'parent_id': row['parent_id'],
                        'created_utc': row["created_utc"],
                        'score': row["score"],
                        'subreddit': row["subreddit"],
                        'controversiality': row['controversiality'],
                        'body': normalize_post_body(row["body"]),
                    }
                    posts.append(post)
                    posts_counter += 1
                    if posts_counter >= max_number_of_posts and max_number_of_posts != -1:
                        print("\nRetrived {} valid posts".format(len(posts)))
                        return posts

            print("\nRetrived {} valid posts".format(len(posts)))
            return posts

    except Exception as err:
        raise err


if __name__ == "__main__":
    main()
