import json
from datetime import datetime
from pymongo import MongoClient
import click
import time
import math
from bson.json_util import dumps

__author__ = "Igor Motyka"

new_line_replacement = " nowaliniaigora "


@click.command()
@click.argument('path', type=click.Path(exists=True))
@click.option('--host', '-h', default="localhost", help="IP address of server hosting MongoDb.")
@click.option('--port', '-p', type=click.IntRange(0, 65535), default=27017, help="Port on which MongoDb listens.")
@click.option('--limit', '-l', type=click.IntRange(-1), default=-1, help="Maximum number of pairs to save into database. By default all found comments are saved.")
def main(path, host, port, limit):
    """Cleans reddit comments and save them into Mongo database"""
    startTime = time.time()
    client = MongoClient(host, port)

    with client.start_session() as mongo_session:
        client.reddit.comments.delete_many({})
        comments_collection = client.reddit.comments
        with mongo_session.start_transaction():
            retrive_comments_from_file(
                path, limit, comments_collection, mongo_session)
            mongo_session.commit_transaction()
        mongo_session.end_session()

    endTime = time.time()
    elapsed = endTime-startTime
    print_time(elapsed)


def print_time(elapsed, header="Execution"):
    click.echo("{} took {} min {} sec".format(
        header, math.floor(elapsed/60), math.ceil(elapsed % 60)))


def normalize_comment_body(comment):
    if not comment:
        return None

    return comment['body'].replace("\n", new_line_replacement).replace("\r", new_line_replacement).replace('"', "'")


def is_comment_valid(comment):
    if comment['score'] < 2:
        return False

    if comment['body'] == "[deleted]" or comment['body'] == "[removed]":
        return False

    if len(comment['body']) < 1 or len(comment['body'].split(' ')) > 75:
        return False

    return True


def normalize_parent_id(parent_id):
    return parent_id.split("_")[1]


def find_best_scoring_comment(parent_id, new_score, comments_collection, mongo_session):
    if not parent_id or new_score:
        return None

    comments = comments_collection.find(
        {'$and': [{'parent_id': parent_id}, {'score': {'$gt': new_score}}]}, session=mongo_session).sort('score', -1).limit(1)

    comments = list(comments)
    if not comments:
        return None
    else:
        return comments[0]


def retrive_comments_from_file(path, max_number_of_pairs, comments_collection, mongo_session):
    comments_counter = 0
    pairs_counter = 0
    total_processed_counter = 0
    try:
        with open(path, 'r') as file:
            with click.progressbar(iterable=file, label="Retriving comments from file...") as progress:
                for row in progress:
                    row = json.loads(row)
                    total_processed_counter += 1
                    if is_comment_valid(row) is False:
                        continue

                    normalized_parent_id = normalize_parent_id(
                        row['parent_id'])

                    parent = comments_collection.find_one(
                        {'comment_id': normalized_parent_id}, session=mongo_session)

                    comment = {
                        'comment_id': row['id'],
                        'parent_id': normalized_parent_id,
                        'created_utc': row["created_utc"],
                        'score': row["score"],
                        'subreddit': row["subreddit"],
                        'controversiality': row['controversiality'],
                        'body': normalize_comment_body(row),
                        'parent_body': normalize_comment_body(parent),
                    }

                    if comment['parent_body']:
                        pairs_counter += 1

                    best_comment = find_best_scoring_comment(
                        normalized_parent_id, comment['score'], comments_collection, mongo_session)

                    if best_comment and comment['score'] > best_comment['score']:
                        comments_collection.replace_one(
                            {'comment_id': best_comment['comment_id']}, comment, session=mongo_session)
                    else:
                        comments_collection.insert_one(
                            comment, session=mongo_session)

                    comments_counter += 1
                    if pairs_counter >= max_number_of_pairs and max_number_of_pairs != -1:
                        break

            click.echo("Processed comments: {} ".format(
                total_processed_counter))

            click.echo("Inserted comments: {} ".format(
                comments_counter))

            click.echo("Valid pairs: {}".format(
                pairs_counter))

    except Exception as err:
        raise err


if __name__ == "__main__":
    main()
