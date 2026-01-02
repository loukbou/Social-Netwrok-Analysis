from kafka import KafkaProducer
import json
import logging

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=3,
    linger_ms=100
)

def send_posts(posts):
    for post in posts:
        producer.send(
            topic="reddit_posts",
            key=post["post_id"].encode("utf-8"),
            value=post
        )
    producer.flush()

def send_comments(comments):
    for comment in comments:
        producer.send(
            topic="reddit_comments",
            key=comment["comment_id"].encode("utf-8"),
            value=comment
        )
    producer.flush()
