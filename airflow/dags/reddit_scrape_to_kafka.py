from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from scrap_reddit import scrape
from kafka_utils.producer import send_posts, send_comments


def scrape_and_stream():
    posts, comments = scrape()

    if posts:
        send_posts(posts)

    if comments:
        send_comments(comments)


with DAG(
    dag_id="reddit_ingestion_dag",
    schedule_interval="*/10 * * * *",  # every 10 minutes (near real-time)
    start_date=days_ago(1),
    catchup=False,
    tags=["reddit", "scraping", "kafka"]
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_reddit_and_send_to_kafka",
        python_callable=scrape_and_stream
    )
