from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.utils.dates import days_ago

from scrap_reddit import scrape
from kafka_utils.producer import send_posts, send_comments


def scrape_and_stream():
    posts, comments = scrape()

    print(f"✅ Posts scraped: {len(posts)}")
    print(f"✅ Comments scraped: {len(comments)}")

    if posts:
        send_posts(posts)

    if comments:
        send_comments(comments)


with DAG(
    dag_id="reddit_ingestion_dag",
    schedule_interval="*/30 * * * *",  # réaliste pour Reddit
    start_date=days_ago(1),
    catchup=False,
    tags=["reddit", "scraping", "kafka"]
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_reddit_and_send_to_kafka",
        python_callable=scrape_and_stream
    )