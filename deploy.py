import os

from prefect import flow
from prefect.deployments import DeploymentImage
from prefect.client.schemas.schedules import CronSchedule

from flow import hun_min_kafka2minio_flow

if __name__ == "__main__":
    hun_min_kafka2minio_flow.deploy(
        name="hun_min_kafka2minio_deploy",
        work_pool_name="docker-agent-pool",
        work_queue_name="docker-agent",
        image=DeploymentImage(
            name="hun-min-kafka2minio",
            tag=os.getenv("VERSION"),
            dockerfile="Dockerfile",
            platform="linux/arm64",
            buildargs={
                       "LOGGING_LEVEL": os.getenv("LOGGING_LEVEL"),
                       "TOPIC_NAME": os.getenv("TOPIC_NAME"),
                       "KAFKA_URL": os.getenv("KAFKA_URL"),
                       "SPARK_URL": os.getenv("SPARK_URL"),
                       "MINIO_URL": os.getenv("MINIO_URL"),
                       "MINIO_ACCESS_KEY" : os.getenv("MINIO_ACCESS_KEY"),
                       "MINIO_SECRET_KEY" :  os.getenv("MINIO_SECRET_KEY"),
                       "MINIO_PATH" : os.getenv("MINIO_PATH"),
                       },
        ),
        schedule=(CronSchedule(cron="0 22 * * 1-5", timezone="Asia/Seoul")),
        build=True,
    )