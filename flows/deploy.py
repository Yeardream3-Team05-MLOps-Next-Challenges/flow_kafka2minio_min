import os

from prefect import flow
from prefect.deployments import DeploymentImage
from prefect.client.schemas.schedules import CronSchedule

from flow import min_kafka2minio_flow

if __name__ == "__main__":
    min_kafka2minio_flow.deploy(
        name="min-kafka2minio-deployment",
        work_pool_name="docker-agent-pool",
        work_queue_name="docker-agent",
        image=DeploymentImage(
            name="min-kafka2minio-flow",
            tag="0.1",
            dockerfile="Dockerfile",
            platform="linux/arm64",
            buildargs={
                       "KAFKA_URL": os.getenv("KAFKA_URL"),
                       "SPARK_URL": os.getenv("SPARK_URL"),
                       "MINIO_URL": os.getenv("MINIO_URL"),
                       },
        ),
        schedule=(CronSchedule(cron="0 22 * * *", timezone="Asia/Seoul")),
        build=True,
    )