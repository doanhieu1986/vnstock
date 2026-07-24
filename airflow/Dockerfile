FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends gcc python3-dev curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Docker CLI (chi client, khong can dockerd) de goi `docker exec` vao container
# spark tu BashOperator - Airflow chi submit lenh, khong tu chay Spark trong chinh no.
RUN curl -fL -o /tmp/docker.tgz \
      https://download.docker.com/linux/static/stable/aarch64/docker-29.6.2.tgz \
    && tar -xzf /tmp/docker.tgz -C /usr/local/bin --strip-components=1 docker/docker \
    && rm /tmp/docker.tgz
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"
