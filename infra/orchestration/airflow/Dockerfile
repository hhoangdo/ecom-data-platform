FROM apache/airflow:2.10.5-python3.12

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends docker.io docker-compose-plugin curl \
  && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md /opt/vina-bim-shop/
COPY src /opt/vina-bim-shop/src

USER airflow

RUN python -m pip install --no-cache-dir /opt/vina-bim-shop \
  && python -m pip install --no-cache-dir "great-expectations==1.17.2"

