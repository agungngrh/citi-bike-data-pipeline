FROM apache/airflow:3.1.7-python3.12

ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-3.1.7/constraints-3.12.txt"

COPY --chown=airflow:root docker/airflow/requirements.txt /tmp/requirements.txt
COPY --chown=airflow:root docker/airflow/dbt-requirements.txt /tmp/dbt-requirements.txt

RUN pip install --no-cache-dir \
        "apache-airflow==3.1.7" \
        --constraint "${CONSTRAINT_URL}"

RUN pip install --no-cache-dir \
        "apache-airflow-providers-google==22.4.0" \
        "apache-airflow-providers-slack==9.10.2"

RUN pip install --no-cache-dir \
        -r /tmp/requirements.txt \
    && pip check \
    && rm -f /tmp/requirements.txt

USER root
RUN mkdir -p /opt/dbt_venv \
    && chown airflow:root /opt/dbt_venv
USER airflow

RUN python -m venv /opt/dbt_venv \
    && /opt/dbt_venv/bin/pip install --no-cache-dir \
        -r /tmp/dbt-requirements.txt \
    && rm -f /tmp/dbt-requirements.txt