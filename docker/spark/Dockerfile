FROM apache/spark:3.5.9-scala2.12-java17-python3-r-ubuntu

USER root

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/spark/app

RUN mkdir -p /opt/spark/conf \
    /opt/spark/app \
    /opt/spark/ivy-cache \
    && chown -R spark:spark /opt/spark/conf /opt/spark/app /opt/spark/ivy-cache

RUN pip install --no-cache-dir python-dotenv==1.2.3

RUN curl -fsSL \
    https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.13/gcs-connector-hadoop3-2.2.13-shaded.jar \
    -o /opt/spark/jars/gcs-connector-hadoop3-2.2.13-shaded.jar

COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark