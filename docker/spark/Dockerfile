# Spark master / worker / history server image.
# Thin layer over the official image (multi-arch, so it runs natively on arm64):
# adds our spark-defaults.conf, the pipeline's Python dependencies and pytest, and an
# event-log directory owned by the spark user.

FROM apache/spark:3.5.9-java17-python3

USER root

# pytest runs the unit tests inside the cluster, with the same Python and Java.
RUN pip3 install --no-cache-dir pyyaml==6.0.2 pytest==8.3.3 \
 && mkdir -p /opt/spark/spark-events /opt/project \
 && chown spark:spark /opt/spark/spark-events

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

ENV PATH="/opt/spark/bin:/opt/spark/sbin:$PATH" \
    PYSPARK_PYTHON=python3

USER spark
WORKDIR /opt/project
