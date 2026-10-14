# HDFS image (namenode / datanode) for Apple Silicon and x86.
# The official apache/hadoop images are amd64-only, so we build our own from
# the Apache release tarball. Hadoop 3.3.x supports Java 8 and 11 at runtime.

FROM eclipse-temurin:11-jre-jammy

ARG HADOOP_VERSION=3.3.6
# Published SHA-512 checksums from dlcdn.apache.org/hadoop/common/hadoop-3.3.6/
ARG HADOOP_SHA512_ARM64=b0a485c5ae0d97b08285bb83a3dd78b834153b2bd53f96ac301852862ccaff139f2c547aee808d5ad4afdc3a204b1c3b6ba459ea83ebf2779e9c129f37c69059
ARG HADOOP_SHA512_AMD64=de3eaca2e0517e4b569a88b63c89fae19cb8ac6c01ff990f1ff8f0cc0f3128c8e8a23db01577ca562a0e0bb1b4a3889f8c74384e609cd55e537aada3dcaa9f8a
ARG TARGETARCH

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates procps \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd -r hadoop && useradd -r -g hadoop -d /hadoop -m hadoop \
 && mkdir -p /hadoop/dfs/name /hadoop/dfs/data \
 && chown -R hadoop:hadoop /hadoop

# Download, verify and unpack Hadoop. Docs and the AWS SDK bundle (~600 MB)
# are removed because this image only runs HDFS. Ownership is set in the same
# layer; a separate `chown -R` would store every file a second time.
RUN set -eux; \
    case "$TARGETARCH" in \
      arm64) file="hadoop-$HADOOP_VERSION-aarch64.tar.gz"; sha="$HADOOP_SHA512_ARM64" ;; \
      amd64) file="hadoop-$HADOOP_VERSION.tar.gz";         sha="$HADOOP_SHA512_AMD64" ;; \
      *) echo "Unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    curl -fSL --retry 5 -o /tmp/hadoop.tar.gz \
      "https://dlcdn.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/$file" \
    || curl -fSL --retry 5 -o /tmp/hadoop.tar.gz \
      "https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/$file"; \
    echo "$sha  /tmp/hadoop.tar.gz" | sha512sum -c -; \
    tar -xzf /tmp/hadoop.tar.gz -C /opt; \
    mv "/opt/hadoop-$HADOOP_VERSION" "$HADOOP_HOME"; \
    rm -rf /tmp/hadoop.tar.gz "$HADOOP_HOME/share/doc" \
           "$HADOOP_HOME"/share/hadoop/tools/lib/bundle-*.jar; \
    chown -R hadoop:hadoop "$HADOOP_HOME"

COPY --chown=hadoop:hadoop core-site.xml hdfs-site.xml $HADOOP_CONF_DIR/
COPY --chmod=755 entrypoint.sh /entrypoint.sh

USER hadoop
WORKDIR /hadoop
ENTRYPOINT ["/entrypoint.sh"]
