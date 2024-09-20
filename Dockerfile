FROM apache/airflow:2.5.1

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

USER root
RUN apt-get update && apt-get install -y openjdk-11-jdk procps
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow