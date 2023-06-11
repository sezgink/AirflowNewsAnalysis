# FROM apache/airflow:latest
FROM apache/airflow:2.6.1
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
         git-all \
         curl \
         jq \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
  
# COPY requirements.txt /
COPY requirements.txt /home/airflow/
USER airflow
RUN pip install --no-cache-dir -r /home/airflow/requirements.txt
RUN pip freeze > /home/airflow/installed.txt
# RUN pip install --no-cache-dir -r /requirements.txt
# RUN pip freeze > /installed.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"
# RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# RUN COMMIT=$(curl -s https://api.github.com/repos/sezgink/TwitterScraper/git/ref/heads/main  | jq -r ".object.sha")
RUN date > /tmp/now
RUN git clone https://github.com/sezgink/TwitterScraper.git
USER airflow