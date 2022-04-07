FROM ubuntu:latest

# ---  install python (from https://phoenixnap.com/kb/how-to-install-python-3-ubuntu) --- #
RUN apt update
RUN apt install -y software-properties-common
# add deadsnakes repository since it contains later releases than default ubuntu repositories
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt update
RUN apt install -y python3.8

RUN apt install -y libpq-dev libssl-dev libcurl4-openssl-dev
RUN apt install -y python3-pip
RUN pip3 install ipython pandas sqlalchemy pyarrow fastparquet
RUN pip3 install wget psycopg2 requests
RUN apt install -y postgresql postgresql-contrib
