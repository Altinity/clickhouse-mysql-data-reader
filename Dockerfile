#FROM python:3.8
FROM ubuntu:20.04 as build
COPY requirements.txt requirements.txt
RUN apt-get update && apt-get install -y python3-pip libmysqlclient-dev openssh-client mysql-client nano -y iputils-ping

RUN pip3 install --upgrade pip
RUN pip3 install mysqlclient
RUN pip3 install -r requirements.txt
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
#COPY . .
#RUN chmod +x dev_run_config_file.sh
#CMD ["/bin/bash", "dev_run_config_file.sh"]
