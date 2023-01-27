# Creates a Docker image that is ready to execute tes compliance suite

FROM ubuntu:latest AS build-env
WORKDIR /app

# Install TES Compliance suite dependencies
# Git is needed for the clone and jq is helpful with parsing results if needed
RUN apt update
ENV DEBIAN_FRONTEND=noninteractive
RUN apt install -y software-properties-common

RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt update
RUN apt -y install python3.8 python3.8-distutils python3-pip git jq

RUN git clone https://github.com/elixir-cloud-aai/tes-compliance-suite.git 

# Install TES Compliance suite
WORKDIR /app/tes-compliance-suite
RUN python3.8 setup.py install

# Precreate results directory
RUN mkdir results

# Copy over specific instructions around how to run the tests
COPY entrypoint.sh /app/tes-compliance-suite/entrypoint.sh

ENTRYPOINT ["/bin/bash", "entrypoint.sh"]