#!/usr/bin/bash
trap "echo Error trapped; exit 0" ERR
# set -e will cause any error to exit the script
set -e
curl -s -L https://nvidia.github.io/libnvidia-container/stable/rpm/nvidia-container-toolkit.repo | \
  sudo tee /etc/yum.repos.d/nvidia-container-toolkit.repo

sudo yum install -y nvidia-container-toolkit
