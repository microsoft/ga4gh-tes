# Get the latest telegraf release tag:
FROM ubuntu:latest AS fetcher
RUN apt-get update && apt-get install -y curl jq patch
WORKDIR /app
RUN curl --silent "https://api.github.com/repos/influxdata/telegraf/releases/latest" | jq -r .tag_name > /app/latest_release
RUN echo "Latest telegraf release: $(cat /app/latest_release)"

# Build a custom telegraf plugin with the Azure Append Blob output plugin enabled
# Also enable changes to the filestat plugin for time filtering
FROM golang:latest AS builder
WORKDIR /app
COPY --from=fetcher /app/latest_release ./latest_release
RUN git clone https://github.com/influxdata/telegraf.git /app/telegraf/
WORKDIR /app/telegraf
RUN echo "Fetching the latest release: $(cat /app/latest_release)"
RUN git checkout tags/$(cat /app/latest_release)

# Add the Azure Append Blob patches:
COPY ./azure_append_blob/ /app/telegraf/plugins/outputs/azure_append_blob/
COPY ./all/azure_append_blob.go /app/telegraf/plugins/outputs/all
COPY ./telegraf.dummy.conf /app/telegraf/telegraf.dummy.conf
COPY ./tes_vm_monitor.once.conf /app/telegraf/tes_vm_monitor.once.conf
COPY ./tes_vm_monitor.continuous.conf /app/telegraf/tes_vm_monitor.continuous.conf
# Copy and apply the filestat patch:
FROM fetcher
WORKDIR /app/telegraf
COPY ./filestat.diff /app/telegraf/filestat.diff
RUN patch -p1 < ./filestat.diff

FROM builder 
WORKDIR /app/telegraf/
# NOTE: We explicitly are not building the entire telegraf code base as we only
# need custom_builder + the plugins it finds we need from telegraf.dummy.conf
# RUN make
RUN make build_tools
RUN export LDFLAGS="-s -w"; ./tools/custom_builder/custom_builder --config ./tes_vm_monitor.once.conf --config ./tes_vm_monitor.continuous.conf --config ./telegraf.dummy.conf

# Further shrink the size of telegraf:
WORKDIR /app/telegraf
RUN strip -s -x ./telegraf
