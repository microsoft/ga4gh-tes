# Build a custom telegraf plugin with the Azure Append Blob output plugin enabled
FROM golang:1.22

WORKDIR /app
RUN git clone https://github.com/influxdata/telegraf.git /app/telegraf/
WORKDIR /app/telegraf
RUN git checkout tags/v1.29.4

# Add the Azure Append Blob patches:
COPY ./azure_append_blob/ /app/telegraf/plugins/outputs/azure_append_blob/
COPY ./all/azure_append_blob.go /app/telegraf/plugins/outputs/all
COPY ./telegraf.dummy.conf /app/telegraf/telegraf.dummy.conf
COPY ./tes_vm_monitor.once.conf /app/telegraf/tes_vm_monitor.once.conf
COPY ./tes_vm_monitor.continuous.conf /app/telegraf/tes_vm_monitor.continuous.conf

WORKDIR /app/telegraf/
# NOTE: We explicitly are not building the entire telegraf code base as we only
# need custom_builder + the plugins it finds we need from telegraf.dummy.conf
# RUN make
RUN make build_tools
RUN export LDFLAGS="-s -w"; ./tools/custom_builder/custom_builder --config ./tes_vm_monitor.once.conf --config ./tes_vm_monitor.continuous.conf --config ./telegraf.dummy.conf

# Further shrink the size of telegraf:
WORKDIR /app/telegraf
RUN strip -s -x ./telegraf
