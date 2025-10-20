FROM mcr.microsoft.com/devcontainers/base:bookworm

RUN apt-get update && apt-get -y upgrade
RUN export DEBIAN_FRONTEND=noninteractive && apt-get -y install --no-install-recommends gdb lld redis-tools