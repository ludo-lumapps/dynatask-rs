FROM mcr.microsoft.com/devcontainers/base:trixie

RUN apt-get update && apt-get -y upgrade
RUN export DEBIAN_FRONTEND=noninteractive && apt-get -y install --no-install-recommends gdb lld valkey

USER vscode
RUN wget -qO - https://sh.rustup.rs | sh -s -- -y
RUN echo 'source $HOME/.cargo/env' >> $HOME/.bashrc