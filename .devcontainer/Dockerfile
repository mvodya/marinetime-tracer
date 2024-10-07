FROM mcr.microsoft.com/devcontainers/python:1-3.11-bullseye

ENV PYTHONUNBUFFERED 1

RUN apt-get update

# Install Chrome Driver
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm ./google-chrome-stable_current_amd64.deb

USER vscode

# Install pip requirements
COPY requirements.txt /tmp/pip-tmp/
RUN pip3 --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt
RUN pip install pylance==0.18.2 jupyter==1.1.1 matplotlib==3.9.2

# Install chromedriver via seleniumbase
RUN python -m sbase install chromedriver

USER root

# Clean pip tmp dir
RUN rm -rf /tmp/pip-tmp

RUN apt-get clean