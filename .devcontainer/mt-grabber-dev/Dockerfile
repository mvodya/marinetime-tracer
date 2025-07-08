FROM mcr.microsoft.com/devcontainers/python:1-3.11-bullseye

ENV PYTHONUNBUFFERED 1

# Add repo for pg client
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

# Update APT
RUN apt-get update

# Install Chrome Driver
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm ./google-chrome-stable_current_amd64.deb

# Install pg-client
RUN apt-get install -y postgresql-client-17

USER vscode

# Install pip requirements
COPY .devcontainer/mt-grabber-dev/requirements.txt /tmp/pip-tmp/
RUN pip3 --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt

# Install chromedriver via seleniumbase
RUN python -m sbase install chromedriver

USER root

# Clean pip tmp dir
RUN rm -rf /tmp/pip-tmp

# Clean APT
RUN apt-get clean