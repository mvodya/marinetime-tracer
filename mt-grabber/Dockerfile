FROM ubuntu:24.04

ENV PYTHONUNBUFFERED 1

# Get APT lists
RUN apt-get update

# Install python & required util packages
RUN apt-get install -y wget software-properties-common python3-pip python3-venv

# Install Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get install -y ./google-chrome-stable_current_amd64.deb && \
    rm ./google-chrome-stable_current_amd64.deb

WORKDIR /workspace

# Create venv
RUN python3 -m venv /workspace/venv

# Install python requirements.txt
COPY requirements.txt /workspace/requirements.txt
RUN /workspace/venv/bin/pip install --no-cache-dir -r /workspace/requirements.txt

# Install ChromeDriver via seleniumbase
RUN /workspace/venv/bin/pip install seleniumbase && \
    /workspace/venv/bin/python -m sbase install chromedriver

# Copy src & setup directories
RUN mkdir /workspace/archive
COPY positions.json /workspace
COPY script.sh /workspace
COPY collector.py /workspace
COPY upload.py /workspace
RUN chmod +x /workspace/script.sh

# Clean pip tmp dir
RUN rm -rf /tmp/pip-tmp

# Clean APT
RUN apt-get clean

# Start script
CMD ["/workspace/script.sh"]