FROM mcr.microsoft.com/devcontainers/typescript-node:1-20-bookworm

# Add repo for pg client
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

# Update APT
RUN apt-get update

# Install pg-client
RUN apt-get install -y postgresql-client-17

# Clean APT
RUN apt-get clean