FROM yiwen/spark:v3.5.1-py3

WORKDIR /app

# Switch to root to perform all system and package installation steps
USER root

# Install system dependencies needed for PostgreSQL's psycopg2 (libpq-dev)
RUN apt-get update && \
    apt-get install -y libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY ./apps/requirements.txt .

# Install Python dependencies as root to guarantee write access, resolving the [Errno 13] Permission denied error.
# We no longer use --user, as we are running as root.
RUN pip install --no-cache-dir -r requirements.txt

# Switch back to the non-root 'spark' user for running the application 
# (crucial for security and Spark operation)
USER spark

# Copy the rest of your application scripts
COPY ./apps/ .

# CMD is not explicitly defined as it is set by the docker-compose.yml file.
