# Set the base image 
FROM python:3.11-alpine

# Adds metadata to image.
LABEL maintainer="Laurent"

# dont write pyc files
ENV PYTHONDONTWRITEBYTECODE 1
# dont buffer to stdout/stderr
ENV PYTHONUNBUFFERED 1

# Sets the user name to use when running the image.
USER root
RUN apk update
RUN apk add --no-cache curl
RUN apk add --no-cache bash

# Make a directory for our app
WORKDIR /producer

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy source code
COPY ./app ./app

# Run the application
CMD ["bash"]
# CMD ["python", "app/producer.py","velib-stations"]

# CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
