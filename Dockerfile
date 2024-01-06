# Use an official Python runtime as a parent image
FROM python:3.8

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
# check package version ``pip freeze``

# Set default values for environment variables
ENV FEATURE_COLUMN=request \
    TARGET_COLUMN=response

# Run app.py when the container launches
CMD ["python", "main.py"]

# docker build -t your_image_name .
# docker run -e FEATURE_COLUMN=value1 -e TARGET_COLUMN=value2 your_image_name
