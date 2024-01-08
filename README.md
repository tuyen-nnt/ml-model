

### Build the app using docker-compose up command in your current project path, you can pass the environment variable from the command line:

When you run docker-compose up, it typically builds the Docker images defined in your docker-compose.yml file (if the images do not already exist) and starts the corresponding containers. The images are created based on the instructions provided in the associated Dockerfiles.
```
docker-compose up --build --env FEATURE_COLUMN="YourFeatureColumnName" --env TARGET_COLUMN="YourTargetColumnName"
```

### Check the image
```
docker-compose images
```

Remember that Docker Compose caches the images, so if you make changes to your application or Dockerfile, you might need to rebuild the images using:
```
docker-compose build
```
