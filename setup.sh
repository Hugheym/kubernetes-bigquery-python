#!/bin/bash
docker build -t gcr.io/project_id/pubsub_pipeline  .\\pubsub\\pubsub-pipe-image\\
gcloud docker -- push gcr.io/project_id/pubsub_pipeline
