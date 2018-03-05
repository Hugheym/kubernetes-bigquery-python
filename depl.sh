#!/bin/bash
kubectl create -f ./pubsub/bigquery-controller.yaml
kubectl create -f ./pubsub/twitter-stream.yaml
kubectl get all