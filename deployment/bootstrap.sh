#!/bin/bash

sudo pip3 install \
  requests \
  boto3==1.23.1 \

export PYTHONPATH=.:./spark_apps:$PYTHONPATH

BUCKET=nalexx-bucket/test

sudo aws s3 cp s3://${BUCKET}/jars /usr/lib/spark/jars --recursive;