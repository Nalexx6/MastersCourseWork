#!/bin/bash

sudo pip3 install \
  requests \
  boto3==1.23.1 \

export PYTHONPATH=.:./spark_apps:$PYTHONPATH
