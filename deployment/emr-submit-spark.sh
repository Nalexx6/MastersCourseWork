#!/bin/bash

export PYSPARK_PIN_THREAD=true

cd /home/hadoop;

time spark-submit \
    $1
