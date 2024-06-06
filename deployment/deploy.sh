#!/usr/bin/env bash

NAMESPACE=$1
PROJECT=etl-helm

cd $PROJECT || exit 1

helm dependency update && helm upgrade $PROJECT . --namespace $NAMESPACE --install --debug --timeout 18m