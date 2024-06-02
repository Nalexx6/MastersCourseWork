#!/usr/bin/env bash

NAMESPACE=$1
PROJECT=etl-helm

cd $PROJECT || exit 1

helm uninstall -n $NAMESPACE $PROJECT