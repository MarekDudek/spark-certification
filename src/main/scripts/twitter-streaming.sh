#!/usr/bin/env bash

THIS_DIR=`dirname $(readlink -f $0)`
source ${THIS_DIR}/common.sh

spark-submit --master local[2] \
	--class interretis.advanced.streaming.TwitterStreaming \
	--packages "org.apache.spark:spark-streaming-twitter_2.10:1.5.1" \
	${ARTIFACT_PATH} $@