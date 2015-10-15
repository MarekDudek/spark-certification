#!/usr/bin/env bash

THIS_DIR=`dirname $(readlink -f $0)`
source ${THIS_DIR}/common.sh

spark-submit --master local --class interretis.intro.PiMonteCarlo ${ARTIFACT_PATH} $@