#!/usr/bin/env bash


spark-submit --class interretis.CharacterCount --master local[4] ./target/scala-2.10/training-project-for-spark-certification_2.10-1.0.jar $@