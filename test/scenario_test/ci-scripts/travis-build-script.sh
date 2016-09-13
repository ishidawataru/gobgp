#!/usr/bin/env sh

echo "travis-build-script.sh"

cd $GOBGP/test/scenario_test

sudo PYTHONPATH=$GOBGP/test python $TEST --gobgp-image $DOCKER_IMAGE -x -s
