#!/usr/bin/env bash

export PYTHONPATH="$(dirname $0)/lib/:$PYTHONPATH"
nosetests --with-doctest -v "$(dirname $0)/lib/sparkperf"
