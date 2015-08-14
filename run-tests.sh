#!/usr/bin/env bash

export PYTHONPATH="$(dirname "$0")/lib/:$HOME/lib/site-python:$PYTHONPATH"
/usr/bin/env python -c "from nose import main; main()" --with-doctest -v "$(dirname "$0")/lib/sparkperf"
