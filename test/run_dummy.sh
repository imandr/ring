#!/bin/sh

source ./setup.sh
python dummy.py -c nodes.yaml -n `hostname`
