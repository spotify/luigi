#!/bin/sh
# sets up pythonpath and runs tests.

PYTHONPATH=`dirname $0` test/test.py
