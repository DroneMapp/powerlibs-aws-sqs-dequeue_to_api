#!/bin/bash

set -e
RET=0

pip uninstall -y $(basename $PWD) || echo "Could not uninstall."
pip install -U "git+file://$PWD"
mv powerlibs x
PYTHONPATH=. pytest tests/ || RET=$?
mv x powerlibs

exit $RET
