#!/bin/bash

# Start the first process
/bin/bash /trading/scripts/wss-python.sh &
  
# Start the second process
#/bin/bash /tracker/scripts/python-process-2.sh &

# Wait for any process to exit
wait -n
  
# Exit with status of process that exited first
exit $?