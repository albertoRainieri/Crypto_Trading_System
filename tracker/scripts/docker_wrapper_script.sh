#!/bin/bash

if [ $ANALYSIS = 1 ]
then
    echo 'Analysis Mode On. Switch to Analysis = 1'
    exit 0
fi

# Start the first process
/bin/bash /tracker/scripts/python-process-1.sh &
  
# Start the second process
/bin/bash /tracker/scripts/python-process-2.sh &

# start the third process
python3 /tracker/trading/wss-pool-order-book.py &

# Wait for any process to exit
wait -n
  
# Exit with status of process that exited first
exit $?