#!/bin/bash

# Start the first process
/bin/bash /backend/scripts/fastapi_process.sh &
  
# Start the second process
/bin/bash /backend/scripts/wss_python_process.sh &
sleep 10

# Start the third process
/bin/bash /backend/scripts/wss_python_process_2.sh &


# Wait for any process to exit
wait -n
  
# Exit with status of process that exited first
exit $?