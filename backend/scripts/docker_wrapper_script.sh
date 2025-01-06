#!/bin/bash

# Start the first process
/bin/bash /backend/scripts/fastapi_process.sh &
echo 'ANALYSIS: ' $ANALYSIS
  
# Start the second process
if [ $ANALYSIS = 1 ]
then
    echo 'Analysis Mode On'
    /bin/bash /backend/scripts/analysis_mode.sh &
else
    /bin/bash /backend/scripts/wss_python_process.sh &
    sleep 5

    # Start the third process
    /bin/bash /backend/scripts/wss_python_process_2.sh &
#     sleep 5
#     /bin/bash /backend/scripts/wss_python_process_3.sh &
#     sleep 5
#     /bin/bash /backend/scripts/wss_python_process_4.sh &
fi


# Wait for any process to exit
wait -n
  
# Exit with status of process that exited first
exit $?