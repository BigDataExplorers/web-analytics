#!/bin/bash
#===================================================================================
#
# FILE: run_web_analytics.sh
#
# USAGE: sh run_web_analytics.sh
# DESCRIPTION: Move the log file from the current system to HDFS.
#
# REQUIREMENTS: The necessary shell scripts must be present
# BUGS: ---
# NOTES: ---
# AUTHOR: Naveen Srinivasan 
# COMPANY: Mindtree Ltd
# VERSION: 1.0
# CREATED: 02.09.2018 04:00 PM
# REVISION: 02.09.2018 04:30 PM
#===================================================================================

# Run the Ingestion Job
echo "***** RUNNING THE INGESTION JOB *****"
sh move-log-files-hdfs.sh | tee -a ../process_logs/ingestion_log.txt