#!/bin/bash
#===================================================================================
#
# FILE: move-log-files-hdfs.sh
#
# USAGE: move-log-files-hdfs.sh
# DESCRIPTION: Move the log file from the current system to HDFS.
#
# REQUIREMENTS: The log files to be in the directory in format 'mmm-dd-yyyy'
# BUGS: ---
# NOTES: ---
# AUTHOR: Naveen Srinivasan 
# COMPANY: Mindtree Ltd
# VERSION: 1.0
# CREATED: 02.09.2018 12:00 PM
# REVISION: 02.09.2018 12:30 PM
#===================================================================================

#Reading the properties file for input and output path

property_file=./app.properties

#=== FUNCTION ================================================================
# NAME: getProperty
# DESCRIPTION: return a property value for a given key.
# PARAMETER 1: ---
#===============================================================================

function getProperty {
   prop_key=$1
   prop_value=`cat $property_file | grep "$prop_key" | cut -d'=' -f2`
   echo $prop_value
}
echo ""
echo "---Reading property from $property_file ---"

log_file_path=$(getProperty "log_file_src_path")
hdfs_path=$(getProperty "log_file_hdfs_path")
echo ""
echo "============================================"
echo "Log File Path: $log_file_path"
echo "HDFS Path: $hdfs_path"
echo "============================================"
echo ""


#Check file type. If not csv, change it. Else ignore

log_file_with_ext=$( printf $log_file_path/*)
if [ ${log_file_with_ext: -4} != ".csv" ]
then
    echo "***** Changing file type to CSV *****"
    echo ""
    for file in $log_file_path/* ; do mv "$file" "${file}.csv" ; done
    log_file_with_ext=$( printf $log_file_path/*)
else
    echo "***** The file type is already CSV *****"
    echo ""
fi

log_dir_name=$(basename "$log_file_path")
hdfs_log_dir_path=$hdfs_path/$log_dir_name


#Move the file to HDFS
if hdfs dfs -test -e $hdfs_log_dir_path; then
    echo "***** The log file already exists on HDFS *****"
    echo ""

else
    echo "***** $hdfs_log_dir_path Not in HDFS ******"
    echo "***** Creating the hdfs path with date as name ******"
    hadoop fs -mkdir $hdfs_log_dir_path
    echo "***** Moving File to HDFS ******"
    hadoop fs -put $log_file_with_ext $hdfs_log_dir_path/	
    echo "File present at $hdfs_log_dir_path/"
    echo "***** Log file successfully copied to HDFS *****"
    echo ""
fi