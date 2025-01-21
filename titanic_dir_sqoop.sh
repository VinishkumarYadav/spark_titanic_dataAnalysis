#!/bin/bash

current_datetime=$(date '+%Y-%m-%d_%H-%M-%S')

target_dir="/user/spark/vinish/titanic_data_sqoop/${current_datetime}_titanic_parquet"

sqoop import \
--options-file /home/test/vinish_automate_sqoop/titanic_automate/titanic_option_file.sh \
--table Titanic \
--target-dir "$target_dir" \
--delete-target-dir \
--as-parquetfile \
--m 1

# Check if the Sqoop command succeeded
if [ $? -eq 0 ]; then
    echo "Sqoop import completed successfully. Data saved to $target_dir"
else
    echo "Sqoop import failed. Please check the logs."
    exit 1
fi

