export AIRFLOW_HOME=${PWD}/airflow
export AIRFLOW__SCHEDULER__SCHEDULE_AFTER_TASK_EXECUTION=False
export AIRFLOW__CORE__KILLED_TASK_CLEANUP_TIME=604800
airflow webserver --port 8080 -D

airflow scheduler -D