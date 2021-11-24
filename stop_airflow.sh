export AIRFLOW_HOME=${PWD}/airflow
cat $AIRFLOW_HOME/airflow-webserver.pid | xargs kill -9
cat $AIRFLOW_HOME/airflow-scheduler.pid | xargs kill -9
cat $AIRFLOW_HOME/airflow-webserver-monitor.pid | xargs kill -9

rm -f $AIRFLOW_HOME/airflow-webserver.pid
rm -f $AIRFLOW_HOME/airflow-scheduler.pid
rm -f $AIRFLOW_HOME/airflow-webserver-monitor.pid
rm -f $AIRFLOW_HOME/airflow-webserver.log
rm -f $AIRFLOW_HOME/airflow-scheduler.log
rm -f $AIRFLOW_HOME/airflow-webserver.err
rm -f $AIRFLOW_HOME/airflow-scheduler.err
rm -f $AIRFLOW_HOME/airflow-webserver.out
rm -f $AIRFLOW_HOME/airflow-scheduler.out
