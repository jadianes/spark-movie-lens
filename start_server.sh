#SPARK_HOME=~/spark-1.3.1-bin-hadoop2.6
SPARK_HOME=~/spark-1.6.1-bin-hadoop2.6

#SPARK_MASTER=spark://169.254.206.2:7077
SPARK_MASTER=spark://192.168.0.11:7077

${SPARK_HOME}/bin/spark-submit --master ${SPARK_MASTER} --total-executor-cores 14 --executor-memory 6g server.py 
