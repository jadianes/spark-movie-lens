export SPARK_MOVIE_LENS_DIR="file:/tmp"

SPARK_HOME=~/spark-1.3.1-bin-hadoop2.6

SPARK_MASTER=spark://169.254.206.2:7077

${SPARK_HOME}/bin/spark-submit --master ${SPARK_MASTER} --deploy-mode client --queue default server.py 
