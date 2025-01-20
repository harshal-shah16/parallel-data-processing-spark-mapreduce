##
Spark - Follower Count (Twitter dataset)

Installation
------------
These components are installed:
- JDK 1.8 (Azul Zulu OpenJDK)
- Scala 2.12.14
- Hadoop 3.3.1
- Spark 3.1.2 (without bundled Hadoop)
- Maven
- AWS CLI (for EMR execution)

Environment
-----------
1) Example ~/.bash_aliases:
   export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home
   export HADOOP_HOME=/opt/homebrew/Cellar/hadoop/3.3.1/libexec
   export PATH=$PATH:$HADOOP_HOME/bin
   export PATH=$PATH:$HADOOP_HOME/sbin
   export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
   export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
   export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
   export HADOOP_MAPRED_HOME=$HADOOP_HOME
   export HADOOP_COMMON_HOME=$HADOOP_HOME
   export HADOOP_HDFS_HOME=$HADOOP_HOME
   export YARN_HOME=$HADOOP_HOME
   export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
   export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
   export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
   export SCALA_HOME=/opt/homebrew/Cellar/scala@2.12/2.12.14
   export SPARK_HOME=/opt/homebrew/Cellar/apache-spark/3.1.2/libexec
   export PATH=$PATH:$SPARK_HOME/bin
   export PATH=$PATH:$SCALA_HOME/bin
   export SPARK_DIST_CLASSPATH=$(hadoop classpath)


2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:
   export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home

3) Explicitly set SPARK_DIST_CLASSPATH in $SPARK_HOME/conf/spark-env.sh
   export SPARK_DIST_CLASSPATH=$(hadoop classpath)

Execution
---------
All of the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
    Sufficient for standalone: hadoop.root, jar.name, local.input
    Other defaults acceptable for running standalone.
5) Standalone Spark:
    Add .setMaster("local[*]") to configuration
    eg. val conf = new SparkConf().setAppName("Follower Count").setMaster("local[*]")
    make switch-standalone		-- set standalone Hadoop environment (execute once)
    make local
6) Pseudo-Distributed Spark: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
    Remove .setMaster("local[*]") from configuration
    eg. val conf = new SparkConf().setAppName("Follower Count")
    make switch-pseudo			-- set pseudo-clustered Hadoop environment (execute once)
    make pseudo					-- first execution
    make pseudoq				-- later executions since namenode and datanode already running 
7) AWS EMR Spark: (you must configure the emr.* config parameters at top of Makefile)
    Remove .setMaster("local[*]") from configuration
    eg. val conf = new SparkConf().setAppName("Follower Count")
    make upload-input-aws		-- only before first execution
    make aws					-- check for successful execution with web interface (aws.amazon.com)
    download-output-aws			-- after successful execution & termination
