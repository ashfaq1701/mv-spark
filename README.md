# Sale Records Summary Project With Apache Spark


This project reads raw sale records from csv files and produces three summaries. summary_base, summary_by_state and summary_over_time.


### Prerequisites


*  Install docker-hive
   
   This project requires apache hive to store raw sale records. For this we need to run the below docker container.
   
   ```
   git clone https://github.com/big-data-europe/docker-hive.git
   cd docker-hive
   docker-compose up -d
   ```

   
   Then we need to set the internal hostnames of those docker containers. For this,

   
   ```
   docker exec -ti docker-hive-master_hive-metastore_1 bash
   ping hive-metastore
   ping hive-metastore-postgresql
   ping namenode
   ```

   And then record each ip addresses and update `\etc\hosts` like below,


   ```
   172.18.0.x  hive-metastore
   172.18.0.y  hive-metastore-postgresql
   172.18.0.z  namenode
   ```

*  Configure mysql parameters.

   This project connects with a mysql database. To make this process configurable for all computers, I used an external file to store mysql config. Mysql configuration has to be kept in `/var/conf/mysql-conf.xml`

   ```
   <conf>
        <host>localhost</host>
        <port>3306</port>
        <database>mv_summary</database>
        <user>xxxx</user>
        <password>xxxxxxx</password>
   </conf>
   ```

*  Create a folder in your home directory called `dump_data`.
   
   ```
   mkdir $HOME\dump_data
   mkdir $HOME\dump_data.salerecords
   ```

   In `$HOME\dump_data` store two files. `ymmt_ids.csv` and `zipcodes.csv`. These files will be imported as `ymmt_ids` and `zipcodes tables`. Inside `$HOME\dump_data\salerecords\` put any number of sale record 
   dump files as this directories will be scanned in each import.


*  To run this project install Java and Maven. Then from project root folder run the below command to build this project.


   ```
   mvn clean package
   ```


*  As this project runs multiple actions, you can run the jar with the below parameters.

   ```
   java -jar target/SummarySaleRecords-0.0.1-SNAPSHOT-jar-with-dependencies.jar import
   ```

   This will import ymmt_ids, zipcodes and all salerecord files in the specific folder in hive. This project will store the hive data warehouse in $HOME/hive-warehouse.

   ```
   java -jar target/SummarySaleRecords-0.0.1-SNAPSHOT-jar-with-dependencies.jar summary #outlier_mode#
   ```

   This will summarize the data and export to mysql. #outlier_mode# will contain the outlier detection mechanism we want to use. I implemented three mechanisms here.


   *  z_score: This will be used in default if you don't specify any outlier_mode. This is less accurate as it doesn't detect any outlier when we have less than 12 datapoint in a dataset. But it is fastest.
   *  cooks_distance: (Recommended) This is the most accurate one due to the sophisticated algorithm used and out data nature of uneven distribution. This is much slower than z_score but is in our time bound. It 
      also requires high memory.
   *  mod_z_score: This is of similar accuracy of cooks_distance. But it is the most expensive one. Takes very long time and space because of it's requirement of median data.

   
   ```
   java -jar target/SummarySaleRecords-0.0.1-SNAPSHOT-jar-with-dependencies.jar delete_partitions
   ```

   This will delete partitions so salerecords older than 6 months which we don't need anymore.

   
   ```
   java -jar target/SummarySaleRecords-0.0.1-SNAPSHOT-jar-with-dependencies.jar delete
   ```

   This will delete the complete salerecords table from hive. But will keep zipcodes and ymmt_ids.


   ```
   java -jar target/SummarySaleRecords-0.0.1-SNAPSHOT-jar-with-dependencies.jar delete_database
   ```

   This will delete the complete hive database from the system.


*  By default this app will store the summaries very quickly in three tables. `summary_base_new`, `summary_by_state_new` and `summary_over_time_new`. These tables don't have any primary key and are inefficient 
   for PHP. Schema for `summary_base`, `summary_by_state` and `summary_over_time` are given in the php repository of this project. After import complete we have to run the below queries to fill up those tables 
   with fresh summaries.

   
   ```
   TRUNCATE summary_base;
   TRUNCATE summary_by_state;
   TRUNCATE summary_over_time;
   INSERT INTO summary_base SELECT * FROM summary_base_new;
   INSERT INTO summary_by_state SELECT * FROM summary_by_state_new;
   INSERT INTO summary_over_time SELECT * FROM summary_over_time_new;
   ```


   As summary always merge old data with new data, though we are truncating tables, we will lose no data at all. For serving summary PHP used the copied tables.
