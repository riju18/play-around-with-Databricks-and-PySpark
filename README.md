# DataBricks and Spark

+ [Apache Spark](#apache_spark)
+ [Databricks](#databricks)

# apache_spark

+ **overview**
    + it's used for BigData processing and ML
    + spark works on top of Hadoop
    + spark processes data in memory. so that it 100x faster than map reduce
    + distributed computing platform
    + unified system which supports SQL, ML
+ **spark architecture**
    ```mermaid
    block-beta
    columns 3

    block:group1:2
        columns 2
        spark_streaming spark_ml spark_graph
    end
    spark_sql
    dataframe/or_dataset_api:3
    ```

    ```mermaid
    block-beta
    columns 3
    block:group2:2
        columns 1
        catalyst_optimizer tungsten
    end
    spark_sql_engine
    ```
    - **catalyst optimizer**: it takes care of converting a computational query to a highly efficient execution plan
    - **tungsten**: memory management and cpu efficeincy

    ```mermaid
    block-beta
    columns 3

    block:group3:2
        columns 2
        python java r scala 
    end
    spark_core
    RDD:3
    ```
+ **code example**
    + **read from csv**
        ```python
        df = spark.read \
        .option("header", True) \  # to identify header
        .option("inferSchema", True) \  # to identify datatype(slow)
        .csv(filepath)  # dataframe
        ```
    + **schema**
        + **get schema**
            ```python
            df.printSchema()  # schema of data
            df.describe().show()  # data description
            ```
        + **design schema**
            ```python
            from pyspark.sql.types import StructType,
                                            StructField,
                                            IntegreType,
                                            DoubleType,
                                            StringType

            data_schema = StructType(fields=[
                StructField("col1", IntegreType(), True),
                StructField("col2", DoubleType(), True),
                StructField("col3", StringType(), True)
            ])
            # 1st: fieldName, 2nd:dataType, 3rd:nullable
            ```
    + **data processing/query**
        + **from df**
            ```python
            from pyspark.sql.functions import col

            df1 = df.select("col1", "col2") # or
            df1 = df.select(df.col1, df.col2)  # or
            df1 = df.select(df["col1"], df["col2"]) # or
            df1 = df.select(col("col1").alias("col1_u"), col("col2"))
            ```
        
        + **rename the col**
            ```python
            df1 = df.withColumnRenamed("col_old", "col_new")
            ```
        
        + **create new col**
            ```python
            from pyspark.sql.functions import current_timestamp,lt

            df1 = df.withColumn("ingestion_date", current_timestamp()) \
            .withColumn("env", lit("prod"))  # col with sattic value
            
            # or,
            df2 = spark.createDataFrame(data=data, schema=StringType()).withColumns(
                {
                    "download_date": f.lit(datetime.today()),
                    "next_page": f.lit(next_page_api)
                }
                )
            ```
        
        + **write data**
            ```python
            df.write.mode("overwrite/append").parquet("filePath")
            ```
        
        + **prevent schema change error**
            ```python
            df.write.option("mergeSchema", "true").mode(
                    "append"
                ).saveAsTable(response_table)
            ```

        + **filtering**
            - ex #1
                ```python
                df_specific_month = df.filter(df.schedule_month == '2024-11-01')
                ```
            - ex #2: multiple cond and convert datetime to date
                ```python
                df = df.withColumn("data_download_date",
                                f.to_date(df.download_date))\
                                .drop("download_date")
                
                df = df.filter((df.data_download_date == '2024-06-07') 
                & (df.schedule_month == '2024-11-01'))
                ```
        + **union**
            ```python
            df = df.union(df2)  # must be same no of columns
            ```
        + **join**
            - **inner**
                ```python
                result = df.join(df1, 
                                df.id == df1.id, 
                                on="inner")\
                                .select(df.name.alias("df_name"), 
                                df1.name.alias("df1_name"))
                ```

            - **semi**
                > returns data only from left DF where there is a match with right DF
                ```python
                result = df.join(df1, 
                                df.id == df1.id, 
                                on="semi")\
                                .select(df.name.alias("df_name"))
                ```

            - **anti**
                > opposite of ```semi``` data only from left DF where there is no match with right DF
                ```python
                result = df.join(df1, 
                                df.id == df1.id, 
                                on="anti")\
                                .select(df.name.alias("df_name"))
                ```
        + **Aggregation**
            + **get max date**
                ```python
                import pyspark.sql.functions as f

                df = df.select(
                    f.to_date(f.max("download_date"))
                    .alias("max_date")
                    .cast("date")
                    )

                print(df.colletct()[0][0])  # 2024-05-15
                ```
            
            + **count**
                ```python
                import pyspark.sql.functions as f

                df = df.select(f.countDistinct("schedule_month").alias("distinct_date"), f.count("schedule_month"))
                
                df.display()
                ```
            
            + **group**
                ```python
                import pyspark.sql.functions as f

                data_count_group = df.groupBy("schedule_month")\
                .agg(f.count("schedule_month").alias("total_downloaded"))\
                .sort("schedule_month",ascending=False)
                
                data_count_group.display()
                ```
            
            + **rank**  
                ```python
                from pyspark.sql.functions import desc, rank
                from pyspark.sql.window import Window

                data_count = Window.partitionBy("schedule_month")\
                .orderBy(desc("schedule_month"))
                data_count_rank = df.withColumn("rank", rank()\
                .over(data_count))
                
                display(data_count_rank)
                ```



# databricks

+ **ecosystem**
    ```mermaid
    flowchart LR
    databricks --> apache_spark
    apache_spark --> clusters
    apache_spark --> workspace/notebook
    apache_spark --> admin_controls
    apache_spark --> optimized_spark_5x_faster
    apache_spark --> databases/tables
    apache_spark --> delta_lake
    apache_spark --> sql_analytics
    apache_spark --> mlflow 
    ```
+ **cluster**
    ```mermaid
    flowchart LR

    master_node --> worker_node1
    master_node --> worker_node2
    master_node --> worker_node3
    ```
    + **cluster types**
        | all purpose       | job cluster |
        | ---------------   | ------------|
        | created manually  | created by jobs    |
        | persistent        | terminated at the end of job|
        | good of interective workloads | automated|
        | shared among many users | isolated just for job|
        | expensive | cheaper |
    
    + **cluster config**
        - single/muti node
        - access mode
        - databricks runtime
            ```mermaid
            flowchart LR
            databricks_runtime --> spark
            databricks_runtime --> scala_python_java_r
            databricks_runtime --> ubuntu_libraries
            databricks_runtime --> gpu_libraries
            databricks_runtime --> delta_lake

            databricks_runtime_ml --> databricks_runtime_
            databricks_runtime_ml --> popular_ml_libraries

            photon_runtime --> _databricks_runtime
            photon_runtime --> photon_engine(photon engine: native vectorized query engine runs SQL workloads faster & reduces cost per workload)

            databricks_runtime_light --> light(runtime option for only jobs not requiring advamced features)
            ```
        - auto teermination
        - auto scaling
        - cluster vm type/size
        - cluster policy
        - cluster permission
            - **can attach permission on cluster**:
                > This most **granular** level permission allows a user to attach notebooks or jobs to a cluster. The user can run code on the cluster but cannot modify or terminate the cluster itself. Essentially, **it enables the user to use the cluster for their computations.**
            
            - **can restart permission on cluster**:
                > This permission allows a user to restart the cluster. Restarting a cluster can be necessary to apply configuration changes or resolve issues. **The user cannot change other settings or terminate the cluster but can refresh it by restarting.**
            
            - **can manage permission on cluster**:
                > This **highest** permission is the most powerful of the three. It allows the user to perform all administrative tasks on the cluster, including creating, editing, restarting, and terminating the cluster. Additionally, the user can manage the cluster's permissions, giving them control over who else can access and modify the cluster.

+ **catalog**
    - create permission
        ```sql
        %sql
        GRANT 
            CREATE SCHEMA, 
            CREATE TABLE, 
            USE CATALOG 
        ON CATALOG catalog_name 
        TO `user_mail` 
        ;
        ```
    - revoke permission
        ```sql
        %sql
        REVOKE 
        CREATE SCHEMA,
        CREATE TABLE
        ON CATALOG samrat_big_data_ecosystem_workspace
        FROM `user_mail`
        ;
        ```
+ **databricks utilities**
    - file system utilities
        ```sh
        %fs
        ls  # list of contents in root folder

        dbutils.fs.ls('/')  # list of files in dir

        dbutils.help()  # all available dbutils cmd
        dbutils.fs.help()  # all available dbutils file cmd
        ```
    - secrets utilities
        - get scope
            ```python
            dbutils.secrets.listScopes()   # list of scopes
            ```
        - get secret list
            ```pyhton
            dbutils.secrets.list('scope-name')
            ```
    - widget utilities
        - set parameter
            ```sh
                dbutils.widgets.text("months_itterate", "2")
            ```
        - get parameter
            ```sh
            months_iterate = dbutils.widgets.get("months_itterate")
            ```
    - notebook workflow utilities
        - include a child notebook
            ```python
            %run% notbook_name
            ```
        - run a notebook passing parameters
            - parent notebook
                ```python
                notebook_result = dbutils.notebook.run("notbook_file", 
                0, 
                {
                    "key": value
                })
                # 1st: notbook_name, 2nd: timeout, 3rd: map
                ```
            - child notebook
                ```python
                dbutils.notebook.exit("Success")  # param: value
                ```
        - job
            
