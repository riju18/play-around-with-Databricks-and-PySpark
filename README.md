# DataBricks and Spark

+ [Apache Spark](#apache_spark)
+ [Databricks](#databricks)
+ [Security](#security)
+ [Get data](#get_data)
+ [Install Packages](#install_package)
+ [Delta vs. Parquet](#delta_vs_parquet)
+ [Delta table time travel](#time_travel)
+ [Databricks widgets](#dbc_widgets)
+ [Databricks Aggregation](#dbc_aggregation)
+ [Databricks filesystem](#dbfs)
+ [Save data as parquet](#parquet)
+ [SQL struct and explode](#struct_explode)
+ [Temporary View](#temp_view)
+ [unity catalog](#unity_catalog)

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

+ **view**
    + **temp**
        > Only available in spark session(current notebook)
    + **global**
        > Available in all notebooks attached to same cluster
+ **code example**
    + **get data**
        - from csv
            ```python
            df = spark.read \
            .option("header", True) \  # to identify header
            .option("inferSchema", True) \  # to identify datatype(slow)
            .csv(filepath)  # dataframe
            ```
        - from table 
            ```python
            sql_code = "select * from schema_name.table_name" 
            df = spark.sql(sql_code)
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
            .withColumn("env", lit("prod"))  # col with static value
            
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

            # or,
            df.write.option('mergeSchema', "true") \ 
            .mode("append") \ 
            .saveAsTable("schema_name.table_name")
            ```
            different sets of example:

            ```python
            df.write \
                .format("parquet") \  # Specify the format (e.g., "parquet", "csv", "json", "orc", "jdbc", "delta")
                .option("path", "/path/to/save") \  # Specify the path to save the data
                .option("mode", "append") \  # Specify the write mode (e.g., "append", "overwrite", "ignore", "error" or "errorifexists")
                .option("partitionBy", "column_name") \  # Specify the column(s) to partition the data by
                .option("bucketBy", 4, "column_name") \  # Specify the number of buckets and the column to bucket by
                .option("sortBy", "column_name") \  # Specify the column to sort by within each bucket
                .option("compression", "snappy") \  # Specify the compression codec (e.g., "none", "bzip2", "gzip", "lz4", "snappy", "deflate")
                .option("mergeSchema", True) \  # Specify whether to merge schemas
                .option("pathGlobFilter", "*.parquet") \  # Specify a glob pattern to filter the files
                .option("recursiveFileLookup", "true") \  # Specify whether to recursively look up files
                .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss") \  # Specify the timestamp format
                .option("dateFormat", "yyyy-MM-dd") \  # Specify the date format
                .option("nullValue", "NULL") \  # Specify the string representation of null values
                .save("/path/to/save")  # Specify the path to save the data 
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
        - auto termination
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
+ **schema**
    - schema info
        ```sql
        %sql
        show grant on schema schema_name;
        ```
    - create table permission
        ```sql
        %sql
        GRANT 
            CREATE TABLE 
        ON SCHEMA schema_name 
        TO `user_mail`
        ;
        ```
    - select table permission
        ```sql
        %sql
        GRANT 
            SELECT 
        ON TABLE workspace_name.schema_name.table_name TO `user_mail`;
        ```
    
    - to prevent drop col is not supported for delta table 
        ```sql
        ALTER TABLE schema_name.table_name 
        SET TBLPROPERTIES (
            'delta.minReaderVersion' = '2',
            'delta.minWriterVersion' = '5',
            'delta.columnMapping.mode' = 'name'
        );

        ALTER TABLE schema_name.table_name 
        DROP COLUMN data_ingestion_time;
        ```
    
    - add & set col default value 
        ```sql
        ALTER TABLE schema_name.table_name 
        SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled'); 

        ALTER TABLE schema_name.table_name 
        ALTER COLUMN col_name SET DEFAULT current_timestamp();
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

# security

- **design**
    ```mermaid
    flowchart TB

    notebook_cluster_job <--> dbc_secret_scope <--> azure_key_vault_aws_or_aws_secret_manager_or_GCP
    ```

- Create secret key

    1. create scope

        ```python
         import requests
         import json

         # Secret scope name you want to create
        scope_name = 'aws_secret_scope'

        # Replace these variables with your Databricks workspace information
        databricks_instance = 'https://dbc-123456-a1c3.cloud.databricks.com'
        databricks_token = 'generated token'

        # Headers for the API request
        headers = {
            'Authorization': f'Bearer {databricks_token}',
            'Content-Type': 'application/json'
        }

        # create scope
        # Payload for creating a secret scope
        payload = {
            'scope': scope_name,
            'initial_manage_principal': 'users'  # Initial managing principal. Can be 'users' or 'admins'
        }

        # Databricks API endpoint for creating a secret scope
        url = f'{databricks_instance}/api/2.0/secrets/scopes/create'

        # Make the API request to create the secret scope
        response = requests.post(url, headers=headers, data=json.dumps(payload))

        # Check the response
        if response.status_code == 200:
            print(f'Successfully created secret scope: {scope_name}')
        else:
            print(f'Failed to create secret scope: {response.status_code}')
            print(response.json())

        ```
    
    2. create secret 

        ```python
        secret_key_payload = {
            "scope": "aws_secret_scope",
            "key": "secret_key",
            "string_value": "aws s3 secret key"
        }

        access_key_payload = {
            "scope": "aws_secret_scope",
            "key": "access_key",
            "string_value": "aws s3 access key"
        }

        url_payload = {
            "scope": "aws_secret_scope",
            "key": "s3_bucket_endpoint",
            "string_value": "s3.ap-southeast-1.amazonaws.com"
        }

        # Databricks API endpoint for creating a secret scope
        url = f'{databricks_instance}/api/2.0/secrets/put'

        # Make the API request to create the secret key in secret scope
        secret_key_response = requests.post(url, 
                                            headers=headers,
                                            data=json.dumps(secret_key_payload)
                                            )

        access_key_response = requests.post(url, 
                                            headers=headers,
                                            data=json.dumps(access_key_payload)
                                            )

        url_payload_response = requests.post(url, 
                                            headers=headers,
                                            data=json.dumps(url_payload)
                                            )

        # Check the response
        if secret_key_response.status_code == 200:
            print(f'Successfully added secret key')
        else:
            print(f'Failed to create secret scope: {secret_key_response.status_code}')
            print(secret_key_response.json())

        if access_key_response.status_code == 200:
            print(f'Successfully added access key')
        else:
            print(f'Failed to create access key: {access_key_response.status_code}')
            print(access_key_response.json())

        if url_payload_response.status_code == 200:
            print(f'Successfully added url endpoint')
        else:
            print(f'Failed to create url endpoint: {url_payload_response.status_code}')
            print(url_payload_response.json())

        ```
    
    3. dbc secret key methods

        - dbutils.secrets.help()

        - list of scope: 
            ```python
            dbutils.secrets.listScopes()
            ```
        
        - specific secret 
            ```python
            dbutils.secrets.list("aws_secret_scope") 
            ```
        
        - get the secret key 
            ```python
            dbutils.secrets.get('aws_secret_scope', 'secret_key')
            ```
    
    4. api list for secret scope: [**click here**](https://docs.databricks.com/api/workspace/secrets)

# get_data

- **s3**
    1. set the env var in databricks cluster
    2. execute the code
        ```python
        import os

        access_key = os.getenv("access_key")
        secret_key = os.getenv("secret_key")
        s3_endpoint = os.getenv("s3_endpoint")

        spark.conf.set("fs.s3a.access.key", access_key)
        spark.conf.set("fs.s3a.secret.key", secret_key)
        spark.conf.set("fs.s3a.endpoint", s3_endpoint)  # s3.ap-southeast-1.amazonaws.com

        df = spark.read.option("header", "true") \
            .csv("s3://samrat-sample-fintech-data-temp/financial_sample_data.csv")

        display(df.limit(10))
        ```

# install_package

- using notebook 
    1. upload `requirements.txt` in dbfs:

        a. navigate to `data ingestion`

        b. from `add data` field 
            - select `upload file to DBFS`

    2. install dependencies
        ```python
        # Install the packages listed in requirements.txt
        dbfs.fs.ls("/")  # get all root path

        dbutils.fs.cp("dbfs file path", "file:/tmp/requirements.txt")

        # Use pip to install the packages
        %pip install -r /tmp/requirements.txt
        ```
    
    3. restart kernel
        ```sh
        %restart_python
        ```
        or 

        ```python
        dbutils.library.restartPython()
        ```

- using `compute`

    a. navigate to `Compute`

    b. select `cluster` 
    
    c. select libraries 
    
    d. click `install new`
    
    e. select `workspace`
    
    f. click `install`

# delta_vs_parquet

- Delta Lake
    - **ACID Transactions**: Delta Lake provides ACID (Atomicity, Consistency, Isolation, Durability) transaction guarantees, which means it can handle concurrent read and write operations without data corruption.

    - **Schema Evolution**: Delta Lake supports schema evolution, allowing you to change the schema of your data without breaking existing queries.

    - **Time Travel**: Delta Lake allows you to query previous versions of your data, which is useful for auditing and debugging.

    - **Data Lineage**: Delta Lake maintains a transaction log that records all changes to the data, providing a full history of operations.

    - **Optimized Performance**: Delta Lake includes optimizations like data skipping and Z-order indexing to improve query performance.

- Parquet
    - **Columnar Storage**: Parquet is a columnar storage format that is optimized for read-heavy operations, making it efficient for analytical queries.

    - **Compression**: Parquet supports various compression algorithms, which can reduce storage costs and improve I/O performance.

    - **Schema Enforcement**: Parquet enforces a schema, but it does not support schema evolution as seamlessly as Delta Lake.

    - **No ACID Transactions**: Parquet does not provide ACID transaction guarantees, which can lead to data consistency issues in concurrent environments.

    - **No Time Travel**: Parquet does not support querying previous versions of the data.

- Recommendation
    >
    > Databricks recommends using Delta Lake instead of Parquet for most use cases due to its additional features and optimizations.
    >
# time_travel

- **Table history**
    ```sql
    describe history scehma.table_name
    ```
- **time travel**
    ```sql
    select 
        * 
    from 
        schema_name.table_name 
    TIMESTAMP AS OF '2025-01-06 08:18:07' limit 10;
    ```

# dbc_widgets

- **widgets info**
    ```python
    display(dbutils.widgets.help())
    ```

- **set widget**
    ```python
    dbutils.widgets.text("username", spark.sql("SELECT current_user() AS user").first()[0])
    ```

- **get value from widget**
    ```python
    get_user = dbutils.widgets.get("username")
    ```

# dbc_aggregation

- **min/max**
    ```python
    max_year = df.agg({'year': 'max'}).collect()
    min_year = df.agg({'year': 'min'}).collect()
    ```

- **grouping**

    ```pyhton
    from pyspark.sql.functions import sum, count
    ```

    1. group by col
        ```python
        grouping_by_yr = df.groupBy('year')\
        .agg(sum('value').alias('total_value'))\
        .limit(10)
        ```
    2. count
        ```python
        no_of_rows_by_date = df.withColumn('bronze_data_ingestion_date', df['bronze_data_ingestion_date'].cast('date'))\
        .groupBy('bronze_data_ingestion_date')\
        .agg((count('stg').cast('float')).alias('total_no_of_rows'))\
        .orderBy('bronze_data_ingestion_date')
        ```

# dbfs

+ **get filelist**
    ```python
    dbutils.fs.ls('/')
    ```

+ **create dir**
    ```python
    dbutils.fs.mkdirs('/data_lake/finance/bronze')
    ```

+ **remove dir**
    ```python
    dbutils.fs.rm('/data_lake/finance/bronze', recurse=True)
    ```

# parquet

- save dataframe as parquet
    ```python
    df = df.withColumn("bronze_data_ingestion_date", to_date("bronze_data_ingestion_date"))

    df.write.format("parquet") \
        .option("mergeSchema", True) \
        .mode("append") \
        .option("sortBy", "year") \
        .partitionBy("bronze_data_ingestion_date") \
        .save(path)
    ```

# struct_explode

- real example
    ```sql

        %sql
        with cte as (
        select
            cast(bronze_data_ingestion_date as date) as bronze_data_ingestion_date
            , year
            , count(distinct industry_aggregation_nzsioc) as total_unique_industry_nzsioc
            , count(distinct industry_code_nzsioc) as total_unique_industry_code_nzsioc
            , count(distinct industry_name_nzsioc) as total_unique_industry_name_nzsioc
            , count(distinct units) as unique_units
            , count(distinct variable_code) as unique_var_codes
            , count(distinct variable_name) as unique_var_names
            , count(distinct variable_category) as unique_variable_category
            , count(distinct industry_code_anzsic06) as unique_industry_code_anzsic06
        from 
            bronze.financial_raw_data
        group by 1,2
        ),
        convert_to_struct as (  -- sql struct to get rid of redundant rows for same value
        select
            bronze_data_ingestion_date
            , struct(  -- make it a struct data type
            array_agg(year) as year
            , array_agg(total_unique_industry_nzsioc) as unique_industry_aggregation_nzsioc
            , array_agg(total_unique_industry_code_nzsioc) as industry_code_nzsioc
            , array_agg(total_unique_industry_name_nzsioc) as industry_name_nzsioc
            , array_agg(unique_units) as units
            , array_agg(unique_var_codes) as var_codes
            , array_agg(unique_var_names) as var_names
            , array_agg(unique_variable_category) as variable_category
            , array_agg(unique_industry_code_anzsic06) as industry_code_anzsic06
            ) as struct_data
        from
        cte
        group by 1
        ),
        exploding as (  -- extract the array value
        select
            bronze_data_ingestion_date
            , explode(struct_data.year) as yr 
            , explode(struct_data.unique_industry_aggregation_nzsioc) as unique_industry_aggregation_nzsioc
        from
            convert_to_struct
        )
        select
        bronze_data_ingestion_date
        , yr
        , sum(unique_industry_aggregation_nzsioc) as total_val
        from
        exploding
        group by 1,2 
        ;
    ```

# temp_view

- **create temp view** 

    ```python
        df.createOrReplaceTempView('df_tmp_view') 
    ```

    ```sql 
        %sql
        select * from df_tmp_view 
    ```

- **create global temp view**

    ```python
    df.createOrReplaceGlobalTempView('df_global_tmp_view') 
    ```

    ```sql 
        %sql
        select * from global_temp.df_global_tmp_view
    ```

- **create permanent view**
    
    ```python
    spark.sql("""
          create or replace view default.df_view 
          as select * from bronze.financial_raw_data 
          limit 10
        """)
    ```

    ```sql 
        %sql
        select * from default.df_view
    ```


# unity_catalog

>
> Unity Catalog is a unified governance solution for all data and AI assets in Databricks. It provides a centralized metadata store and a three-level namespace (catalog.schema.table) for organizing and managing data assets such as tables, views, volumes, models, and functions. Unity Catalog operates on the principle of least privilege, ensuring users have the minimum access necessary to perform their tasks. It supports familiar ANSI SQL syntax for creating and managing database objects and integrates with Delta Sharing and Databricks Marketplace for secure data sharing and exchange
> 