## Data quality on Databricks

### Introduction
In this blog post, we will reveal a topic of Data Quality from both theoretical and practical implementation using the Databricks platform.
The primary focus would on end to end implementation to showcase platform capabilities, that will include profiling, monitoring and alerting.

### Definition of Data Quality
The definition of Data Quality given in many resources, can be summarised measurement of how well the data represents real-world facts.
This is usually done though the number of checks or measurements in different dimensions such as:
- _Accuracy_ — whether data comply syntactic and semantic rules.
- _Completeness_ - whether expected data is present in full form.
- _Consistency_ — whether components of data set are coherent between each other.
- _Credibility_ - whether data can be trusted and reflects correctly state of real world.
- _Currentnes_ - whether data is fresh enough and reflect recent state of real world.
- _Timeliness_ - whether data is updated within the expected time range
- _Reasonableness_ - whether high-level value patterns are close to expectations
- _Uniqueness_ - whether duplicates are present

We won't cover all the implementation for each dimension, but rather focus on one example to showcase building complete data quality monitoring.

### Data Quality in Databricks
Databricks provides two main capabilities for the Data Quality monitoring:
- **Anomaly detection** - this is a monitoring for all tables in a schema based on learned historical patterns alert on un-usual events.
  In particular, this covers data freshness (how long ago the table was updated) and completeness (proportion of missing data).
- **Data profiling** - this is a mechanism for calculating a number of table metrics which then can be used to create alerts based on. 
  This is more controlled and gradual mechanism for data quality monitoring.  

We will explore both of these features on a one example.

### Setup a data
To showcase the platform capabilities we will use [Airline](https://relational.fel.cvut.cz/dataset/Airline) data-set from "CTU Relational Learning Repository" repository.
This data set represents flight data in the US for 2016, consisting of the main `On_Time_On_Time_Performance_2016_1` table and several dimensions.
It's a bit messy so should perfectly fit for this purpose.
First lets upload a single table `On_Time_On_Time_Performance_2016_1` with flights into target Unity Catalog
```python
from pyspark.sql import SparkSession, DataFrame

JDBC_URL = "jdbc:mysql://relational.fel.cvut.cz:3306/Airline?permitMysqlScheme"
JDBC_USER = "guest"
JDBC_PASSWORD = "ctu-relational"
JDBC_DRIVER = "org.mariadb.jdbc.Driver"

# Catalog and schema names are just examples, make sure to substitue correct values
UC_CATALOG = "catalog"  
UC_SCHEMA = "schema"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {UC_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")

table = "On_Time_On_Time_Performance_2016_1"
full_target = f"{UC_CATALOG}.{UC_SCHEMA}.{table}"

(
        spark
        .read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("driver", JDBC_DRIVER)
        .load()
        .write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_target)
)
```
To give a general sense of data lets output a small portion of it:
```python
print(f"Flights table: columns count: {len(spark.table(full_target).columns)}; rows count: {spark.table(full_target).count()}")
spark.table(full_target).select(spark.table(full_target).columns[0:10]).show(10)
```
That produces the following result:
```text
Flights table: columns count: 83; rows count: 445827
+----+-------+-----+----------+---------+----------+-------------+---------+-------+-------+
|Year|Quarter|Month|DayofMonth|DayOfWeek|FlightDate|UniqueCarrier|AirlineID|Carrier|TailNum|
+----+-------+-----+----------+---------+----------+-------------+---------+-------+-------+
|2016|      1|    1|         6|        3|2016-01-06|           AA|    19805|     AA| N4YBAA|
|2016|      1|    1|         7|        4|2016-01-07|           AA|    19805|     AA| N434AA|
|2016|      1|    1|         8|        5|2016-01-08|           AA|    19805|     AA| N541AA|
|2016|      1|    1|         9|        6|2016-01-09|           AA|    19805|     AA| N489AA|
|2016|      1|    1|        10|        7|2016-01-10|           AA|    19805|     AA| N439AA|
|2016|      1|    1|        11|        1|2016-01-11|           AA|    19805|     AA| N468AA|
|2016|      1|    1|        12|        2|2016-01-12|           AA|    19805|     AA| N4YBAA|
|2016|      1|    1|        13|        3|2016-01-13|           AA|    19805|     AA| N569AA|
|2016|      1|    1|        14|        4|2016-01-14|           AA|    19805|     AA| N466AA|
|2016|      1|    1|        15|        5|2016-01-15|           AA|    19805|     AA| N501AA|
+----+-------+-----+----------+---------+----------+-------------+---------+-------+-------+
only showing top 10 rows
```

As you may see this is a pretty wide and table big table with details of flights in US.  

### Anomaly detection
After we have initial data in place now we can create anomaly detection monitoring.

**NOTE**: To make it work enable "Data quality monitoring with anomaly detection (workspace level)" in
"Previews" workspaces configuration. 

This can be done either manually on the UI or though the following tools:
- Terraform via [databricks_data_quality_monitor Resource](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/data_quality_monitor)
- Databricks SDK [data_quality API](https://databricks-sdk-py.readthedocs.io/en/latest/clients/workspace.html#databricks.sdk.WorkspaceClient.data_quality)

For a sake of demonstration simplicity lets' proceed with SDK that is by default installed in a cluster:
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dataquality import Monitor
from databricks.sdk.service.dataquality import AnomalyDetectionConfig

w = WorkspaceClient()
schema = w.schemas.get(full_name='catalog.schema')

monitor = Monitor(
    object_type='schema',
    object_id=schema.schema_id,
    anomaly_detection_config=AnomalyDetectionConfig()
)
w.data_quality.create_monitor(monitor)
```
After the creation, it takes some for an internal model to learn data patterns, after which initial results won't be 
super informative because this is just a first run. You can find them by opening "Catalog", choosing your catalog,
schema, "on_time_on_time_performance_2016_1" table and then "Quality" tab in the UI which should look something like this:
![](1_table_anomaly_detection.png)

First checks will appear after at least 24 hours once internal model will learn how many rows daily are written into a table.
TODO: show after 24 hours. both UI and `select * from system.data_quality_monitoring.table_results`

### Data Profiling
As it was mentioned before, Data Profiling is more controlled mechanism for Data Quality verification for which we need
to define quality thresholds. They are [three types of Data Profiling](https://docs.databricks.com/aws/en/data-quality-monitoring/data-profiling/monitor-output#profile-metrics-table-schema)
for different use case: Timeseries (for data with corresponding timestamp), Snapshot (for managed tables, external tables views etc.) and Inference (for tables with machine learning model outputs).
Since we are interested in measuring quality for a plain delta table, the Snapshot profile type will be our primary focus. 

At the high level, monitoring and alerting with Data Profiling looks the following:

**Create a data profiling monitor**.
It will calculate default and custom metrics per table on a given schedule.
Among default metrics calculated per table are: `min`, `max`, `percent_null`. A complete list of metrics can be found [here](https://docs.databricks.com/aws/en/data-quality-monitoring/data-profiling/monitor-output#profile-metrics-table-schema)

This can be done with using one of the following tools:
- [`databricks_data_quality_monitor` resource in Terraform](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/data_quality_monitor)
- [`quality_monitor` in Asset bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/resources#quality_monitor-unity-catalog)
- [`data_quality` API in Databricks SDK](https://docs.databricks.com/aws/en/data-quality-monitoring/data-profiling/create-monitor-api)

After profiling monitor creation it will be automatically scheduled for a refresh. 
Each refresh result will be written into two tables:
- `{source_table}_profile_metrics` - table metrics calculated for the table snapshot at the moment of schedule.
- `{source_table}_drift_metrics` - table metrics version-over-version.

**Create an alert for one of the tables**.
Similarly to the case of anomalies detection, to be notified about any issues with the data corresponding alerts needs 
to be created. Although this time, alert condition will be based on profile, and we need to decide metrics thresholds.

This can be done with using one of the following tools:
- [`databricks_alert_v2` resource in Terraform](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/alert_v2)
- [`alert` in Asset bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/resources#alert)
- [`alerts_v2` API in Databricks SDK](https://databricks-sdk-py.readthedocs.io/en/latest/clients/workspace.html#databricks.sdk.WorkspaceClient.alerts_v2)

So let's proceed to implementation. We will create a snapshot profile with additional custom metric named `outdated` 
which counts a number of dates older than 2016 year and apply it for the `FlightDate` column.
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dataquality import (
    Monitor,
    DataProfilingConfig,
    SnapshotConfig,
    DataProfilingStatus,
    RefreshState,
    Refresh,
    DataProfilingCustomMetric,
    DataProfilingCustomMetricType,
    CronSchedule,
)
from pyspark.sql import types as T

w = WorkspaceClient()

catalog_name = "personal"
schema_name = "airline_raw"
table_name = "on_time_on_time_performance_2016_1"
full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

schema = w.schemas.get(full_name=f"{catalog_name}.{schema_name}")
table = w.tables.get(full_name=f"{catalog_name}.{schema_name}.{table_name}")

table_id = table.table_id
table_object_type = "table"

output_schema_name = "airline_raw_quality"
output_schema = w.schemas.get(full_name=f"{catalog_name}.{output_schema_name}")
output_schema_id = output_schema.schema_id

# Schedule profiling once a day.
schedule = CronSchedule(quartz_cron_expression="0 0 0 * * ?", timezone_id="UTC")

# Custom metric to be added to profile
outdated_metric = DataProfilingCustomMetric(
    type=DataProfilingCustomMetricType.DATA_PROFILING_CUSTOM_METRIC_TYPE_AGGREGATE,
    name="outdated",
    input_columns=["FlightDate"],
    definition="sum(cast(year(`{{input_column}}`) < 2016 as int))",
    output_data_type=T.StructField("outdated", T.IntegerType()).json(),
)

# Profiling monitor
data_profiling_config = DataProfilingConfig(
    output_schema_id=output_schema_id,
    monitored_table_name=full_table_name,
    snapshot=SnapshotConfig(),
    monitor_version=1,
    schedule=schedule,
    custom_metrics=[outdated_metric],
)

monitor = Monitor(
    object_type="table",
    object_id=table.table_id,
    data_profiling_config=data_profiling_config,
)
w.data_quality.create_monitor(monitor)
```

Once monitor have been created, it will schedule a first refresh as soon as possible. It is also possible to manually
trigger refresh with the SDK:
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dataquality import Refresh

w = WorkspaceClient()

catalog_name = "personal"
schema_name = "airline_raw"
table_name = "on_time_on_time_performance_2016_1"

schema = w.schemas.get(full_name=f"{catalog_name}.{schema_name}")
table = w.tables.get(full_name=f"{catalog_name}.{schema_name}.{table_name}")

table_id = table.table_id
table_object_type = "table"

w.data_quality.create_refresh(
    object_type=table_object_type,
    object_id=table.table_id,
    refresh=Refresh(
        object_type=table_object_type,
        object_id=table.table_id
    )
)
```

After profiling refresh has been finished, resulting tables can be found in the specified output schema.
For instance, we can query `outdated` custom metric value from the latest profiling result:
```sql
%sql
select window.start, window.end, outdated
from personal.airline_raw_quality.on_time_on_time_performance_2016_1_profile_metrics
where column_name = 'FlightDate'
order by window.end desc
limit 1
```
So far this query is supposed to return `0` for the latest refresh. To monitor the metric values does not exceeds `0`
threshold we need to create a dedicated SQL Alert:
```python
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    AlertCondition,
    AlertEvaluationState,
    AlertV2,
    AlertV2Evaluation,
    AlertV2Notification,
    AlertV2Operand,
    AlertV2OperandColumn,
    AlertV2OperandValue,
    AlertV2Subscription,
    ComparisonOperator,
    CronSchedule,
)

w = WorkspaceClient()

# Choose the first Warehouse in your workspace
warehouses = list(w.warehouses.list())
warehouse_id = warehouses[0].id

# Query latest `outdated` metric value
query_text = """
select outdated
from personal.airline_raw_quality.on_time_on_time_performance_2016_1_profile_metrics
where column_name = 'FlightDate'
order by window.end desc
limit 1
"""

# Create an alert to trigger once `outdated` metric value is greater than 0 
evaluation = AlertV2Evaluation(
    source=AlertV2OperandColumn(name="outdated"),
    comparison_operator=ComparisonOperator.GREATER_THAN,
    threshold=AlertV2Operand(
        value=AlertV2OperandValue(double_value=0)
    ),
    empty_result_state=AlertEvaluationState.ERROR,
    notification=AlertV2Notification(
        notify_on_ok=True,
        subscriptions=[
            AlertV2Subscription(user_email="ivankoorchenko@gmail.com")
        ],
    )
)

# Schedule the alert to run daily
schedule = CronSchedule(
    quartz_cron_schedule="0 0 0 * * ?",
    timezone_id="UTC",
)

alert_config = AlertV2(
    warehouse_id=warehouse_id,
    display_name="outdated_flights_alert",
    query_text=query_text,
    evaluation=evaluation,
    schedule=schedule,
)

w.alerts_v2.create_alert(alert=alert_config)
```

For the sake of demonstration lets pollute a monitored table. For this purpose we will add to the table 10 rows
from original dataset, but replaced `FlightDate` column with the 1st of January 2015:
```python
from pyspark.sql import functions as F

full_table_name = f"personal.airline_raw.on_time_on_time_performance_2016_1"

(
    spark
    .table(full_table_name)
    .where(F.year(F.col("FlightDate")) == 2016)
    .limit(10)
    .drop("FlightDate")
    .withColumn("FlightDate", F.to_date(F.lit("2015-01-01")))
    .write
    .mode("append")
    .saveAsTable(full_table_name)
)
```
After having this in place we need to trigger monitor refresh. This can be done though the tooling or in UI by going to:
"Unity Catalog -> {table} -> Quality tab -> Data Profiling -> View Refresh History -> Refresh Metrics"

![2_table_profiling_trigger_refresh.png](2_table_profiling_trigger_refresh.png)

After metrics are refreshed, the alert needs to be triggered to raise an error.
After the alert execution notification will be sent, which in the current case is an email:
![](3_alert_notification.png)

### Alerts
Although Data Profiling is a powerful tool for data quality monitoring, it might be not enough to cover more complicated
cases such as foreign keys integrity between two or more tables or duplicates percentage by several columns.
These cases can be covered with plain SQL queries and Alerts. 

### Note
In conclusion, couple points to bear in mind if you are considering these platforms tools for implementation:
- Leverages SQL warehouses that might be more generally slightly more expensive in comparison to all-purpose compute;
- Tooling is not always aligned: Anomaly detection can not be created though asset bundles, but everything else can be.

### References
Bellow is the list of other sources used to write this post:
- https://iso25000.com/index.php/en/iso-25000-standards/iso-25012
- https://arxiv.org/pdf/2102.11527
- https://medium.com/the-thoughtful-engineer/part-5-iso-25012-in-action-what-data-quality-really-means-5e334b828595
- https://www.ibm.com/think/topics/data-reconciliation

Databricks documentation: 
- [Anomaly detection](https://docs.databricks.com/aws/en/data-quality-monitoring/anomaly-detection/)
- [Data profiling](https://docs.databricks.com/aws/en/data-quality-monitoring/data-profiling/)
- [Data profiling: Profiling tables](https://docs.databricks.com/aws/en/data-quality-monitoring/data-profiling/monitor-output)
- [Data profiling: custom metrics example](https://docs.databricks.com/aws/en/data-quality-monitoring/data-profiling/custom-metrics#aggregate-metric-example)
- [Data profiling: using the API](https://databricks-sdk-py.readthedocs.io/en/latest/clients/workspace.html#databricks.sdk.WorkspaceClient.data_quality)
- [Query data](https://docs.databricks.com/aws/en/query/)
- [Databricks SQL Alerts](https://docs.databricks.com/aws/en/sql/user/alerts/)
- [Data and AI Governance book](https://www.databricks.com/resources/ebook/data-analytics-and-ai-governance)