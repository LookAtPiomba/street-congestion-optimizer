import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.window import Slide

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env_settings = (
    EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
)
st_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

# Define source
st_env.execute_sql(
    f"""
    CREATE TABLE source (
        step FLOAT,
        veh_id STRING,
        speed FLOAT,
        next_tl STRING,
        next_tl_state STRING,
        ts BIGINT,
        rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts, 'yyyy-MM-dd HH:mm:ss')),
        WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND
    ) WITH (
        'connector' = 'kafka-0.11',
        'topic' = '{os.environ["KAFKA_TOPIC"]}',
        'scan.startup.mode' = 'latest-offset',
        'properties.bootstrap.servers' = '{os.environ["KAFKA_HOST"]}',
        'properties.zookeeper.connect' = '{os.environ["ZOOKEEPER_HOST"]}',
        'properties.group.id' = '{os.environ["KAFKA_CONSUMER_GROUP"]}',
        'format' = 'json'
    )
    """
)

# create and initiate loading of source Table
tbl = st_env.from_path('source')

print('\nSource Schema')
tbl.print_schema()

# Define output sink
st_env.execute_sql(
    f"""
    CREATE TABLE sink (
        avg_speed FLOAT,
        tl_id STRING       
    ) WITH (
        'connector' = 'kafka-0.11',
        'topic' = 'output',
        'properties.bootstrap.servers' = '{os.environ["KAFKA_HOST"]}',
        'format' = 'json'
    )
"""
)

#query source table for avg speed for each traffic light
st_env.from_path("source").window(
    Slide.over("4.seconds").every("4.seconds").on("rowtime").alias("w")
).group_by('w, next_tl').select(
    "AVG(speed) as avg_speed, next_tl as tl"
).where('avg_speed < 3').insert_into(
    "sink"
)

#Execute the job
st_env.execute("PyFlink job")
