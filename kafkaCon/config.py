# Kafka集群配置
SOURCE_KAFKA_CONFIG = {
    'bootstrap_servers': '47.101.54.189:23001,47.101.54.189:23002,47.101.54.189:23003',
    'consumer': {
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000
    }
}

TARGET_KAFKA_CONFIG = {
    'bootstrap_servers': '172.31.1.122:9092,172.31.1.123:9092,172.31.1.124:9092',
    'producer': {
        'acks': 'all'
    }
}

# 设置并行度
PARALLELISM = 6

# Topic映射配置
TOPICS_MAPPING = [
    {
        'source': 'prod-robot-events-v3',
        'raw_topic': 'flume_ods_robot_behavior_inc',     # 原始数据
        'processed_topic': 'ods_robot_behavior_inc',      # 处理后数据
        'type': 'robot',
        'group': 'hw_flink_robotbehavior'
    },
    {
        'source': 'app-dispatch-RobotRealtimeInfo',
        'raw_topic': 'flume_ods_app_dispatch_RobotRealtimeInfo',
        'processed_topic': 'ods_app_dispatch_RobotRealtimeInfo',
        'type': 'realtime',
        'group': 'hw_flink_robotrealtimeinfo'
    },
    {
        'source': 'prod-iot-events-v3',
        'raw_topic': 'flume_ods_iot_behavior_inc',
        'processed_topic': 'ods_iot_behavior_inc',
        'type': 'iot',
        'group': 'hw_flink_iot'
    },
    {
        'source': 'locker-events-v3-new',
        'raw_topic': 'flume_ods_locker_behavior_inc',
        'processed_topic': 'ods_locker_behavior_inc',
        'type': 'locker',
        'group': 'hw_flink_locker'
    },
    {
        'source': 'vendor-events-v3-new',
        'raw_topic': 'flume_ods_retailcabinet_behavior_inc',
        'processed_topic': 'ods_retailcabinet_behavior_inc',
        'type': 'retail_cabinet',
        'group': 'hw_flink_vendor'
    },
    {
        'source': 'prod-station-events-v3',
        'raw_topic': 'flume_ods_station_behavior_inc',
        'processed_topic': 'ods_station_behavior_inc',
        'type': 'station',
        'group': 'hw_flink_station'
    }
]

# MySQL配置
MYSQL_CONFIG = {
    'host': '7a422a820a664c9fb0e007a6f76501c9in01.internal.cn-north-9.mysql.rds.myhuaweicloud.com',
    'port': 3306,
    'user': 'yogo_quality',
    'password': '963e2cC)vq72468fW4q4',
    'database': 'yogo_robotbehavior',
    'charset': 'utf8mb4',
    'use_unicode': True,
    'connect_timeout': 60,
    'pool_size': 5,
    'pool_name': 'mypool',
    
    # 额外的连接参数
    'connection_params': {
        'use_pure': True,
        'ssl_disabled': True,
        'autocommit': True,
        'time_zone': '+8:00',
        'allow_local_infile': True
    }
}