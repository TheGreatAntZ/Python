import mysql.connector
from mysql.connector import pooling
import logging
import json
from .config import MYSQL_CONFIG

class MySQLConnectionPool:
    _instance = None
    _pool = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MySQLConnectionPool, cls).__new__(cls)
            try:
                # 创建连接池
                dbconfig = {
                    'host': MYSQL_CONFIG['host'],
                    'port': MYSQL_CONFIG['port'],
                    'user': MYSQL_CONFIG['user'],
                    'password': MYSQL_CONFIG['password'],
                    'database': MYSQL_CONFIG['database'],
                    'charset': MYSQL_CONFIG['charset'],
                    **MYSQL_CONFIG['connection_params']
                }
                
                cls._pool = mysql.connector.pooling.MySQLConnectionPool(
                    pool_name=MYSQL_CONFIG['pool_name'],
                    pool_size=MYSQL_CONFIG['pool_size'],
                    **dbconfig
                )
                logging.info("MySQL连接池初始化成功")
                
            except Exception as e:
                logging.error(f"MySQL连接池初始化失败: {e}")
                raise
                
        return cls._instance

    @classmethod
    def get_connection(cls):
        """获取数据库连接"""
        if cls._pool is None:
            MySQLConnectionPool()
        return cls._pool.get_connection()

    @staticmethod
    def execute_query(sql, params=None):
        """执行SQL查询"""
        conn = None
        cursor = None
        try:
            conn = MySQLConnectionPool.get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(sql, params)
            conn.commit()
            return True
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"SQL执行错误: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @staticmethod
    def insert_robot_behavior(data):
        """插入机器人行为数据"""
        sql = """
            INSERT INTO robot_behavior_inc (
                date_time, event_uuid, site_uid, unit_uid,
                event_name, event_type, app_name,
                event_tags, event_info, event_logs,
                created_at, received_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        
        try:
            event_tags = data.get('event_tags', {})
            date_time = data['created_at'].split(' ')[0]
            
            params = (
                date_time,
                data.get('event_uuid'),
                event_tags.get('site_uid'),
                event_tags.get('unit_uid'),
                data.get('event_name'),
                data.get('event_type'),
                data.get('app_name'),
                json.dumps(data.get('event_tags')),
                json.dumps(data.get('event_info')),
                json.dumps(data.get('event_logs')),
                data.get('created_at'),
                data.get('received_at')
            )
            
            return MySQLConnectionPool.execute_query(sql, params)
            
        except Exception as e:
            logging.error(f"插入机器人行为数据错误: {e}")
            return False 