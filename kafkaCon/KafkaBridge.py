from confluent_kafka import Consumer, Producer, KafkaError
from datetime import datetime
import json
import logging
import time
from .config import SOURCE_KAFKA_CONFIG, TARGET_KAFKA_CONFIG
from .db_utils import MySQLConnectionPool

class KafkaBridge:
    def __init__(self):
        # 源Kafka配置（阿里云）- 消费者配置
        self.source_bootstrap_servers = SOURCE_KAFKA_CONFIG['bootstrap_servers']
        self.consumer_config = {
            'bootstrap.servers': self.source_bootstrap_servers,
            'session.timeout.ms': 45000,          # 会话超时时间
            'heartbeat.interval.ms': 10000,       # 心跳间隔
            'max.poll.interval.ms': 300000,       # 最大轮询间隔
            'socket.timeout.ms': 30000,           # Socket超时时间
            'fetch.min.bytes': 1024 * 1024,       # 每次至少拉取1MB数据
            'fetch.max.bytes': 52428800,          # 每次最多拉取50MB数据
            'fetch.wait.max.ms': 500,             # 如果没有足够数据，最多等待500ms
            'enable.auto.commit': True,           # 自动提交
            'auto.commit.interval.ms': 5000,      # 自动提交间隔
            'auto.offset.reset': 'earliest',      # 偏移量重置策略
            **SOURCE_KAFKA_CONFIG.get('consumer', {})
        }
        
        # 目标Kafka配置（本地）- 生产者配置
        self.target_bootstrap_servers = TARGET_KAFKA_CONFIG['bootstrap_servers']
        self.producer_config = {
            'bootstrap.servers': self.target_bootstrap_servers,
            'message.timeout.ms': 30000,          # 消息超时时间
            'request.timeout.ms': 30000,          # 请求超时时间
            'retry.backoff.ms': 1000,             # 重试回退时间
            'delivery.timeout.ms': 120000,        # 传递超时时间
            'linger.ms': 0,                       # 消息发送延迟
            'acks': 'all',                        # 确认机制
            'batch.size': 16384,                  # 批量大小
            'compression.type': 'snappy',         # 压缩类型
            **TARGET_KAFKA_CONFIG.get('producer', {})
        }

        # 初始化生产者池
        self.producer_pool = []
        for _ in range(3):  # 创建3个生产者实例
            self.producer_pool.append(Producer(self.producer_config))

    def process_timestamp(self, timestamp, add_hours=0):
        """处理时间戳格式"""
        if not timestamp:
            return None
            
        try:
            # 分离日期和时间
            date_part, time_part = timestamp.split('T')
            
            # 获取毫秒部分
            microseconds = '000000'
            if len(time_part) > 8:
                microseconds = time_part[9:15]
                
            # 格式化时间
            dt = datetime.strptime(f"{date_part} {time_part[:8]}", 
                                "%Y-%m-%d %H:%M:%S")
            if add_hours:
                dt = dt.timestamp() + add_hours * 3600
                dt = datetime.fromtimestamp(dt)
                
            # 添加毫秒并格式化
            return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{microseconds}"
            
        except Exception as e:
            logging.error(f"时间戳处理错误: {e}")
            return None

    def process_message(self, msg, msg_type='robot'):
        """处理不同类型的消息"""
        try:
            data = json.loads(msg)
            processed_data = data.copy()  # 创建副本进行处理
            
            if msg_type == 'robot':
                # 处理机器人行为数据
                if 'received_at' in processed_data:
                    processed_data['received_at'] = self.process_timestamp(processed_data['received_at'])
                if 'created_at' in processed_data:
                    processed_data['created_at'] = self.process_timestamp(processed_data['created_at'], 8)
                    
                # 如果是机器人数据，同时写入MySQL
                MySQLConnectionPool.insert_robot_behavior(processed_data)
                    
            elif msg_type == 'realtime':
                # 处理实时信息数据
                if 'online_at' in processed_data:
                    processed_data['online_at'] = self.process_timestamp(processed_data['online_at'])
                    
            elif msg_type == 'iot':
                # 处理IOT数据，只处理T格式，不调整时区
                if 'created_at' in processed_data:
                    processed_data['created_at'] = self.process_timestamp(processed_data['created_at'])
                if 'received_at' in processed_data:
                    processed_data['received_at'] = self.process_timestamp(processed_data['received_at'])
                    
            elif msg_type in ['locker', 'retail_cabinet', 'station']:
                # 处理设备数据
                if 'created_at' in processed_data:
                    processed_data['created_at'] = self.process_timestamp(processed_data['created_at'], 8)
                if 'received_at' in processed_data:
                    processed_data['received_at'] = self.process_timestamp(processed_data['received_at'])
                # 添加日期时间字段
                if 'created_at' in processed_data:
                    dt = datetime.strptime(processed_data['created_at'].split('.')[0], 
                                         "%Y-%m-%d %H:%M:%S")
                    processed_data['date_time'] = dt.strftime("%Y-%m-%d")
                    
            return data, processed_data  # 返回原始数据和处理后的数据
            
        except Exception as e:
            logging.error(f"消息处理错误: {e}")
            return None, None

    def get_consumer(self, topic, group_id):
        """获取消费者实例"""
        config = self.consumer_config.copy()
        config['group.id'] = group_id
        
        consumer = Consumer(config)
        consumer.subscribe([topic])
        return consumer

    def get_producer(self):
        """获取生产者实例"""
        return Producer(self.producer_config)

    def produce_message(self, topic, message):
        """使用生产者池发送消息"""
        # 从池中轮询选择一个生产者
        producer = self.producer_pool[hash(str(message)) % len(self.producer_pool)]
        try:
            producer.produce(
                topic,
                json.dumps(message).encode('utf-8'),
                callback=self.delivery_report
            )
            producer.poll(0)
        except Exception as e:
            logging.error(f"消息发送错误: {e}")
            raise

    @staticmethod
    def delivery_report(err, msg):
        """消息发送回调函数"""
        if err is not None:
            logging.error(f'消息发送失败: {err}')
        else:
            logging.debug(f'消息发送成功: {msg.topic()} [{msg.partition()}]')

    def consume_and_produce(self, source_topic, raw_topic, processed_topic, msg_type, group_id):
        """优化的消费和生产逻辑"""
        max_retries = 3
        retry_interval = 5
        batch_size = 100  # 批处理大小
        messages_buffer = []
        consumer = None
        
        while True:
            try:
                consumer = self.get_consumer(source_topic, group_id)
                
                while True:
                    try:
                        # 使用poll替代consume
                        msg = consumer.poll(1.0)
                        
                        if msg is None:
                            # 如果有缓存的消息，处理它们
                            if messages_buffer:
                                self._send_batch(messages_buffer, raw_topic, processed_topic)
                                messages_buffer.clear()
                                consumer.commit()
                            continue
                            
                        if msg.error():
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                continue
                            else:
                                raise Exception(f"Kafka消费错误: {msg.error()}")
                                
                        value = msg.value().decode('utf-8')
                        raw_data, processed_data = self.process_message(value, msg_type)
                        
                        if raw_data and processed_data:
                            messages_buffer.append((raw_data, processed_data))
                            
                        if len(messages_buffer) >= batch_size:
                            self._send_batch(messages_buffer, raw_topic, processed_topic)
                            messages_buffer.clear()
                            consumer.commit()
                            
                    except Exception as e:
                        logging.error(f'处理消息错误 {source_topic}: {e}')
                        raise
                        
            except KeyboardInterrupt:
                break
            except Exception as e:
                logging.error(f'消费者错误 {source_topic}: {e}')
                time.sleep(retry_interval)
                continue
            finally:
                try:
                    if consumer:
                        consumer.close()
                except Exception as e:
                    logging.error(f'关闭消费者错误: {e}')

    def _send_batch(self, messages, raw_topic, processed_topic):
        """批量发送消息"""
        for raw_data, processed_data in messages:
            try:
                self.produce_message(raw_topic, raw_data)
                self.produce_message(processed_topic, processed_data)
            except Exception as e:
                logging.error(f"批量发送消息失败: {e}")
                raise
        
        # 确保所有生产者都完成发送
        for producer in self.producer_pool:
            producer.flush()