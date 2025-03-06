import asyncio
import json
import re
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers
from confluent_kafka import Producer

class NatsKafkaBridge:
    def __init__(self, nats_servers, kafka_config, topics, thread_pool_size=10):
        self.nats_servers = nats_servers
        self.kafka_producer = Producer(kafka_config)
        self.topics = topics
        self.thread_pool_size = thread_pool_size
        self.loop = asyncio.get_event_loop()
        self.subscriptions = {}  # 用于存储已订阅的主题和订阅 ID

    async def connect_nats(self):
        """Connect to NATS and subscribe to topics"""
        self.nc = NATS()

        try:
            await self.nc.connect(
                servers=self.nats_servers,
                ping_interval=5,
                reconnect_time_wait=2,
                max_reconnect_attempts=5,
                connect_timeout=3,
                name="my-nats-connection",
                allow_reconnect=True,
                verbose=True
            )
            print("NATS connected successfully!")

            async def disconnected_cb():
                print("与 NATS 的连接已断开")

            async def reconnected_cb():
                print("已重新连接到 NATS")

            async def error_cb(e):
                print(f"NATS 错误: {str(e)}")

            self.nc.on_disconnect = disconnected_cb
            self.nc.on_reconnect = reconnected_cb
            self.nc.on_error = error_cb

            # 订阅主题
            for subject in self.topics:
                await self.subscribe_to_topic(subject)

        except ErrNoServers as e:
            print(f"无法连接到 NATS 服务器: {e}")
            raise e
        except Exception as e:
            print(f"NATS 连接过程中发生错误: {e}")
            raise e

    async def subscribe_to_topic(self, subject):
        """Subscribe to a topic if not already subscribed"""
        if subject in self.subscriptions:
            print(f"已订阅主题: {subject}")
            return

        try:
            sid = await self.nc.subscribe(
                subject,
                cb=self.handle_message,
                pending_bytes_limit=1024 * 1024
            )
            self.subscriptions[subject] = sid  # 保存订阅 ID
            print(f"Successfully subscribed to topic: {subject}")
        except Exception as e:
            print(f"订阅主题 {subject} 失败: {str(e)}")

    async def handle_message(self, msg):
        """Handle incoming NATS messages"""
        try:
            subject = msg.subject
            data = msg.data.decode()
            message_json = json.loads(data)
            message_json["unit_uid"] = subject

            # if re.match(r"tm\..*\.metrics\.batch", subject):
            #     # 处理 metrics 数据
            #     await self.send_to_kafka("nats_metrics", message_json)
            if re.match(r"tm\..*\.batch\.props", subject):
                # 根据 tm.1. 前缀决定发送到哪个主题
                kafka_topic = "nats_robotindex" if subject.startswith("tm.1.") else "nats_gate"
                await self.send_to_kafka(kafka_topic, message_json)

        except Exception as e:
            print(f"消息处理错误: {str(e)}")
            return

    async def send_to_kafka(self, kafka_topic, message):
        """Send message to Kafka"""
        try:
            self.kafka_producer.produce(
                topic=kafka_topic,
                value=json.dumps(message).encode('utf-8'),
                callback=self.kafka_callback
            )
            self.kafka_producer.poll(0)
        except Exception as e:
            print(f"发送消息到 Kafka 失败 ({kafka_topic}): {str(e)}")

    @staticmethod
    def kafka_callback(err, msg):
        """Kafka callback for delivery reports"""
        if err is not None:
            print(f"Message delivery failed: {err}")

    async def close_nats(self):
        """Close NATS connection"""
        if self.nc.is_connected:
            await self.nc.close()
            print("NATS connection closed")

    def start(self):
        """Start the bridge"""
        try:
            self.loop.run_until_complete(self.connect_nats())
            self.loop.run_forever()
        except KeyboardInterrupt:
            print("Interrupted")
        finally:
            self.loop.run_until_complete(self.close_nats())
            self.kafka_producer.flush()
            print("Kafka producer closed")
            self.loop.close()