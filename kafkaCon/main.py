import logging
import threading
import signal
import sys
from .KafkaBridge import KafkaBridge
from .config import TOPICS_MAPPING

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class KafkaService:
    def __init__(self):
        self.bridge = KafkaBridge()
        self.threads = []
        self.running = True

    def start_consumer(self, topic_config):
        """启动单个消费者的处理函数"""
        try:
            while self.running:
                try:
                    self.bridge.consume_and_produce(
                        source_topic=topic_config['source'],
                        raw_topic=topic_config['raw_topic'],
                        processed_topic=topic_config['processed_topic'],
                        msg_type=topic_config['type'],
                        group_id=topic_config['group']
                    )
                except Exception as e:
                    logging.error(f"消费者处理错误 {topic_config['source']}: {e}")
                    if self.running:
                        logging.info(f"尝试重新启动消费者 {topic_config['source']}")
                        continue
                    break
        except KeyboardInterrupt:
            logging.info(f"收到终止信号，正在关闭消费者 {topic_config['source']}")

    def signal_handler(self, signum, frame):
        """处理终止信号"""
        logging.info("收到终止信号，正在优雅关闭...")
        self.running = False
        for thread in self.threads:
            thread.join()

    def run(self):
        """运行服务"""
        # 注册信号处理
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        logging.info("启动Kafka数据分发服务...")

        # 为每个topic配置创建一个线程
        for topic_config in TOPICS_MAPPING:
            thread = threading.Thread(
                target=self.start_consumer,
                args=(topic_config,)
            )
            thread.daemon = True
            self.threads.append(thread)
            thread.start()
            logging.info(f"启动消费者线程: {topic_config['source']}")

        # 等待所有线程完成
        try:
            for thread in self.threads:
                thread.join()
        except KeyboardInterrupt:
            self.signal_handler(None, None)

def main():
    """主函数，供外部调用"""
    try:
        service = KafkaService()
        service.run()
    except Exception as e:
        logging.error(f"Kafka服务启动错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 