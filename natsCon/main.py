from .NatsKafkaBridge import NatsKafkaBridge


class Config:
    def __init__(self):
        # NATS 配置
        self.nats_servers = [
            "nats://139.196.170.193:5222",
            "nats://139.224.11.75:5222",
            "nats://47.100.232.115:5222"
        ]  # 将每个 NATS 地址拆分成单独的字符串
        # Kafka 配置
        self.kafka_config = {
            'bootstrap.servers': '172.31.1.122:9092,172.31.1.123:9092,172.31.1.124:9092',
            'client.id': 'nats-kafka-bridge'
        }
        # 订阅的 NATS 主题
        self.topics = [
            "tm.*.*.batch.props",
            # "tm.*.*.metrics.batch"  # 暂时不订阅 metrics 数据
        ]


def main():
    # 实例化配置
    config = Config()

    # 创建 NatsKafkaBridge 对象
    bridge = NatsKafkaBridge(
        nats_servers=config.nats_servers,
        kafka_config=config.kafka_config,
        topics=config.topics
    )

    # 启动服务
    try:
        bridge.start()
    except Exception as e:
        print(f"Error running bridge: {e}")


if __name__ == "__main__":
    main()
