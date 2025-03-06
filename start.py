import argparse
import logging
from kafkaCon.main import main as kafka_main
from natsCon.main import main as nats_main

def main():
    # 配置命令行参数
    parser = argparse.ArgumentParser(description='启动服务')
    parser.add_argument(
        'service', 
        choices=['kafka', 'nats', 'all'], 
        help='选择要启动的服务: kafka, nats 或 all'
    )
    
    args = parser.parse_args()
    
    try:
        if args.service == 'kafka':
            kafka_main()
        elif args.service == 'nats':
            nats_main()
        else:  # all
            # TODO: 实现同时启动两个服务的逻辑
            # 可以使用多进程或者其他方式
            pass
            
    except KeyboardInterrupt:
        logging.info("收到终止信号，正在关闭服务...")
    except Exception as e:
        logging.error(f"服务启动错误: {e}")

if __name__ == "__main__":
    main() 