#!/usr/bin/env python
# encoding: utf-8
"""
消费者

http://pika.readthedocs.org/en/latest/examples/asynchronous_consumer_example.html
http://pika.readthedocs.org/en/latest/examples/blocking_consume.html

daemon
https://github.com/gmr/rejected
https://github.com/serverdensity/python-daemon
http://slaytanic.blog.51cto.com/2057708/742049
https://docs.python.org/2/library/multiprocessing.html daemon
"""

import pika

# TODO: 线程启动时, 作为deamon, 一个consumer启动一个进程


class Consumer(object):
    """
    消费者, 从消息队列中取出, 处理
    """
    def __init__(self, host, username="", password=""):
        """
        """
        credentials = pika.PlainCredentials(username, password)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials))
        self.channel = self.connection.channel()

        self.exchange_name = None
        self.queue_name = None

    def start_consuming(self, callback_func, no_ack=False):
        """
        """
        self.channel.basic_consume(callback_func,
                            queue=self.queue_name,
                            no_ack=no_ack)
        self.channel.start_consuming()

    def stop_consuming(self):
        """
        """
        self.channel.stop_consuming()

    def close(self):
        """
        停止
        """
        self.connection.close()

    def declare_exchange(self, exchange_name, durable=True):
        """
        定义一个exchange
        """
        # self.channel.exchange_declare(exchange=exchange, type='fanout')
        self.exchange_name = exchange_name
        self.channel.exchange_declare(exchange=exchange_name, type='topic', durable=durable)

    def declare_queue(self, queue_name, routing_key="*", durable=True):
        """
        定义一个queue
        """
        # result = self.channel.queue_declare(exclusive=True)
        # queue_name = result.method.queue
        # print "TRACK ================= queue_name", queue_name
        # self.channel.queue_bind(exchange=exchange, queue="test", routing_key="a")
        # self.channel.basic_consume(callback_func,
                            # queue=queue_name,
                            # no_ack=True)

        self.queue_name = queue_name
        self.channel.queue_declare(queue=queue_name, durable=durable)
        self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=routing_key)



# TODO:问题, 如何进行流量控制
# TODO: 如果我们想一次只吐一条消息, 当其它消费者连上来时, 还可以并行处理, 简单地把 ack 打开就可以了(默认就是打开的).
# 再考虑一下细节. 当有多个消费者连上时, 它是从队列一次取一条消息, 还是一次取多条消息(这样至少可以改善性能).
# 这可以通过配置 channel 的 qos 相关参数实现
# connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
# channel = connection.channel()
# channel.queue_declare(queue='A')
# channel.basic_qos(prefetch_count=2)

# def callback(ch, method, properties, body):
    # import time
    # time.sleep(10)
    # print body
    # ch.basic_ack(delivery_tag = method.delivery_tag)

# channel.basic_consume(callback, queue='A', no_ack=False)
# channel.start_consuming()

# TODO: 提取时进行消息确认
# connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
# channel = connection.channel()
# r = channel.basic_get(queue='A', no_ack=False) #0
# print r[-1], r[0].delivery_tag
# #channel.basic_ack(delivery_tag=r[0].delivery_tag)
# channel.basic_reject(delivery_tag=r[0].delivery_tag)

# 一次确认多条
# connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
# channel = connection.channel()
# r = channel.basic_get(queue='A', no_ack=False) #0
# r = channel.basic_get(queue='A', no_ack=False) #1
# r = channel.basic_get(queue='A', no_ack=False) #2
# channel.basic_nack(delivery_tag=r[0].delivery_tag, multiple=True)

# import requests
# import json

# def callback(ch, method, properties, body):

    # print " [x] %r" % (body,)
    # # 1. 操作成功, 只要http没有500

    # print "TRACK ================= ", json.loads(body)


    # r = requests.get("http://www.baidu.com")

    # if r.status_code == 200:
        # print "TRACK ================= success"
        # ch.basic_ack(delivery_tag=method.delivery_tag)
    # else:
        # print "TRACK ================= fail"

    # # 2. 操作失败, 这个会导致rabbitmq收到后, 再次将消息发出.....
    # # ch.basic_reject(delivery_tag=method.delivery_tag)

    # # 3. 如果不操作, 那么这个消息将不会消失, 也不会立即分派


# # TODO: consumer变成多进程的
# if __name__ == '__main__':
    # print ' [*] Waiting for logs. To exit press CTRL+C'

    # exchange_name = "INCOME_ACTION"
    # queue_name = "ALL_ACTION"
    # con = Consumer("localhost")

    # con.declare_exchange(exchange_name, durable=True)
    # con.declare_queue(exchange_name, callback, queue_name, routing_key='ACTION.#', durable=True, no_ack=False)
    # # con.declare_queue(exchange_name, callback, queue_name, routing_key='*', durable=True, no_ack=False)

    # con.start_consuming()


