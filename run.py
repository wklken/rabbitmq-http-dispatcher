#!/usr/bin/env python
# encoding: utf-8

import os
import sys
import time
import psutil
import logging
import multiprocessing

import requests
from daemon import Daemon
from consumer import Consumer

from config import CONSUMERS, RABBITMQ_CONFIG


class ConsumerHandler(object):
    """
    消费者处理hander
    """
    def __init__(self, exchange, queue, host, username, password, routing_key, http_method, http_url):
        """
        初始化
        """
        logging.getLogger("rabbit_consumer").info("pid[%s] Init consumer begin......" % os.getpid())

        self.http_method = http_method
        self.http_url = http_url


        self.consumer = Consumer(host, username, password)
        self.consumer.declare_exchange(exchange, durable=True)
        self.consumer.declare_queue(queue, routing_key=routing_key, durable=True)

        logging.getLogger("rabbit_consumer").info("pid[%s] Init consumer end...... " % os.getpid())


    def callback(self, ch, method, properties, body):
        """
        回调方法
        """
        print "RECEIVE MESSAGE: %s" % body
        params = {"message": body}
        try:
            if self.http_method == 'GET':
                r = requests.get(self.http_url, params=params, timeout=60)
            elif self.http_method == 'POST':
                r = requests.post(self.http_url, data=params, timeout=60)
            else:
                return
        except Exception, e:
            # import traceback
            # traceback.print_exc()
            logging.getLogger("rabbit_consumer").error("pid[%s] ERROR: exception happend when callback - %s" % (os.getpid(), str(e)) )
        else:
            if r.status_code == 200:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logging.getLogger("rabbit_consumer").error("pid[%s] ERROR: http return error url[%s] message[%s]" % (os.getpid(), self.http_url, body))

    # def run(self):
        # """
        # """
        # try:
            # self.consumer.start_consuming(callback_func=self.callback)
        # except Exception, e:
            # print e
            # import traceback
            # traceback.print_exc()
            # self.consumer.stop_consuming()
        # finally:
            # self.consumer.close()

    def run(self):
        """
        检查并启动参考 : http://stackoverflow.com/questions/22572922/how-to-start-multiple-pika-workers
        """
        while True:
            try:
                if len(self.consumer.channel.consumer_tags) == 0:
                    logging.getLogger("rabbit_consumer").info("pid[%s] No consumer running, start one " % os.getpid())
                    self.consumer.start_consuming(callback_func=self.callback)
                time.sleep(10)
            except Exception, e:
                logging.getLogger("rabbit_consumer").error("pid[%s] ERROR: exception happend when start - %s" % (os.getpid(), str(e)))
                break
            finally:
                self.consumer.close()


def setup_consumer(kwargs):
    """
    初始化一个consumer, 并开始处理消息

    """
    exchange = kwargs.get('exchange')
    queue = kwargs.get('queue')
    host = kwargs.get('host')
    username = kwargs.get('username')
    password = kwargs.get('password')

    routing_key = kwargs.get('routing_key')
    http_method = kwargs.get('http_method')
    http_url = kwargs.get('http_url')

    logging.getLogger("rabbit_consumer").info("pid[%s] setup consumer begin......" % os.getpid())
    try:
        handler = ConsumerHandler(exchange, queue, host, username, password, routing_key, http_method, http_url)
        handler.run()
    except Exception, e:
        logging.getLogger("rabbit_consumer").error("pid[%s] setup consumer exception: %s" % (os.getpid(), str(e)))

    logging.getLogger("rabbit_consumer").info("pid[%s] setup consumer end......" % os.getpid())


class ConsumerDispatcherDaemon(Daemon):
    """
    守护进程
    """
    def before_stop(self, pid):
        """
        杀
        """
        parent = psutil.Process(pid)
        for child in parent.get_children(recursive=True):
            child.kill()

        # if including_parent:
            # parent.kill()

    def run(self):
        """
        运行
        """
        logging.getLogger("rabbit_consumer").info("Begin to start processor")

        ps = []
        for c in CONSUMERS:
            c.update(RABBITMQ_CONFIG)

            logging.getLogger("rabbit_consumer").info("init by config: %s" % str(c))

            p = multiprocessing.Process(target=setup_consumer, args=(c,))
            p.daemon = True
            p.start()
            ps.append(p)

        for p in ps:
            p.join()


def _logger_init(logger_name, logger_path, logger_level):
    """
    日志初始化
    """
    formatter = logging.Formatter('[%(asctime)s]-[%(name)s]-[%(levelname)s]:  %(message)s')
    logger = logging.getLogger(logger_name)
    logger.setLevel(logger_level)
    fh = logging.FileHandler(logger_path)
    fh.setFormatter(formatter)
    logger.addHandler(fh)


if __name__ == '__main__':

    if len(sys.argv) != 3:
        print "ERROR: Wrong options for  run.py [start|stop|restart] DATA_PATH"
        sys.exit(1)

    cmd = sys.argv[1]
    data_dir = sys.argv[2]

    if not os.path.exists(data_dir):
        print "ERROR: pid_idr [%s] not exists!" % data_dir
        sys.exit(1)

    pid_path = os.path.join(data_dir, 'consumer_dispatcher.pid')
    log_path = os.path.join(data_dir, 'consumer_dispatcher.log')

    _logger_init("rabbit_consumer", log_path, logging.INFO)

    cdd = ConsumerDispatcherDaemon(pid_path)
    if cmd == "start":
        cdd.start()
    elif cmd == 'stop':
        cdd.stop()
    elif cmd == 'restart':
        cdd.restart()
    else:
        print "run.py [start|stop|restart] DATA_PATH"
        sys.exit(1)


