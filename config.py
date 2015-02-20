#!/usr/bin/env python
# encoding: utf-8


RABBITMQ_CONFIG = {
        "host": "localhost",
        "username": "guest",
        "password": "guest"
}

CONSUMERS = [
        {
            "exchange": "ACTION",
            "queue": "ALL_ACTION",
            "routing_key": "ACTION.*",
            "http_method": "POST",
            "http_url": "http://127.0.0.1:5000/api",
        },
        {
            "exchange": "log",
            "queue": "atest",
            "routing_key": "*",
            "host": "localhost",
            "http_method": "POST",
            "http_url": "http://127.0.0.1:5001/api/",
        },
        ]


# "http_url": "http://127.0.0.1/test2/",
