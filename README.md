# rabbitmq-http-dispatcher

An simple consumer, dispatch messages to http server.

First, it's a daemon, read `config.py` and start a lot of consumers.

Then, each consumer get message from rabbitmq, dispatch to a http api, and get http response, read the status code to decide if it's necessary send `ack` to rabbitmq.

```

    MQ    ->    http-dispatcher   ->    http api

```


# dependency

1. pika
2. the `daemon.py` is from [python-daemon](https://github.com/serverdensity/python-daemon) and make a little change.
   thanks to serverdensity




