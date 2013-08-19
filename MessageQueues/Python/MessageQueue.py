"""
    By: Matt Livingston
    Requires = pip install pika
"""
import pika
import time

class Producer:
    def __init__(self, host, queue, exchange, routing_key, username, passwd):
        self._host = host
        self._queue = queue
        self._exchange = exchange
        self._routing_key = routing_key
        self._credentials = pika.PlainCredentials(username, passwd)
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._host, 
                                                                             credentials = self._credentials,
                                                                             heartbeat=300))
        self._channel = self._connection.channel()
        
    def send(self, body, queue=''):
        if queue == '':
            queue = self._queue
        
        self._channel.queue_declare(queue=queue)
        channel.basic_publish(exchange=self._exchange, 
                              routing_key = self._routing_key,
                              body=body)
    def Close(self):
        self._connection.close()    
    
        
class Consumer:
    def __init__(self, host, queue, exchange, routing_key, username, passwd):
        self._host = host
        self._queue = queue
        self._exchange = exchange
        self._routing_key = routing_key
        self._credentials = pika.PlainCredentials(username, passwd)        
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._host, 
                                                                             credentials = self._credentials,
                                                                             heartbeat=300))
        self._channel = self._connection.channel()
    
    def callback(self, ch, method, properties, body):
        print "[x] Received %r" % (body,)
        time.sleep(body.count('.'))
        print "[x] Done"
        ch.basic_ack(delivery_tag = method.delivery_tag)
        
    def start(self):
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(callback, queue = self._queue)
        self._channel.start_consuming()
        
        

class ManageQueue:
    def __init__(self, host, queue, exchange, routing_key, username, passwd):
        self._host = host
        self._queue = queue
        self._exchange = exchange
        self._routing_key = routing_key
        self._credentials = pika.PlainCredentials(username, passwd)        
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._host, 
                                                                             credentials = self._credentials,
                                                                             heartbeat=300))
        self._channel = self._connection.channel()
    
    def DeleteQueue(self):
        self._connection.queue_delete(queue=self._queue)
        
    def PurgeQueue(self):
        self._connection.queue_purge(queue=self._queue)

    
        
        
        