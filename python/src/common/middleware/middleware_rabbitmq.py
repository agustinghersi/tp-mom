import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import sys
import os

# Por defecto, RabbitMQ envía cada mensaje al siguiente consumidor, en secuencia. 
# En promedio, cada consumidor recibe la misma cantidad de mensajes (Round Robin)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name

    # Receptor de HOla Mundo
    def start_consuming(self, on_message_callback):
        try:
            # Aca conecto a un broker de localhost para recibir mensajes
            connection = pika.BlockingConnection(pika.ConnectionParameters(self.host)) # POner IP de otra maquina para enviarlo ahi
            channel = connection.channel()

            # Creacion de queue idempotente, conviene siempre hacerlo 2 veces
            channel.queue_declare(queue=self.queue_name, durable=True, arguments={'x-queue-type': 'quorum'})
            
            channel.basic_qos(prefetch_count=1) # Hasta no terminar la tarea, Rabbit no envia otra al worker
            # Ver que esto puede dar error de llenar la queue despues
            channel.basic_consume(queue=self.queue_name,
                        on_message_callback=on_message_callback) # Saco el ACK automatico

            # Aca se entra en un bucle infinito, se sale con ctrl C
            print(' [*] Waiting for messages. To exit press CTRL+C')
            channel.start_consuming()
        except KeyboardInterrupt:
            print('Interrupted')
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)


    def stop_consuming(self):
        pass
    
    def send(self, message):
        # Aca conecto a un broker de localhost
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.host)) # POner IP de otra maquina para enviarlo ahi
        channel = connection.channel()

        # Declaro la queue a la que envio los mensajes
        channel.queue_declare(queue=self.queue_name, durable=True, arguments={'x-queue-type': 'quorum'})

        # Mando el mensaje
        channel.basic_publish(exchange=self.exchange_name, # Ver que poner aca, no tengo el nombre
                      routing_key=self.routing_keys,
                      body=message,
                      properties=pika.BasicProperties( # Hago que los mensajes sean persistentes
                         delivery_mode = pika.DeliveryMode.Persistent # Ver el error de que queden en cache si pasa algo raro
                      ))

        # Para vaciar buffers de red y garantizar envio de mensaje a rabbit
        connection.close()

    def close(self):
        pass
    

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

    def start_consuming(self, on_message_callback):
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.host)) # POner IP de otra maquina para enviarlo ahi
        channel = connection.channel()

        channel.exchange_declare(exchange=self.exchange_name, exchange_type='fanout')
        
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue # Rabbit me da el nombre de la queue

        channel.queue_bind(exchange=self.exchange_name, queue=queue_name)
        print(' [*] Waiting for logs. To exit press CTRL+C')

        channel.basic_consume(
            queue=queue_name, on_message_callback=on_message_callback, auto_ack=True)

        channel.start_consuming()
    
    def stop_consuming(self):
        pass

    def send(self, message):
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.host)) # POner IP de otra maquina para enviarlo ahi
        channel = connection.channel()

        # Creo el exchange
        channel.exchange_declare(exchange=self.exchange_name, exchange_type='fanout')
        # fanout manda cada mensaje a cada cola conocida

        # Envio el mensaje
        channel.basic_publish(exchange=self.exchange_name, routing_key=self.routing_keys, body=message)
        connection.close()

    def close(self):
        pass
