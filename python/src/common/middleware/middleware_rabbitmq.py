import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import sys
import os

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        pass

    # Receptor de HOla Mundo
    def start_consuming(self, on_message_callback):
        try:
            # Aca conecto a un broker de localhost para recibir mensajes
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost')) # POner IP de otra maquina para enviarlo ahi
            channel = connection.channel()

            # Creacion de queue idempotente, conviene siempre hacerlo 2 veces
            channel.queue_declare(queue='hello', durable=True, arguments={'x-queue-type': 'quorum'})

            # Utilizo la funcion callback que invoca pika para leer un mensaje de la cola
            def callback(ch, method, properties, body):
                print(f" [x] Received {body}")
            
            channel.basic_consume(queue='hello',
                        auto_ack=True,
                        on_message_callback=callback)

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
        pass


    def close(self):
        pass
    

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass

    def start_consuming(self, on_message_callback):
        pass
    
    def stop_consuming(self):
        pass

    # Sender de Hola Mundo
    def send(self, message):
        # Aca conecto a un broker de localhost
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost')) # POner IP de otra maquina para enviarlo ahi
        channel = connection.channel()

        # Declaro la queue a la que envio los mensajes
        channel.queue_declare(queue='hello', durable=True, arguments={'x-queue-type': 'quorum'})

        # Aca se manda el Hola Mundo a la queue hello
        channel.basic_publish(exchange='',
                      routing_key='hello',
                      body='Hello World!')
        print(" [x] Sent 'Hello World!'")

        # Para vaciar buffers de red y garantizar envio de mensaje a rabbit
        connection.close()

    def close(self):
        pass
