import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import sys
import os
import time

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

            # Utilizo la funcion callback que invoca pika para leer un mensaje de la cola
            def callback(ch, method, properties, body):
                pprint(f" [x] Received {body.decode()}")
                # Simula 1 seegundo de trabajo por punto en el mensaje
                time.sleep(body.count(b'.'))
                print(" [x] Done")
                # Ahora mando ack manual para confirm que el mensaje se proceso
                # Ante un ctrl C el mensaje no se pierde
                ch.basic_ack(delivery_tag = method.delivery_tag)
            
            channel.basic_qos(prefetch_count=1) # Hasta no terminar la tarea, Rabbit no envia otra al worker
            # Ver que esto puede dar error de llenar la queue despues
            channel.basic_consume(queue=self.queue_name,
                        on_message_callback=callback) # Saco el ACK automatico

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
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

    def start_consuming(self, on_message_callback):
        pass
    
    def stop_consuming(self):
        pass

    # Sender de Hola Mundo
    def send(self, message):
        # Aca conecto a un broker de localhost
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.host)) # POner IP de otra maquina para enviarlo ahi
        channel = connection.channel()

        # Declaro la queue a la que envio los mensajes
        channel.queue_declare(queue=self.queue_name, durable=True, arguments={'x-queue-type': 'quorum'})

        # Mando un mensaje o el hola mundo
        message = ' '.join(sys.argv[1:]) or "Hello World!"
        channel.basic_publish(exchange=self.exchange_name,
                      routing_key=self.routing_keys,
                      body=message,
                      properties=pika.BasicProperties( # Hago que los mensajes sean persistentes
                         delivery_mode = pika.DeliveryMode.Persistent # Ver el error de que queden en cache si pasa algo raro
                      ))
        print(" [x] Sent 'Hello World!'")

        # Para vaciar buffers de red y garantizar envio de mensaje a rabbit
        connection.close()

    def close(self):
        pass
