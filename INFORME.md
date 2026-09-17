Para este trabajo no se exige redactar un informe, pero pueden documentarse decisiones de diseño en este archivo.

Voy a dejar algunos comentarios sobre lo hecho en el tp. Casi todo el codigo y como construir la solucion se baso en el tutorial provisto por la catedra en el Readme sobre rabbit.
Las funciones lambda utilizando el ack/nack_basic fueron de las ultimas funcionalidades agregadas. La idea es que la funcion ack/nack utilice metodos de pika para encapsular su funcionamiento en el middleware, y para el exterior se desconozca como se devuelve el ack/nack.
La decision de que el tipo de exchange sea direct y no fanout es porque la clase exchange recibe routing_keys en la firma. Esto indica que hay que bindear la cola a N routing_keys, y luego en el send enviara el message a toda queue bindeada a una key especifica. De momento, como send no tiene en la firma un key, envio a todas las keys el mensaje.
En los init no se pedia levantar ningun error, pero se crea un channel y un connection. Como puede fallar cualquiera, capturo el error levantando MessageMiddlewareDisconnectedError.
Por ultimo, se realizo el manejo de errores. Al principio, capturaba todo con Except. Para evitar esto y solo levantar los errores ante problemas con pika, ahora solo capturo errores del tipo AMQPError y AMQPConnectionError. 
