Para este trabajo no se exige redactar un informe, pero pueden documentarse decisiones de diseño en este archivo.

Voy a dejar algunos comentarios porque el tp era corto. Casi todo el codigo del tp y como construir la solucion se logro siguiendo el tutorial provisto por la catedra en el Readme sobre rabbit.
Algunas cosas como las funciones lambda utilizando el ack/nack_basic fueron las ultimas a agregar. La idea es que la funcion ack/nack utilice metodos de pika para encapsular su funcionamiento en el middleware, y para el exterior se desconozca como se devuelve el ack/nack.
Por ultimo, se realzio el manejo de errores. Al principio, capturaba todo con Except. Para evitar esto y solo levantar los errores ante probelmas con pika, ahora solo capturo errores del tipo AMQPError. 
En los init no se pedia levantar ningun error, pero se crea un channel y un connection. Como puede fallar cualquiera, capturo el error levantando MessageMiddlewareDisconnectedError.
