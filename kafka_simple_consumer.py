# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

# Configuración
BOOTSTRAP_SERVERS="192.168.80.34:9092"
TOPIC="gittba10_test"
GROUP_ID="gittba10_group1"

def main() -> None:

    # Crea el KafkaConsumer
    consumer = KafkaConsumer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        group_id=GROUP_ID, ##para decirle cuando consumo quien soy
        auto_offset_reset="latest",
        enable_auto_commit=True, 
        ##siempre cuando yo consumo tengo que decirle a kafka si lo he consumido 
        ##o no porque ese proceso puede fallar, independientemente de que no falle 
        ##que haga autoconsumer
        key_deserializer=lambda v: v.decode("utf-8"), ##como deserializar la clave
        value_deserializer=lambda v: v.decode("utf-8") ##como deserializar el valor (siempre deben ser iguales)
    )

    # Asigna topic y partición
    consumer.assign([TopicPartition(TOPIC, 0)]) 
    #quiero subscribirme a un topic y a una partición concreta, si no se hace esto se suscribe a todos los topics
    #el producer solo tendrá uno pero el consumer puede tener varios, entonces con esto le digo a qué topic y a qué partición quiero suscribirme 
    #(en nuestro caso será el mismo)

    # Lee los mensajes
    records = consumer.poll(timeout_ms=3600.0) 
    #tiempo que espero a mensajes, si no llegan en ese tiempo se cierra el consumidor, si llega un mensaje 
    # antes de ese tiempo se procesa y se vuelve a esperar otro mensaje, si llega otro mensaje antes de que 
    # se procese el primero se procesa el primero y luego el segundo, etc.

    # Procesa los mensajes
    for topic_data, consumer_records in records.items():
        print(topic_data)
        for consumer_record in consumer_records:
            print("key:       " + str(consumer_record.key))
            print("value:     " + str(consumer_record.value))
            print("offset:    " + str(consumer_record.offset))
            print("timestamp: " + str(consumer_record.timestamp))

    #sprint 5 llega hasta consumir el topic

    # Cierra el consumidor
    consumer.close()

if __name__ == "__main__":
    main()