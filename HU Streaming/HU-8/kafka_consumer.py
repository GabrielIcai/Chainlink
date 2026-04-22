import json
import requests
from kafka import KafkaConsumer
from requests.auth import HTTPBasicAuth

#Kafka
BOOTSTRAP_SERVERS = '192.168.80.34:9092'
TOPIC_LINK        = 'gittba10_LINK'
TOPIC_VWAP        = 'gittba10_LINK_VWAP'
GROUP_ID          = 'gittba10_LINK_elastic_group' #para identificarnos ante kafka

#Elasticsearch
ES_URL      = 'http://192.168.80.37:9201'
ES_USER     = 'elastic'
ES_PASSWORD = 'pass4icai'
HEADERS     = {'Content-Type': 'application/json'}

#Índices
INDEX_LINK = 'gittba_link'
INDEX_VWAP = 'gittba_link_vwap'


def insert_document(index, document):
    """Inserta un documento JSON en el índice indicado de Elasticsearch."""
    url = f'{ES_URL}/{index}/_doc'
    response = requests.post(
        url,
        data=json.dumps(document),
        headers=HEADERS,
        auth=HTTPBasicAuth(ES_USER, ES_PASSWORD)
    )
    res = response.json()
    print(f'[{index}] Insertado: {res.get("result", res)}')


def main():
    # Consumer suscrito a los dos topics a la vez
    consumer = KafkaConsumer(
        TOPIC_LINK,
        TOPIC_VWAP,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )

    print(f'Escuchando topics: {TOPIC_LINK}, {TOPIC_VWAP}')

    try:
        for message in consumer:
            topic   = message.topic
            doc     = message.value      # ya es un dict Python
            key     = message.key

            print(f'\nTopic: {topic} | Key: {key} | Mensaje: {doc}')

            # El documento ya viene con @timestamp desde la HU-6 y HU-7,
            # así que lo insertamos directamente
            if topic == TOPIC_LINK:
                insert_document(INDEX_LINK, doc)

            elif topic == TOPIC_VWAP:
                # El VWAP tiene window_start y window_end.
                # Usamos window_end como @timestamp si no viene explícito
                if '@timestamp' not in doc:
                    doc['@timestamp'] = doc.get('window_end')
                insert_document(INDEX_VWAP, doc)

    except KeyboardInterrupt:
        print('\nParando el consumer...')
    finally:
        consumer.close()


if __name__ == '__main__':
    main()