import json
from datetime import datetime, timezone

from binance import ThreadedWebsocketManager
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "192.168.80.34:9092"
TOPIC = "gittba10_link"   # lo hemos hecho primero con test y ahora con el real
SYMBOL = "LINKUSDT"       # Chainlink
INTERVAL = "1m"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda v: v.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def ms_to_utc_string(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def handle_message(msg):
    if msg.get("e") != "kline":
        return

    k = msg["k"]

    # Solo enviamos cuando la vela esté cerrada
    if not k["x"]:
        return

    symbol = k["s"]          # LINKUSDT
    ts_ms = int(k["T"])      # timestamp de cierre de vela en ms

    payload = {
        "symbol": symbol,
        "@timestamp": ms_to_utc_string(ts_ms),
        "close": float(k["c"]),
        "volume": float(k["v"])
    }

    producer.send(
        TOPIC,
        key=symbol,
        value=payload,
        timestamp_ms=ts_ms
    )
    producer.flush()

    print("Mensaje enviado a Kafka")
    print(f"Topic: {TOPIC}")
    print(f"Key: {symbol}")
    print(f"Value: {payload}")
    print(f"timestamp_ms: {ts_ms}")
    print()

def main():
    twm = ThreadedWebsocketManager()
    twm.start()

    twm.start_kline_socket(
        callback=handle_message,
        symbol=SYMBOL,
        interval=INTERVAL
    )

    input("Escuchando Binance. Pulsa ENTER para salir...\n")

    twm.stop()
    producer.close()

if __name__ == "__main__":
    main()