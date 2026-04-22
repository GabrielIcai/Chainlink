import json
from datetime import datetime, timezone

from binance import ThreadedWebsocketManager
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "192.168.80.34:9092"
TOPIC = "gittba10_LINK"
SYMBOL = "LINKUSDT"
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
    try:
        if not isinstance(msg, dict):
            return

        if msg.get("e") != "kline":
            return

        k = msg.get("k")
        if not k:
            return

        # Solo enviar cuando la vela esté cerrada
        if not k.get("x"):
            return

        symbol = k["s"]
        ts_ms = int(k["T"])

        payload = {
        "symbol": symbol,
        "@timestamp": ms_to_utc_string(ts_ms),
        
        # AÑADIR ESTOS:
        "open":   float(k["o"]),
        "high":   float(k["h"]),
        "low":    float(k["l"]),
        "close":  float(k["c"]),
        "volume": float(k["v"]),
        "trades": int(k["n"]),      # número de trades en la vela (opcional pero útil)
        }

        producer.send(
            TOPIC,
            key=symbol,
            value=payload,
            timestamp_ms=ts_ms
        )

        print(f"[PRODUCER] topic={TOPIC} key={symbol} value={payload} timestamp_ms={ts_ms}")

    except Exception as e:
        print(f"[PRODUCER] Error procesando mensaje: {e}")

def main():
    twm = ThreadedWebsocketManager()
    twm.start()

    twm.start_kline_socket(
        callback=handle_message,
        symbol=SYMBOL,
        interval=INTERVAL
    )

    try:
        input("Escuchando Binance. Pulsa ENTER para salir...\n")
    finally:
        producer.flush()
        twm.stop()
        producer.close()

if __name__ == "__main__":
    main()