# Chainlink Big Data Project

Proyecto de arquitectura Big Data para el análisis de datos de trading de criptomonedas, combinando procesamiento **batch y streaming**.

**Alejandra Goetsch, Aurora Junco y Gabriel Llaneza**

## Estructura del proyecto

### 1. Batch (HU Batch)
- Obtención de datos históricos (TradingView)
- Almacenamiento en **HDFS + Hive**
- Arquitectura en capas:
  - **Bronce**: datos raw (CSV)
  - **Plata**: datos procesados (Parquet)
  - **Oro**: datos enriquecidos con indicadores
- Procesamiento con **Spark** (SMA, EMA, RSI, MACD)
- Visualización en **Jupyter**

### 2. Streaming (HU Streaming)
- Ingesta en tiempo real desde **Binance**
- Publicación en **Kafka (topics)**
- Procesamiento con **Spark Structured Streaming**
- Cálculo de métricas en tiempo real (VWAP)
- Consumo y almacenamiento en **Elasticsearch**
- Visualización en **Kibana**

### 3. Organización del repositorio
- `HU Batch/` → procesamiento histórico
- `HU Streaming/` → pipeline en tiempo real
- Scripts Python (producers, consumers)
- Notebooks de Spark y visualización

## Idea clave
Arquitectura Big Data completa:

**Ingesta → Almacenamiento → Procesamiento → Visualización**