# -*- coding: utf-8 -*-
from binance import Client
from binance import ThreadedWebsocketManager

def handle_kline(msg):
    k = msg['k']
    print(
        f"Intervalo: {k['i']} | "
        f"Cierre: {k['c']} | "
        f"Volumen: {k['v']}"
    )

twm = ThreadedWebsocketManager()
twm.start()

twm.start_kline_socket(
    symbol='BTCUSD', ##debemos poner aqui nuestro simbolo
    interval=Client.KLINE_INTERVAL_1MINUTE, ##intervalo (tamaño de vela) que queremos que nos de binance
    #empezaremos con velas de 1 minuto y luego podemos probar con otras
    callback=handle_kline
)

##tenemos que conectar binance producer para cada vez que recibamos algo de binance lo publicamos en nuestro topic de kafka
##queremos publicar que en la clave este el simbolo de nuestra criptomoneda y en el json queremos meter simbol, timestamp con el formato de la practica, 
##asi como lo demas porque grafana y kivana neceitan estos formatos para hacer los cálculso
##los timestamp deben ser los mismos
##binance aunque pongamos intervalos de un minuto no espera aunque la vela este cerrada, va enviando actualizaciones
##nosotros no queremos esto, queremos el ultimo valor de la vela, solo queremos que lo envie cuando la vela este cerrada
##entonces tenemos que mirar el campo k['x'] que es un booleano que nos dice si la vela esta cerrada o no, si esta cerrada 
##lo publicamos en kafka y si no esta cerrada no hacemos nada
##hay dos timestamp, uno en el cuerpo del mensaje y otro para las herramientas posteriores
##kafka aunque sea una arquitectura en streaming necesito recibir y almacenar los eventos en el cluster kafka ya que todavia no hay un consumidor
##en el consumidor vamos a procesar los mensajes, que hara procesamiento en tiempo real y el consumidor y producer estarán hablando
##despues tambien usaremos herramientas para visualizar en tiempo real (kivana) la cotización de la criptomoneda, pero eso lo haremos en los siguientes sprints
##si tu no consumes en kafka no puedes ver lo que hay por lo que podemos usar el consumer de pythton o el de linea de comandos
##necesitamos que el timestamp venga de kafka, el producto tiene que asociar el timestamp de binance de cada mensaje, porque si dejo a kafka que ponga el timestamp 
##habra una diferencia entre cuando cierra la vela y cuando llega a kafka, entonces el timestamp que pongamos en el mensaje de binance tiene que ser el mismo que el 
##que pongamos en kafka, para eso tenemos que usar el timestamp de binance y no el de kafka
##esto es porque en real time hay muchos problemas de timestamp y cuando hagamos ventanas debemos tener cuidado y asegurarnos de tener el timestamp correcto
##el timestamp siempre en UTC
##el objetivo es fijar conceptos de kafka


input("Pulsa ENTER para salir\n")
twm.stop()