import json
import asyncio
import websockets
from threading import Thread

MSG = [
    [
        {
            "Name": "Person",
            "Bounding_box": {
                "Width": 0.9989283680915833,
                "Height": 0.9963576197624207,
                "Left": 0.00043359556002542377,
                "Top": 0.000661010795738548
            },
            "Confidence": 99.42082214355469
        },
        {
            "Name": "Glasses",
            "Bounding_box": {
                "Width": 0.3723424971103668,
                "Height": 0.19906555116176605,
                "Left": 0.22577500343322754,
                "Top": 0.23328320682048798
            },
            "Confidence": 97.55232238769531
        }
    ]
]

class WebSocketServer(Thread):
    def __init__(self, host="localhost", port=8765):
        super().__init__()
        self.host = host
        self.port = port
        self._loop = asyncio.new_event_loop()
        self.clients = set()
        self._stop_event = False

    async def handler(self, websocket, path):
        self.clients.add(websocket)
        print(f"Nuevo cliente conectado. Total clientes: {len(self.clients)}")
        try:
            async for message in websocket:
                print(f"Mensaje recibido: {message}")
        except websockets.exceptions.ConnectionClosed:
            print("Cliente desconectado")
        finally:
            self.clients.remove(websocket)
            print(f"Cliente desconectado. Total clientes: {len(self.clients)}")

    async def send_message(self, message):
        if self.clients:
            await asyncio.gather(*(client.send(message) for client in self.clients))

    def send_message_sync(self, message):
        print("Mando mensaje")
        asyncio.run_coroutine_threadsafe(self.send_message(message), self._loop)

    async def _start_server(self):
        print(f"Servidor WebSocket escuchando en ws://{self.host}:{self.port}")
        async with websockets.serve(self.handler, self.host, self.port):
            while not self._stop_event:
                await asyncio.sleep(0.1)

    def stop_server(self):
        self._stop_event = True

    def run(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._start_server())

# Clase principal donde se inicializa el WebSocket Server
if __name__ == "__main__":
    ws_server = WebSocketServer(host="localhost", port=8765)

    try:
        ws_server.start()
        print("Servidor WebSocket en ejecución. Presiona Ctrl+C para detener.")

        while True:
            mensaje = input("Escribe un mensaje para enviar a los clientes: ")
            ws_server.send_message_sync(json.dumps(MSG))  # Este es el mensaje a enviar

    except KeyboardInterrupt:
        print("\nDeteniendo el servidor WebSocket...")
        ws_server.stop_server()
        ws_server.join()
        print("Servidor WebSocket detenido.")
