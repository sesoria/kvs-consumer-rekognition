import timeit  # Permite medir tiempos de ejecución
import logging  # Permite generar logs para el monitoreo del proceso
from threading import Thread  # Herramienta para trabajar con hilos
from queue import Queue  # Cola que facilita la comunicación entre hilos
import av  # PyAV: Librería para decodificar archivos multimedia (MKV en este caso)
import imageio.v3 as iio  # Procesamiento de imágenes y video
import io  # Permite manejar datos en buffers de memoria
import numpy as np  # Librería para operaciones numéricas y manejo eficiente de arrays

# Inicialización del logger para reportar eventos, errores y procesos.
log = logging.getLogger(__name__)

class MkvParser(Thread):
    """
    Clase MkvParser: 
    Lee archivos MKV de manera fragmentada, procesa los frames en grupos y los entrega mediante callbacks.
    Hereda de Thread para permitir la ejecución en un hilo separado.
    """
    def __init__(self,
                 mkv_file_path, 
                 on_fragment_arrived, 
                 on_read_stream_complete, 
                 on_read_stream_exception,
                 frame_group_size=60):
        '''
        Constructor de MkvParser: Inicializa parámetros y callbacks necesarios.

        Parámetros:
            mkv_file_path (str): Ruta del archivo MKV a procesar.
            on_fragment_arrived (function): Callback cuando un fragmento es procesado y listo.
            on_read_stream_complete (function): Callback cuando se finaliza la lectura del archivo.
            on_read_stream_exception (function): Callback cuando ocurre una excepción durante el proceso.
            frame_group_size (int): Cantidad de frames que se agruparán por fragmento. (Default: 60)
        '''
        Thread.__init__(self)  # Inicializa la clase Thread
        self._stop_get_media = False  # Bandera para detener el proceso de lectura

        # Almacena los parámetros de entrada
        self.mkv_file_path = mkv_file_path
        self.on_fragment_arrived_callback = on_fragment_arrived
        self.on_read_stream_complete_callback = on_read_stream_complete
        self.on_read_stream_exception = on_read_stream_exception
        self.frame_group_size = frame_group_size

        # Cola que almacena fragmentos de frames para ser procesados
        self.frame_queue = Queue()

    def stop_thread(self):
        """
        Detiene la ejecución del hilo principal al cambiar la bandera de control.
        """
        self._stop_get_media = True

    def run(self):
        """
        Método principal de ejecución del hilo.
        Lee frames del archivo MKV, los agrupa y los envía a una cola para su procesamiento.
        """
        try:
            log.info(f'Reading MKV file: {self.mkv_file_path}')
            container = av.open(self.mkv_file_path)  # Abre el archivo MKV con PyAV

            # Inicializa un hilo trabajador que procesa la cola de frames
            worker_thread = Thread(target=self.process_queue)
            worker_thread.start()

            frames_accumulated = []  # Lista temporal para acumular frames

            # Itera sobre los frames decodificados del archivo
            for frame in container.decode(video=0):  # Decodifica el primer stream de video (video=0)
                if self._stop_get_media:  # Verifica si debe detenerse la lectura
                    break

                frames_accumulated.append(frame)  # Agrega el frame actual a la lista acumulada

                # Si la lista alcanza el tamaño del grupo, la envía a la cola
                if len(frames_accumulated) >= self.frame_group_size:
                    self.frame_queue.put(frames_accumulated)
                    frames_accumulated = []  # Reinicia la lista acumuladora

            # Envía los frames restantes (si hay) a la cola
            if frames_accumulated:
                self.frame_queue.put(frames_accumulated)

            # Envía señal de finalización al trabajador
            self.frame_queue.put(None)
            worker_thread.join()  # Espera a que el hilo trabajador finalice

            log.info('Stream parsing completed successfully.')
            self.on_read_stream_complete_callback(self.mkv_file_path)

        except Exception as e:
            # Captura y reporta cualquier excepción ocurrida durante el procesamiento
            log.error(f'Error processing MKV stream: {e}', exc_info=True)
            self.on_read_stream_exception(str(e))

    def process_queue(self):
        """
        Procesa los fragmentos de frames almacenados en la cola.
        Convierte los frames en bytes y simula procesamiento adicional.
        """
        while True:
            start_time = timeit.default_timer()  # Marca el tiempo inicial del procesamiento
            item = self.frame_queue.get()  # Obtiene un fragmento de frames de la cola

            if item is None:  # Si recibe None, finaliza el procesamiento
                break

            frames = item  # Lista de frames del fragmento actual

            # Convierte los frames a un único buffer de bytes
            combined_buffer = self.frames_to_bytes(frames)
            fragment_receive_duration = timeit.default_timer() - start_time  # Tiempo total del procesamiento

            # Simula procesamiento y genera un objeto "DOM"
            fragment_dom = self.process_frame_to_dom(combined_buffer)

            # Llama al callback con el fragmento procesado y sus metadatos
            self.on_fragment_arrived_callback(
                combined_buffer,
                fragment_dom,
                fragment_receive_duration
            )

    def frames_to_bytes(self, frames):
        """
        Convierte una lista de av.VideoFrame en un único buffer de bytes.
        
        Parámetros:
            frames (list): Lista de frames (av.VideoFrame) a convertir.

        Retorna:
            bytes: Buffer combinado de los frames en formato JPEG.
        """
        combined_buffer = io.BytesIO()  # Crea un buffer en memoria
        for frame in frames:
            # Convierte cada frame a una imagen JPEG y lo guarda en el buffer
            frame.to_image().save(combined_buffer, format="jpeg")
        return combined_buffer.getvalue()  # Devuelve el buffer como bytes

    def process_frame_to_dom(self, fragment_bytes):
        """
        Simula el procesamiento del fragmento y genera una estructura con metadatos.

        Parámetros:
            fragment_bytes (bytes): Datos binarios del fragmento procesado.

        Retorna:
            dict: Estructura con datos del fragmento y metadatos.
        """
        return {
            "raw_bytes": fragment_bytes,
            "metadata": {
                "size": len(fragment_bytes),  # Tamaño del fragmento en bytes
                "timestamp": timeit.default_timer(),  # Marca de tiempo del procesamiento
            }
        }
