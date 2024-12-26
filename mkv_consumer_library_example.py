import os
import sys
import time
import logging
import threading
from multiprocessing import Process, Queue
from amazon_kinesis_video_consumer_library.mkv_parser import MkvParser
from amazon_kinesis_video_consumer_library.motion_detector import MotionDetector

# Configuración del logger.
log = logging.getLogger(__name__)
logging.basicConfig(format="[%(name)s.%(funcName)s():%(lineno)d] - [%(levelname)s] - %(message)s", 
                    stream=sys.stdout, 
                    level=logging.INFO)

KVS_STREAM01_NAME = 'camera_cinnado'  # Nombre de tu stream de KVS
total_frames = 0
class KvsPythonConsumerExample:
    def __init__(self):
        self.motion_detector = MotionDetector()
        self.last_good_fragment_tags = None

    def service_loop(self):
        log.info(f'Starting MkvParser for stream: {KVS_STREAM01_NAME}........')
        my_stream01_consumer = MkvParser(
                                              "./mkv/video1.mkv", 
                                              self.on_fragment_arrived, 
                                              self.on_stream_read_complete, 
                                              self.on_stream_read_exception
                                            )
        my_stream01_consumer.start()

        while True:
            log.info(f"Main application loop running...{total_frames}")
            time.sleep(5)

    def on_fragment_arrived(self, fragment_bytes, fragment_dom, fragment_receive_duration):
        try:
            # Log para verificar el hilo en el que se ejecuta el callback
            start_time = time.time()

            print(f"Callback ejecutándose en el hilo: {threading.current_thread().name}")
            log.info(f'\n\n##########################\nFragment Received on Stream: {KVS_STREAM01_NAME}\n##########################\nGETTING FRAGMENT Duration: {fragment_receive_duration} Secs')

            # Directorios para guardar los frames
            save_dir = 'frames/'
            motion_dir = 'motion_frames/'
            if not os.path.exists(save_dir):
                os.makedirs(save_dir)
            if not os.path.exists(motion_dir):
                os.makedirs(motion_dir)

            # Guardar frames del fragmento como imágenes JPEG
            one_in_frames_ratio = 30
            jpg_file_base_name = str(fragment_dom["metadata"]["timestamp"])
            jpg_file_base_path = os.path.join(save_dir, jpg_file_base_name)
            jpg_file_motion_path = os.path.join(motion_dir, jpg_file_base_name)


            # Crear proceso hijo para realizar la detección de movimiento sin bloquear el hilo de la cola
            motion_frames =self.process_frame_in_subprocess(fragment_bytes, jpg_file_base_path)
            print(f"Resultado del procesamiento: {motion_frames}")
            processing_duration = time.time() - start_time
            print(f"Callback completado en {processing_duration:.4f} segundos.")

        except Exception as err:
            log.error(f'on_fragment_arrived Error: {err}')

    def process_frame_in_subprocess(self, fragment_bytes, path):
        """Función que ejecuta el procesamiento intensivo de los frames en un proceso hijo"""
        try:
            start_time = time.time()
            # Aquí iría tu procesamiento intensivo de CPU, como la detección de movimiento
            frames = self.motion_detector.get_frames_as_ndarray(fragment_bytes, 30)
            motion_frames = self.motion_detector.frame_differencing(frames)
            self.motion_detector.save_frames_as_jpeg(motion_frames, path)
            processing_duration = time.time() - start_time
            print(f"Procesamiento de movimiento completado en {processing_duration:.4f} segundos.")
            
            # Pasar los resultados al hilo principal usando la cola
            return motion_frames

        except Exception as e:
            print(f"Error en el proceso hijo: {e}")

    def on_stream_read_complete(self, stream_name, start_time):
        fragment_receive_duration = time.time() - start_time  # Tiempo total del procesamiento

        log.info(f'Read Media on stream: {stream_name} Completed successfully - TOTAL TIME: {fragment_receive_duration}')

    def on_stream_read_exception(self, stream_name, error):
        log.error(f'####### ERROR: Exception on read stream: {stream_name}\n####### Fragment Tags:\n{self.last_good_fragment_tags}\nError Message:{error}')

    def _get_data_endpoint(self, stream_name, api_name):
        response = self.kvs_client.get_data_endpoint(
            StreamName=stream_name,
            APIName=api_name
        )
        return response['DataEndpoint']

if __name__ == "__main__":
    kvsConsumerExample = KvsPythonConsumerExample()
    kvsConsumerExample.service_loop()
