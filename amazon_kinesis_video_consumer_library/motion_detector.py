import io
import cv2
import logging
import numpy as np
import imageio.v3 as iio

# Init the logger.
log = logging.getLogger(__name__)

class MotionDetector():

    def get_frames_as_ndarray(self, fragment_bytes, one_in_frames_ratio):
        '''
        Parses fragment_bytes and returns a ratio of available frames in the MKV fragment as
        a list of numpy.ndarray's.

        e.g: Setting one_in_frames_ratio = 5 will return every 5th frame found in the fragment.
        (Starting with the first)

        To return all available frames just set one_in_frames_ratio = 1

        ### Parameters:

            fragment_bytes: bytearray
                A ByteArray with raw bytes from exactly one fragment.

            one_in_frames_ratio: Str
                Ratio of the available frames in the fragment to process and return.

        ### Return:

            frames: List<numpy.ndarray>
            A list of frames extracted from the fragment as numpy.ndarray
        
        '''

        # Parse all frames in the fragment to frames list
        frames = iio.imread(io.BytesIO(fragment_bytes), plugin="pyav", index=...)
        log.info(f"HAY {len(frames)}FRAMES")
        # Store and return frames in frame ratio of total available 
        ret_frames = []
        for i in range(0, len(frames), one_in_frames_ratio):
            ret_frames.append(frames[i])

        return ret_frames

    def save_frames_as_jpeg(self, ndarray_frames, jpg_file_base_path):
        '''
        Saves frames as
        JPEGs on the local disk.
        ### Parameters:

        ndarray_frames: List<numpy.ndarray>
            A ByteArray with raw bytes from exactly one fragment.

        ### Return
        jpeg_paths : List<Str>
            A list of file paths to the saved JPEN files. 
        
        '''

        # Write frames to disk as JPEG images
        jpeg_paths = []
        for i in range(len(ndarray_frames)):
            frame = ndarray_frames[i]
            image_file_path = '{}-{}.jpg'.format(jpg_file_base_path, i)
            iio.imwrite(image_file_path, frame, format=None)
            jpeg_paths.append(image_file_path)
        
        return jpeg_paths

    def frame_differencing(self, frames, threshold=90, min_motion_pixels=3500):
        """
        Analiza una lista de frames para detectar movimiento.

        ### Parámetros:
        - frames: List[numpy.ndarray]
            Lista de frames en formato numpy.ndarray (preprocesados).
        - threshold: int, opcional
            Umbral para la diferencia de píxeles entre frames. Default es 30.
        - min_motion_pixels: int, opcional
            Número mínimo de píxeles cambiados para considerar que hay movimiento. Default es 500.

        ### Retorno:
        - motion_frames: List[int]
            Lista de índices de los frames donde se detectó movimiento.
        """
        prev_frame = None
        motion_frames = []

        for idx, frame in enumerate(frames):
            # Convertir el frame a escala de grises
            frame_gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

            if prev_frame is not None:
                # Calcular diferencia entre el frame actual y el anterior
                diff = cv2.absdiff(prev_frame, frame_gray)
                
                # Umbralización
                _, diff_thresh = cv2.threshold(diff, threshold, 255, cv2.THRESH_BINARY)

                # # Dilatar para ampliar las áreas detectadas
                # diff_thresh = cv2.dilate(diff_thresh, None, iterations=2)

                # Contar píxeles de movimiento
                non_zero_count = cv2.countNonZero(diff_thresh)
                log.info(f"Frame {idx} con pixeles valiosos {non_zero_count}")
                # Detectar si hay movimiento
                if non_zero_count > min_motion_pixels:
                    motion_frames.append(frame)

            # Actualizar el frame anterior
            prev_frame = frame_gray

        return motion_frames
