import io
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
        print("hola")
        frames = iio.imread(io.BytesIO(fragment_bytes), plugin="pyav", index=...)
        # Store and return frames in frame ratio of total available 
        ret_frames = []
        for i in range(0, len(frames), one_in_frames_ratio):
            ret_frames.append(frames[i])
        print("adios")

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

    def pixel_difference(self, frames, threshold=50):
        significant_frames = [frames[0]]  # Almacena el primer frame como base
        for i in range(1, len(frames)):
            diff = np.abs(frames[i].astype(np.int16) - frames[i-1].astype(np.int16))  # Diferencia de píxeles
            if np.mean(diff) > threshold:  # Compara con el umbral
                significant_frames.append(frames[i])
        return significant_frames
