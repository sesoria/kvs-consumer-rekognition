import os
import sys
import time
import boto3
import threading
import logging
from amazon_kinesis_video_consumer_library.mkv_parser import MkvParser
from amazon_kinesis_video_consumer_library.motion_detector import MotionDetector

# Config the logger.
log = logging.getLogger(__name__)
logging.basicConfig(format="[%(name)s.%(funcName)s():%(lineno)d] - [%(levelname)s] - %(message)s", 
                    stream=sys.stdout, 
                    level=logging.INFO)

# Update the desired region and KVS stream name.
KVS_STREAM01_NAME = 'camera_cinnado'  # Replace with your KVS stream name

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
            log.info("Main application loop running...")
            time.sleep(5)

    def on_fragment_arrived(self, fragment_bytes, fragment_dom, fragment_receive_duration):
        try:
            print(f"Callback ejecutándose en el hilo: {threading.current_thread().name}")
            log.info(f'\n\n##########################\nFragment Received on Stream: {KVS_STREAM01_NAME}\n##########################\nProcessing Duration: {fragment_receive_duration} Secs')
            # Define the directory to save frames
            save_dir = 'frames/'
            motion_dir = 'motion_frames/'
            if not os.path.exists(save_dir):
                os.makedirs(save_dir)
            if not os.path.exists(motion_dir):
                os.makedirs(motion_dir)

            # Guardar frames del fragmento como imágenes JPEG
            one_in_frames_ratio = 10
            jpg_file_base_name = str(fragment_dom["metadata"]["timestamp"])
            jpg_file_base_path = os.path.join(save_dir, jpg_file_base_name)
            jpg_file_motion_path = os.path.join(motion_dir, jpg_file_base_name)
            
            
            frames = self.motion_detector.get_frames_as_ndarray(fragment_bytes, one_in_frames_ratio)
            
            self.motion_detector.save_frames_as_jpeg(frames, jpg_file_base_path)
            motion_frames = self.motion_detector.frame_diff(frames)
            # self.motion_detector.save_frames_as_jpeg(motion_frames, jpg_file_motion_path)
            log.info(f'\n\n##########################\nKey Frames on callback: {len(frames)}\n##########################\nMotion Frames on callback: {len(motion_frames)}')

            # log.info(f'####### Saving 1 in {one_in_frames_ratio} Frames from fragment as JPEG to base path: {jpg_file_base_path}')
            # jpeg_paths = self.motion_detector.save_frames_as_jpeg(fragment_bytes, one_in_frames_ratio, jpg_file_base_path)

            # for i, jpeg_path in enumerate(jpeg_paths):
            #     log.info(f'Saved JPEG-{i} Path: {jpeg_path}')

        except Exception as err:
            log.error(f'on_fragment_arrived Error: {err}')

    
    def on_stream_read_complete(self, stream_name):
        log.info(f'Read Media on stream: {stream_name} Completed successfully - Last Fragment Tags: {self.last_good_fragment_tags}')

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
