import os
import sys
import time
import boto3
import logging
from amazon_kinesis_video_consumer_library.kinesis_video_streams_parser import KvsConsumerLibrary
from amazon_kinesis_video_consumer_library.kinesis_video_fragment_processor import KvsFragementProcessor
from amazon_kinesis_video_consumer_library.motion_detector import MotionDetector

# Config the logger.
log = logging.getLogger(__name__)
logging.basicConfig(format="[%(name)s.%(funcName)s():%(lineno)d] - [%(levelname)s] - %(message)s", 
                    stream=sys.stdout, 
                    level=logging.INFO)

# Update the desired region and KVS stream name.
KVS_STREAM01_NAME = 'camera_cinnado'  # Replace with your KVS stream name

class KvsPythonConsumerExample:
    '''
    Example class to demonstrate usage the AWS Kinesis Video Streams KVS) Consumer Library for Python.
    '''

    def __init__(self):
        '''
        Initialize the KVS clients as needed. The KVS Comsumer Library intentionally does not abstract 
        the KVS clients or the various media API calls. These have individual authentication configuration and 
        a variety of other user defined settings so we keep them here in the users application logic for configurability.

        The KvsConsumerLibrary sits above these and parses responses from GetMedia and GetMediaForFragmentList 
        into MKV fragments and provides convenience functions to further process, save and extract individual frames.  
        '''

        # Create shared instance of KvsFragementProcessor
        self.kvs_fragment_processor = KvsFragementProcessor()

        # Variable to maintaun state of last good fragememt mostly for error and exception handling.
        self.last_good_fragment_tags = None

        # Init the KVS Service Client and get the accounts KVS service endpoint
        log.info('Initializing Amazon Kinesis Video client....')
        self.session = boto3.Session()
        self.kvs_client = self.session.client("kinesisvideo")
        self.motion_detector = MotionDetector()


    ####################################################
    # Main process loop
    def service_loop(self):
        
        ####################################################
        # Start an instance of the KvsConsumerLibrary reading in a Kinesis Video Stream

        # Get the KVS Endpoint for the GetMedia Call for this stream
        log.info(f'Getting KVS GetMedia Endpoint for stream: {KVS_STREAM01_NAME} ........')
        get_media_endpoint = self._get_data_endpoint(KVS_STREAM01_NAME, 'GET_MEDIA')
        
        # Get the KVS Media client for the GetMedia API call
        log.info(f'Initializing KVS Media client for stream: {KVS_STREAM01_NAME}........')
        kvs_media_client = self.session.client('kinesis-video-media', endpoint_url=get_media_endpoint)

        # Make a KVS GetMedia API call with the desired KVS stream and StartSelector type and time bounding.
        log.info(f'Requesting KVS GetMedia Response for stream: {KVS_STREAM01_NAME}........')
        get_media_response = kvs_media_client.get_media(
            StreamName=KVS_STREAM01_NAME,
            StartSelector={
                'StartSelectorType': 'NOW'
            }
        )

        # Initialize an instance of the KvsConsumerLibrary, provide the GetMedia response and the required call-backs
        log.info(f'Starting KvsConsumerLibrary for stream: {KVS_STREAM01_NAME}........')
        my_stream01_consumer = KvsConsumerLibrary(KVS_STREAM01_NAME, 
                                              get_media_response, 
                                              self.on_fragment_arrived, 
                                              self.on_stream_read_complete, 
                                              self.on_stream_read_exception
                                            )

        # Start the instance of KvsConsumerLibrary, any matching fragments will begin arriving in the on_fragment_arrived callback
        my_stream01_consumer.start()

        # Can create another instance of KvsConsumerLibrary on a different media stream or continue on to other application logic. 

        # Here can hold the process up by waiting for the KvsConsumerLibrary thread to finish (may never finish for live streaming fragments)
        #my_stream01_consumer.join()

        # Or 
    
        # Run a loop with the applications main functionality that holds the process open.
        # Can also use to monitor the completion of the KvsConsumerLibrary instance and trigger a required action on completion.
        while True:
            log.info("Main application loop running...")
            time.sleep(5)
            
            
            # Call below to exit the streaming get_media() thread gracefully before reaching end of stream. 
            #my_stream01_consumer.stop_thread()


    ####################################################
    # KVS Consumer Library call-backs


            # Call below to exit the streaming get_media() thread gracefully before reaching end of stream. 
            #my_stream01_consumer.stop_thread()


    ####################################################
    # KVS Consumer Library call-backs

    def on_fragment_arrived(self, stream_name, fragment_bytes, fragment_dom, fragment_receive_duration):
        try:
            # Log the arrival of a fragment. 
            # use stream_name to identify fragments where multiple instances of the KvsConsumerLibrary are running on different streams.
            start_time = time.time() 
            log.info(f'\n\n##########################\nFragment Received on Stream: {stream_name}\n##########################')
            log.info(f'####### Fragment Receive and Processing Duration: {fragment_receive_duration} Secs')
            self.last_good_fragment_tags = self.kvs_fragment_processor.get_fragment_tags(fragment_dom)

            log.info('')
            log.info(f'####### Fragment MKV Tags:')
            # for key, value in self.last_good_fragment_tags.items():
                # log.info(f'{key} : {value}')

            # Define the directory to save frames
            save_dir = 'frames/'
            if not os.path.exists(save_dir):
                os.makedirs(save_dir)

            # Save frames from fragment as JPEG images
            one_in_frames_ratio = 5
            jpg_file_base_name = self.last_good_fragment_tags['AWS_KINESISVIDEO_FRAGMENT_NUMBER']
            jpg_file_base_path = os.path.join(save_dir, jpg_file_base_name)

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
            frames = self.motion_detector.get_frames_as_ndarray(fragment_bytes, 5)
            motion_frames = self.motion_detector.frame_differencing(frames)
            self.motion_detector.save_frames_as_jpeg(motion_frames, path)
            processing_duration = time.time() - start_time
            print(f"Procesamiento de movimiento completado en {processing_duration:.4f} segundos.")
            
            # Pasar los resultados al hilo principal usando la cola
            return motion_frames

        except Exception as e:
            print(f"Error en el proceso hijo: {e}")
    
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
