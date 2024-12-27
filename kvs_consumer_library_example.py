import os
import sys
import json
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
        self.rekognition_client = self.session.client("rekognition")
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

    def on_fragment_arrived(self, stream_name, fragment_bytes, fragment_dom, fragment_receive_duration):
        '''
        This is the callback for the KvsConsumerLibrary to send MKV fragments as they are received from a stream being processed.
        The KvsConsumerLibrary returns the received fragment as raw bytes and a DOM like structure containing the fragments meta data.

        With these parameters you can do a variety of post-processing using the KvsFragementProcessor including saving the fragment as a
        standalone MKV file to local disk, request individual frames as a numpy.ndarray for data science applications or as JPEG/PNG files to
        save to disk or pass to computer vison solutions. Finally, you can also use the Fragment DOM to access Meta-Data such as the MKV tags
        as well as track ID and codec information. 

        In the below example we apply motion detection through the numpy.ndarray frames and image recognition using an AWS Rekognition client.

        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary thread triggering this callback was initiated.
                Use this to identify a fragment when multiple streams are read from different instances of KvsConsumerLibrary to this callback.

            **fragment_bytes**: bytearray
                A ByteArray with raw bytes from exactly one fragment. Can be save or processed to access individual frames

            **fragment_dom**: mkv_fragment_doc: ebmlite.core.Document <ebmlite.core.MatroskaDocument>
                A DOM like structure of the parsed fragment providing searchable list of EBML elements and MetaData in the Fragment

            **fragment_receive_duration**: float
                The time in seconds that the fragment took for the streaming data to be received and processed. 
        
        '''
        try:
            # Log the arrival of a fragment. 
            # use stream_name to identify fragments where multiple instances of the KvsConsumerLibrary are running on different streams.
            start_time = time.time() 
            log.info(f'\n\n##########################\nFragment Received on Stream: {stream_name}\nProcessing Duration: {fragment_receive_duration} Secs\n##########################')
            self.last_good_fragment_tags = self.kvs_fragment_processor.get_fragment_tags(fragment_dom)

            # log.info(f'####### Fragment MKV Tags:')
            # for key, value in self.last_good_fragment_tags.items():
            #     log.info(f'{key} : {value}')

            # Define the directory to save frames
            save_dir_frames = 'frames/'
            if not os.path.exists(save_dir_frames):
                os.makedirs(save_dir_frames)
            save_dir_labels = 'bounding_boxes/'
            if not os.path.exists(save_dir_labels):
                os.makedirs(save_dir_labels)
            save_dir_detections = 'detections/'
            if not os.path.exists(save_dir_detections):
                os.makedirs(save_dir_detections)

            # Using the fragment number as a key/name for the frames
            jpg_file_base_name = self.last_good_fragment_tags['AWS_KINESISVIDEO_FRAGMENT_NUMBER']
            jpg_file_base_path = os.path.join(save_dir_frames, jpg_file_base_name)
            labels_file_base_path = os.path.join(save_dir_labels, jpg_file_base_name)
            detections_file_base_path = os.path.join(save_dir_detections, jpg_file_base_name)
            
            motion_frames = self.process_fragment_frames(fragment_bytes, jpg_file_base_path, labels_file_base_path, detections_file_base_path)
            print(f"Resultado del procesamiento: {'Movimiento Detectado' if len(motion_frames) > 0 else '[]'}")
            processing_duration = time.time() - start_time
            print(f"Callback completado en {processing_duration:.4f} segundos.")

        except Exception as err:
            log.error(f'on_fragment_arrived Error: {err}')

    def process_fragment_frames(self, fragment_bytes, frames_path, labels_path, detections_path):
        try:
            start_time = time.time()
            # Obtains the frames of the fragment in a numpy array type
            frames = self.kvs_fragment_processor.get_frames_as_ndarray(fragment_bytes, one_in_frames_ratio=5)
            # Detects which frames of the fragment have motion
            motion_frames = self.motion_detector.frame_differencing(frames)
            if len(motion_frames) > 0 :
                # Gets the AWS Rekognition response with the labels
                labels_fragment = self.get_labels_from_frames(motion_frames, labels_path)
                # Parse the Rekognition Response
                fragment_bounding_boxes = self.get_bounding_boxes(labels_fragment)
                # Gets the frames with a mask of the bounding boxes added
                detection_frames = self.kvs_fragment_processor.get_frames_with_bounding_boxes(motion_frames, fragment_bounding_boxes)
                self.kvs_fragment_processor.save_frames_as_jpeg(motion_frames, frames_path)
                self.kvs_fragment_processor.save_frames_as_jpeg(detection_frames, detections_path)
                processing_duration = time.time() - start_time
                print(f"Procesamiento de movimiento completado en {processing_duration:.4f} segundos.")
            
            return motion_frames

        except Exception as e:
            print(f"Error en el procesamiento del fragmento: {e}")
    
    def get_labels_from_frames(self, frames, save_path):
        labels = []
        frames_jpeg = self.kvs_fragment_processor.get_ndarray_frames_to_jpeg(frames)
        for idx, jpeg in enumerate(frames_jpeg):
            response = self.rekognition_client.detect_labels(
                        Image={'Bytes': jpeg},
                        MaxLabels=10,
                        MinConfidence=80
            )
            with open(f"{save_path}_{idx}.json", "w") as f:
                f.write(json.dumps(response))
            labels.append(response["Labels"])
        return labels
                
    def get_bounding_boxes(self, labels_fragment):
        """
        Parse the Rekognition response. We want the youngest labels in terms of relative labels. 
        Ex: We can have a Person but also it could be an Adult or a Male and more specifically it would be a Man
        So we keep the labels that has instances and dont have childs with instances
        """
        fragment_bounding_boxes = []
        valid_fragment_labels = []
        # Getting all the lables with instances that dont have childs with instances
        for labels_frame in labels_fragment:
            parents = set()
            child_labels = []
            for label in labels_frame:
                if len(label["Instances"]) > 0:
                    if label["Name"] not in parents:
                        child_labels.append(label)
                        for parent in label["Parents"]:
                            parents.add(parent["Name"])
            valid_fragment_labels.append([label for label in child_labels if label["Name"] not in parents])
        # Prettying the labels to get only the keys related to the BBoxes
        for labels_frame in valid_fragment_labels:
            frame_bounding_boxes = []
            for label in labels_frame:
                for instance in label["Instances"]:
                    frame_bounding_boxes.append(
                        {
                            "Name" : label["Name"],
                            "Bounding_box": instance["BoundingBox"],
                            "Confidence": instance["Confidence"],
                        }
                    )
            fragment_bounding_boxes.append(frame_bounding_boxes)

        print(fragment_bounding_boxes)
        return fragment_bounding_boxes
                            
    
    def on_stream_read_complete(self, stream_name):
        '''
        This callback is triggered by the KvsConsumerLibrary when a stream has no more fragments available.
        This represents a graceful exit of the KvsConsumerLibrary thread.

        A stream will reach the end of the available fragments if the StreamSelector applied some 
        time or fragment bounding on the media request or if requesting a live steam and the producer 
        stopped sending more fragments. 

        Here you can choose to either restart reading the stream at a new time or just clean up any
        resources that were expecting to process any further fragments. 
        
        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary thread triggering this callback was initiated.
                Use this to identify a fragment when multiple streams are read from different instances of KvsConsumerLibrary to this callback.
        '''

        # Do something here to tell the application that reading from the stream ended gracefully.
        print(f'Read Media on stream: {stream_name} Completed successfully - Last Fragment Tags: {self.last_good_fragment_tags}')
        log.info(f'Read Media on stream: {stream_name} Completed successfully - Last Fragment Tags: {self.last_good_fragment_tags}')

    def on_stream_read_exception(self, stream_name, error):
        '''
        This callback is triggered by an exception in the KvsConsumerLibrary reading a stream. 
        
        For example, to process use the last good fragment number from self.last_good_fragment_tags to
        restart the stream from that point in time with the example stream selector provided below. 
        
        Alternatively, just handle the failed stream as per your application logic requirements.

        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary thread triggering this callback was initiated.
                Use this to identify a fragment when multiple streams are read from different instances of KvsConsumerLibrary to this callback.

            **error**: err / exception
                The Exception obje tvthat was thrown to trigger this callback.

        '''

        # Can choose to restart the KvsConsumerLibrary thread at the last received fragment with below example StartSelector
        #StartSelector={
        #    'StartSelectorType': 'FRAGMENT_NUMBER',
        #    'AfterFragmentNumber': self.last_good_fragment_tags['AWS_KINESISVIDEO_CONTINUATION_TOKEN'],
        #}

        # Here we just log the error 
        print(f'####### ERROR: Exception on read stream: {stream_name}\n####### Fragment Tags:\n{self.last_good_fragment_tags}\nError Message:{error}')

    def _get_data_endpoint(self, stream_name, api_name):
        '''
        Convenience method to get the KVS client endpoint for specific API calls. 
        '''
        response = self.kvs_client.get_data_endpoint(
            StreamName=stream_name,
            APIName=api_name
        )
        return response['DataEndpoint']

if __name__ == "__main__":
    kvsConsumerExample = KvsPythonConsumerExample()
    kvsConsumerExample.service_loop()
