import os
import sys
import json
import time
import boto3
import logging
from amazon_websocket_apigateway import websocket_apigateway as websocket_ag
from amazon_kinesis_video_consumer_library.motion_detector import MotionDetector
from amazon_kinesis_video_consumer_library.kinesis_video_streams_parser import KvsConsumerLibrary
from amazon_kinesis_video_consumer_library.kinesis_video_fragment_processor import KvsFragementProcessor

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

        # Se inicializa el servidor WebSocket
        self.apigw_client = websocket_ag.client_ag_manegement_api()

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
            fragment_number = self.last_good_fragment_tags['AWS_KINESISVIDEO_FRAGMENT_NUMBER']
            jpg_file_base_path = os.path.join(save_dir_frames, fragment_number)
            labels_file_base_path = os.path.join(save_dir_labels, fragment_number)
            detections_file_base_path = os.path.join(save_dir_detections, fragment_number)
            
            producer_timestamp = self.last_good_fragment_tags['AWS_KINESISVIDEO_PRODUCER_TIMESTAMP']
            
            motion_frames = self.process_fragment_frames(fragment_bytes, jpg_file_base_path, labels_file_base_path, detections_file_base_path, producer_timestamp, fragment_number)
            print(f"Resultado del procesamiento: {'Movimiento Detectado' if len(motion_frames) > 0 else '[]'}")
            processing_duration = time.time() - start_time
            print(f"Callback completado en {processing_duration:.4f} segundos.")

        except Exception as err:
            log.error(f'on_fragment_arrived Error: {err}')

    def process_fragment_frames(self, fragment_bytes, frames_path, labels_path, detections_path, producer_timestamp, fragment_number):
        '''
        Processes the fragment frames for motion detection and object recognition.

        This function extracts frames from the fragment, detects motion, and identifies objects using AWS Rekognition.

        ### Parameters:

            **fragment_bytes**: bytearray
                Raw bytes of the fragment to process.

            **frames_path**: str
                Path to save the extracted frames as images.

            **labels_path**: str
                Path to save the labels detected by AWS Rekognition.

            **detections_path**: str
                Path to save the frames with bounding boxes of detected objects.

        ### Returns:

            **motion_frames**: list
                List of frames where motion was detected.
        '''
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
                # fragment_bounding_boxes, fragment_unique_bboxs = self.get_bounding_boxes(labels_fragment)
                fragment_bounding_boxes = self.get_bounding_boxes(labels_fragment)
                
                # Enviar bounding boxes al frontend mediante AWS
                active_connections = websocket_ag.get_connection_ids_by_stream(KVS_STREAM01_NAME)
                websocket_ag.send_message_to_clients(self.apigw_client, active_connections, {
                    "fragment_number": fragment_number,
                    "timestamp": producer_timestamp,
                    "labels": fragment_bounding_boxes
                })

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
        '''
        Extracts labels from frames using AWS Rekognition, saves the Rekognition response for each frame as a JSON file,
        and collects all the detected labels for further processing.

        ### Parameters:
            **frames**: list of ndarray
                List of frames represented as numpy arrays.

            **save_path**: str
                The base path where JSON files containing the Rekognition response for each frame will be saved.

        ### Returns:
            **labels**: list
                A list of labels detected by Rekognition for each frame.
        '''
        labels = []
        # Convert frames to JPEG format using the processor utility.
        frames_jpeg = self.kvs_fragment_processor.get_ndarray_frames_to_jpeg(frames)
        for idx, jpeg in enumerate(frames_jpeg):
            # Call AWS Rekognition to detect labels in the JPEG image.
            response = self.rekognition_client.detect_labels(
                Image={'Bytes': jpeg},
                MaxLabels=10,
                MinConfidence=80
            )
            # Save the Rekognition response as a JSON file for debugging or future reference.
            with open(f"{save_path}_{idx}.json", "w") as f:
                f.write(json.dumps(response))
            # Append detected labels to the result list.
            labels += response["Labels"]
        return labels

    def get_bounding_boxes(self, labels_fragment):
        '''
        Parses the Rekognition response to extract bounding boxes and corresponding metadata 
        for labels with instances. Filters out parent labels to retain the most specific labels.
        The idea is to keep all labels from each frame in the fragment together.

        ### Parameters:
            **labels_fragment**: list of dict
                A list containing Rekognition responses (labels and instances) for multiple frames.

        ### Returns:
            **fragment_bounding_boxes**: list of list of dict
                A list of bounding boxes for each frame. Each bounding box contains the label name, 
                bounding box coordinates, and confidence score.
        '''
        fragment_bounding_boxes = []
        valid_fragment_labels = []
        
        # Filter out parent labels that have children with instances.
        parents = set()
        child_labels = []
        for label in labels_fragment:
            if len(label["Instances"]) > 0:
                if label["Name"] not in parents:
                    child_labels.append(label)
                    # Add parent names to the set to identify child-parent relationships.
                    for parent in label["Parents"]:
                        parents.add(parent["Name"])
        # Retain only the child labels without parents in the final list.
        valid_fragment_labels += [label for label in child_labels if label["Name"] not in parents]
        
        # Extract bounding box information for valid labels.
        frame_bounding_boxes = []
        for label in valid_fragment_labels:
            for instance in label["Instances"]:
                frame_bounding_boxes.append(
                    {
                        "Name": label["Name"],
                        "Bounding_box": instance["BoundingBox"],
                        "Confidence": instance["Confidence"],
                    }
                )
        fragment_bounding_boxes.append(frame_bounding_boxes)

        # filter_duration = time.time()
        # fragment_unique_bboxs = self.filter_detections(fragment_bounding_boxes[0])
        # print(f"filter FUNCION en {time.time() - filter_duration:.4f} segundos.")
        
        # print(f"fragment_unique_bboxs:{fragment_unique_bboxs}\n\n")
        print(f"fragment_bounding_boxes:{fragment_bounding_boxes}\n\n")
        # return fragment_bounding_boxes, fragment_unique_bboxs
        return fragment_bounding_boxes

    # def calculate_iou(self, box1, box2):
    #     """
    #     Calcula el IoU (Intersection over Union) entre dos bounding boxes.
    #     Devuelve un valor entre 0 y 1 indicando el grado de solapamiento.
    #     """
    #     x1 = max(box1['Left'], box2['Left'])
    #     y1 = max(box1['Top'], box2['Top'])
    #     x2 = min(box1['Left'] + box1['Width'], box2['Left'] + box2['Width'])
    #     y2 = min(box1['Top'] + box1['Height'], box2['Top'] + box2['Height'])

    #     # Calcular ancho y alto de la intersección
    #     width = max(0, x2 - x1)
    #     height = max(0, y2 - y1)

    #     # Área de intersección
    #     intersection = width * height

    #     # Área de cada bounding box
    #     area1 = box1['Width'] * box1['Height']
    #     area2 = box2['Width'] * box2['Height']

    #     # Área de unión
    #     union = area1 + area2 - intersection

    #     # Evitar división por cero
    #     return intersection / union if union > 0 else 0

    # def filter_detections(self, detections, iou_threshold=0.5):
    #     """
    #     Filtra las detecciones para eliminar duplicados basándose en la superposición de Bounding Boxes.
    #     Se queda con la última detección en la lista si hay solapamiento alto.
    #     """
    #     grouped = {}  # Agrupar por 'Name'

    #     for obj in detections:
    #         name = obj['Name']
    #         if name not in grouped:
    #             grouped[name] = []
            
    #         # Comprobamos si este objeto solapa con alguno de los ya almacenados
    #         new_list = []
    #         should_add = True  # Indica si debemos añadir este objeto
            
    #         for existing in grouped[name]:
    #             iou = self.calculate_iou(obj['Bounding_box'], existing['Bounding_box'])
                
    #             if iou > iou_threshold:  # Si el solapamiento es alto
    #                 should_add = True  # Queremos quedarnos con la última detección
    #             else:
    #                 new_list.append(existing)  # Mantenemos los anteriores si el IoU es bajo
            
    #         if should_add:
    #             new_list.append(obj)  # Añadimos el nuevo objeto
            
    #         grouped[name] = new_list  # Actualizamos la lista para este 'Name'

    #     # Convertimos el diccionario en lista
    #     result = [obj for obj_list in grouped.values() for obj in obj_list]
    #     return result

    def on_stream_read_complete(self, stream_name):
        '''
        Callback triggered when the KvsConsumerLibrary finishes reading all available fragments 
        from the specified stream. Can be used to clean up resources or restart the stream.

        ### Parameters:
            **stream_name**: str
                Name of the stream being read, useful when processing multiple streams.
        '''
        print(f'Read Media on stream: {stream_name} Completed successfully - Last Fragment Tags: {self.last_good_fragment_tags}')
        log.info(f'Read Media on stream: {stream_name} Completed successfully - Last Fragment Tags: {self.last_good_fragment_tags}')
        # self.websocket_server.stop_server()

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
        # self.websocket_server.stop_server()

    def _get_data_endpoint(self, stream_name, api_name):
        '''
        Retrieves the endpoint for a specific API call using the KVS client. 
        This endpoint is required for subsequent interactions with the KVS API.

        ### Parameters:
            **stream_name**: str
                Name of the Kinesis Video Stream.

            **api_name**: str
                Name of the API for which the endpoint is requested (e.g., "GET_MEDIA").

        ### Returns:
            **data_endpoint**: str
                The endpoint URL for the requested API.
        '''
        response = self.kvs_client.get_data_endpoint(
            StreamName=stream_name,
            APIName=api_name
        )
        return response['DataEndpoint']

if __name__ == "__main__":
    kvsConsumerExample = KvsPythonConsumerExample()
    kvsConsumerExample.service_loop()
