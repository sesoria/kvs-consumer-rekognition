import boto3
import json

API_GATEWAY_ENDPOINT = f"https://uhqyaqtrqh.execute-api.eu-west-1.amazonaws.com/production"
DYNAMODB_TABLE_NAME = "c-IrisWebSocket"
# Inicializar el cliente de DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

####################################################
# ApiGateway WebSocket

def client_ag_manegement_api():
    return boto3.client('apigatewaymanagementapi', endpoint_url=API_GATEWAY_ENDPOINT)


def send_message_to_clients(apigw_client, active_connections, message):
    for connection_id in active_connections:
        try:
            apigw_client.post_to_connection(
                ConnectionId=connection_id,
                Data=json.dumps(message)
            )
        except apigw_client.exceptions.GoneException:
            print(f"La conexión {connection_id} ya no existe.")
        except Exception as e:
            print(f"Error enviando mensaje a {connection_id}: {e}")

####################################################
# DynamoDb

def get_connection_ids_by_stream(stream_name):
    """
    Obtiene todos los connectionId asociados a un stream_name específico.
    """
    response = table.scan(
        FilterExpression="stream_name = :stream_name",
        ExpressionAttributeValues={":stream_name": stream_name}
    )
    
    # Extraer solo los connectionId
    connection_ids = [item['connection_id'] for item in response.get('Items', [])]
    return connection_ids