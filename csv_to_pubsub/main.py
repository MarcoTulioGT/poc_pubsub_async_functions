import csv
import base64
from google.cloud import pubsub_v1, storage

PROJECT_ID = "focus-infusion-463923-t8"
TOPIC_ID = "dlq-topic"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def process_csv_to_pubsub(event, context):
    """Se activa cuando un archivo .csv es subido a GCS"""
    bucket_name = event['bucket']
    file_name = event['name']

    if not file_name.endswith(".csv"):
        print(f"Archivo {file_name} ignorado (no es CSV)")
        return

    print(f"Procesando archivo: gs://{bucket_name}/{file_name}")

    # Cliente de Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Descargar el archivo CSV en memoria
    data = blob.download_as_text().splitlines()

    reader = csv.DictReader(data)  # Usa encabezados como keys
    for row in reader:
        message = str(row).encode("utf-8")  # Convierte fila a string
        future = publisher.publish(topic_path, message)
        print(f"Enviado a Pub/Sub: {row} (msg_id={future.result()})")
