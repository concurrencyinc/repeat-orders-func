import os
import json
import base64
import logging
from io import BytesIO
from email import policy

import fitz
import tiktoken
import azure.functions as func
from azure.storage.queue import (
    QueueServiceClient,
    QueueClient,
    QueueMessage,
    BinaryBase64EncodePolicy,
    BinaryBase64DecodePolicy,
)
from azure.storage.blob import BlobServiceClient

# Local application-specific imports
from ai_interface import *

app = func.FunctionApp()


@app.blob_trigger(arg_name="myblob", path="repeat-orders-emails/msg-body/{name}",
                  connection="repeatorderstorage_STORAGE") 
def blob_trigger(myblob: func.InputStream, context: func.Context):
    logging.info(f"Python blob trigger function processed blob "
                 f"Name: {myblob.name} "
                 f"Blob Size: {myblob.length} bytes")
    
    try:  
        email_content = myblob.read().decode('utf-8')  
   
        json_dict = json.loads(email_content)
        logging.info(f"Successfully loaded JSON from blob {myblob.name}")
  
        for attachment in json_dict['attachments']:
            filename = attachment['name']
            
            if filename.lower().endswith('.pdf'):

                content_base64 = attachment['contentBytes']
                content_bytes = base64.b64decode(content_base64)

                try:
                    pdf_document = fitz.open(stream=content_bytes, filetype="pdf")
                    logging.info(f"Successfully opened PDF document: {filename}")

                    # Write PDF attachment to blob storage
                    write_attachment_to_blob(myblob.name, filename, content_bytes)
                except Exception as pdf_error:
                    logging.error(f"Error processing PDF {filename}: {str(pdf_error)}")
            else:
                logging.info(f"Skipping non-PDF attachment: {filename}") 
    except Exception as e:  
        logging.error(f"Error processing blob {myblob.name}: {str(e)}")  
    

    if pdf_document:
        full_text_list = process_pdf_attachment(myblob.name, filename, pdf_document)
        if isinstance(full_text_list, list):
            full_text = ' '.join(full_text_list)
        # TODO Dockerize Azure Function APP
        # https://stackoverflow.com/questions/69737043/azure-function-poppler-utils
        # and install Docker to get better results

        # TOD extract table using camelot - see if any difference in results
        # table_dict = get_table_data(pdf_content)
        # logging.info(f"Table Dictionary from unstructured text of {filename}: {table_dict}...")
    else:
        logging.warning(f"Empty PDF attachment found: {filename}")

    # Get product and customer data
    logging.info(f"Extracted data from {filename}: {full_text}")

    tokenizer = tiktoken.encoding_for_model('gpt-4o')
    tokens = tokenizer.encode(full_text)
    logging.info(f"Number of tokens: {len(tokens)}")

    if len(tokens) > 3600:
        logging.warning(f"Too many tokens in: {filename}. No results extracted.")
    else:
        extracted_data = product_customer_llm_extraction(full_text)
        logging.info(f"Extracted Pydantic Model from {filename}: {extracted_data}")
        extended_data = ExtendedExtractedData.from_extracted_data(extracted_data)
        logging.info(f"Extracted Pydantic Model from {filename}: {extended_data}")

        # Convert to dictionary
        data_dict = extended_data.model_dump()
        data_dict["input_blob_name"] = filename

        pdf_to_json_pydantic_model = json.dumps(data_dict, indent=4)
        logging.info(f"Extracted Pydantic Model from {filename}: {pdf_to_json_pydantic_model}")

        write_json_to_blob(myblob.name, filename, pdf_to_json_pydantic_model)
        send_queue_message(json_data=pdf_to_json_pydantic_model)


def process_pdf_attachment(
    original_blob_name: str, filename: str, fitz_doc: fitz.Document
) -> list[str]:
    """Extract text from the first two pages of a PDF document."""
    
    # Extract text
    full_text = []
    if fitz_doc.page_count > 0:
        for i, page in enumerate(fitz_doc):
            text = page.get_text()
            logging.info(f"Page text of {filename}: {text[:100]}...")
            full_text.append(text)
            if i >= 2:
                break

    # Close the document
    fitz_doc.close()

    return full_text


def send_queue_message(json_data: dict):
    # Get the connection string from app settings
    connection_string = os.environ['AzureWebJobsStorage']

    # Create a QueueClient
    queue_client = QueueClient.from_connection_string(conn_str=connection_string, queue_name="order-processing-queue")

    # Setup Base64 encoding and decoding functions
    queue_client.message_encode_policy = BinaryBase64EncodePolicy()
    queue_client.message_decode_policy = BinaryBase64DecodePolicy()

    json_data_bytes = json_data.encode('ascii')

    # Send a message to the queue
    try:
        queue_client.send_message(queue_client.message_encode_policy.encode(content=json_data_bytes))
        logging.info("Successfully sent message to the queue")
    except Exception as e:
        logging.error(f"Error sending message to the queue: {str(e)}")

def get_blob_client(original_blob_name: str, subfolder: str, filename: str):
    connect_str = os.environ['AzureWebJobsStorage']
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name, blob_path = original_blob_name.split('/', 1)
    container_client = blob_service_client.get_container_client(container_name)
    blob_name = f"{subfolder}/{blob_path}/{filename}"
    return container_client.get_blob_client(blob_name)

def write_attachment_to_blob(original_blob_name: str, filename: str, content: bytes):
    try:
        blob_client = get_blob_client(original_blob_name, "attachments", filename)
        blob_client.upload_blob(content, overwrite=True)
        logging.info(f"Successfully uploaded PDF attachment: {blob_client.blob_name}")
    except Exception as e:
        logging.error(f"Error uploading PDF attachment {filename}: {str(e)}")

def write_json_to_blob(original_blob_name: str, filename: str, json_data: dict):
    try:
        json_filename = f"{os.path.splitext(filename)[0]}_extract.json"
        blob_client = get_blob_client(original_blob_name, "json_output", json_filename)
        json_content = json.dumps(json_data).encode('utf-8')
        blob_client.upload_blob(json_content, overwrite=True)
        logging.info(f"Successfully uploaded JSON output: {blob_client.blob_name}")
    except Exception as e:
        logging.error(f"Error uploading JSON for {filename}: {str(e)}")

