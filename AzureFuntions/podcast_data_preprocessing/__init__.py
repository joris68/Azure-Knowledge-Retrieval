import logging
import os
import azure.functions as func
from common import tiktoken_len
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.document_loaders import AzureBlobStorageContainerLoader
import openai
from azure.storage.blob import BlobServiceClient
import traceback
import hashlib
import json

#TODO url auf den file der bob storage wäre noch sehr nice

def main(req: func.HttpRequest) -> func.HttpResponse:

    # mögliche probleme:
    #       die umlaute -> wie tauscht man umlaute aus ?
    # die metadaten sind nicht ganz korrekt -> wie macht man das besser?


    try:


        #first we are gonna retrieve the text files from the blob storage

        conn = os.environ['storage_PodcastExplorer']

        loader = AzureBlobStorageContainerLoader(conn_str=conn, container="transcriptions", prefix="Angebissen - der Angelpodcast/2021")


        text_docs = loader.load()

        logging.info("this is the first text of the ducoment loader : " +text_docs[0].page_content)

        #--------------------- Now we chunk the textdocuments -------------------


        text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=400,
        chunk_overlap=20,  # number of tokens overlap between chunks
        length_function=tiktoken_len,
        separators=['\n\n', '\n', ' ', '']
        )

        text_chunks_with_metaData = []


        for x in range(len(text_docs)):

            # chunking the the content of the document object
            chunk = text_splitter.split_text(text_docs[x].page_content)

            # creating a unique id for the data chunk based on the metadaata provided
            key = text_docs[x].metadata['source']

            for txt in chunk:

                a = {
        
                    'text' : txt,
                    'source': key

                }

                text_chunks_with_metaData.append(a)

        
        if len(text_chunks_with_metaData) > len(text_docs):
            logging.info("chunking hat jetzt besser funktioniert")
        
        else:
            logging.info("chunking ist immer noch falsch")


        logging.info("Die for schleife hat funktioniert")

        json_data = json.dumps(text_chunks_with_metaData)


        # now we are gonna save it as a json file to my storage account

        app_setting = 'my_storage'

        connection_string = os.environ[app_setting] 

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        container_client = blob_service_client.get_container_client('podcastfiles')

        output_blob = container_client.get_blob_client('2021_data_chunks.json')

        output_blob.upload_blob(json_data,overwrite=True)

        logging.info("Blobdata was successfully uploaded")

            

        return func.HttpResponse( "Function was successfully executed"  ,status_code=200)

    except Exception as e:

        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)


