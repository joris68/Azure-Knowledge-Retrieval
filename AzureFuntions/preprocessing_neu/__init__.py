import logging
import os
import azure.functions as func
from common import tiktoken_len
from langchain.text_splitter import RecursiveCharacterTextSplitter
from azure.storage.blob import BlobServiceClient
import traceback
import json

# Veränderte Sachen: 
# 1. Neues tokenizer Modell
# 2. wir speichern und den blob namen und direkt die URl ab
# 3. auch wenns blöd ist, lassen wir alles in der cloud laufen, dann werden die logs gespeichert....und wir können es überwachen

def main(req: func.HttpRequest) -> func.HttpResponse:


    try:

        # das machen wir für jedes Jahr
        connection_string = os.environ['storage_PodcastExplorer']
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
      
        container_client = blob_service_client.get_container_client('transcriptions')

        blob_list = container_client.list_blobs(name_starts_with= 'Angebissen - der Angelpodcast/2023')

        logging.info("Blob list hat funktioniert")
        

        #--------------------- Now we chunk the textdocuments -------------------

        # initialisieren des Textsplitters
        text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=400, # 400 hat sich als gute chunk size herausgestellt
        chunk_overlap=20,  # number of tokens overlap between chunks
        length_function=tiktoken_len,
        separators=['\n\n', '\n', ' ', '']
        )
        logging.info("Textsplitter initialized")
        

        # for the Url (I am sure there is a way to do it programtically)

        start_point = 'https://stpodcastexplorer.blob.core.windows.net/'
        relative_path = 'transcriptions/'

        # for uploading the JSON files

        app_setting = 'my_storage'

        connection_string_sink = os.environ[app_setting] 

        blob_service_client_sink = BlobServiceClient.from_connection_string(connection_string_sink)

        container_client_sink = blob_service_client_sink.get_container_client('cleartextchunks')

        logging.info("connection to my storage was established")


        for x in blob_list:

            logging.info("Now propcessing this blob: " +  x.name )

            text_chunks_with_metaData = []

            # connect to the specific blob
            blob_client = container_client.get_blob_client(x)
            
            data = blob_client.download_blob()

            # chunking the text of the blob into several chunks of size 400
            chunks = text_splitter.split_text(data.content_as_text())


            for txt in chunks:

                a = {
        
                    'text' : txt, 
                    'blob_url': start_point + relative_path + str(data.name),
                    'blob_name_short': data.name

                }

                text_chunks_with_metaData.append(a)

            try:
                json_data = json.dumps(text_chunks_with_metaData)
                # we do  not want the '.txt' ending
                output_blob = container_client_sink.get_blob_client(str(data.name[:-4]) + '_chunked.json')
                output_blob.upload_blob(json_data,overwrite=True)
                logging.info("Blob for file : " + data.name + ' was successfully written to storage' )

            except:

                logging.warning("Blob for file : " + data.name + ' could NOT BE WRITTEN!!!!' )


        return func.HttpResponse( "Function was successfully executed"  ,status_code=200)

    except Exception as e:

        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse( str(e) + "\n--\n" + error_traceback, status_code=400)


