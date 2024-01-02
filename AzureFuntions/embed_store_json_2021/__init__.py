import logging
from common import get_latest_blob_from_storage, upload_blob_to_storage
import json
import traceback
from langchain.embeddings import OpenAIEmbeddings
import time
import numpy as np
import pandas as pd
from azure.storage.blob import BlobServiceClient
import re
import os
import pinecone



import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
   

     # getting the prepared json file from our blob storgae
    try:


      connection_string = os.environ['my_storage']
        
      blob_service_client = BlobServiceClient.from_connection_string(connection_string)

      
      container_client = blob_service_client.get_container_client('podcastfiles' )

      blob_client = container_client.get_blob_client('2021_data_chunks.json')

      blob = blob_client.download_blob()

      data = json.loads(blob.content_as_text())

      logging.info("blob was retrieved")
   
    except Exception as e:
      # If an exception occurs, capture the error traceback
      error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
      return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
       


   #-----------------------------------------------------------------
   # now we are gonna embed the data chunks out of the storage and 

    try:

            

      pinecone.init(      
	    api_key='41fe2d0f-afb6-42e7-adbc-e8b60baf9aed',      
	    environment='asia-southeast1-gcp-free'      
      )      
      index = pinecone.Index('mypodcastindex')

      logging.info("start embedding")

      embeddings = OpenAIEmbeddings(deployment="text-embedding-ada-002")

      identity_counter = 5513


      # list slicing for testing purposes

      vectors = []

      for chunk in data[38:]:
         
        pattern = r"/(\d{4})(/\d{8})_(.*?)(\d{2})_Transcriptions.txt"

        # Use re.search to find the match
        match = re.search(pattern, chunk['source'])

        if match:
            date_sequence = match.group(1)    # "20210820"
            description_sequence = match.group(3)   # "Stippe, Feeder, Pose - Was fängt Mit drei Methoden auf Schleie"
            number_sequence = match.group(4)  # "57"
            logging.info("Date Sequence:" + date_sequence)
            logging.info("Description Sequence:" + description_sequence)
            logging.info("Number Sequence:" + number_sequence)
        else:
            logging.info("No match found.")
            date_sequence =""  # "20210820"
            description_sequence = ""  # "Stippe, Feeder, Pose - Was fängt Mit drei Methoden auf Schleie"
            number_sequence = ""  # "57"

       
        curr = {
           "id": "vec" + str(identity_counter),
           "values" : embeddings.embed_query(chunk['text']),
           "metadata": {
            "source": chunk['source'], 
            "year" : 2021,
            "date" : date_sequence, 
            "episode" : number_sequence,
            "title": description_sequence,
            "clear_text": chunk['text']
           }        
         }

        identity_counter += 1

        vectors.append(curr)

        index.upsert(vectors=[curr], namespace="angebissen")

         #andererseits würden wir den sever überlasten, weil wir zu viele token pro minute schicken
        time.sleep(0.1)

    
    
      json_to_return = {
        "vectors": vectors,

        "namespace": "angebissen"

      }
      
      logging.info("for loop finished")


    except Exception as e:
       
       # If an exception occurs, capture the error traceback
      error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
      return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
   

   # now instead of storing it into a database, we  will use store it into an CSV file in our blob storage



   # now we are gonna write it into our blob storage
    try:
      
      upload_blob_to_storage('vectordatabase', json.dumps(json_to_return), '2021_chunks_upserted.json')

      return func.HttpResponse("VectorDatabase was successfully written to a json file", status_code=200)

    except Exception as e:
      
       # If an exception occurs, capture the error traceback
      error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
      return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
   
      
      

