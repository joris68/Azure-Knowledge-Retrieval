import logging
from common import get_latest_blob_from_storage, upload_blob_to_storage
import json
import traceback
from langchain.embeddings import OpenAIEmbeddings
import time
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
	    api_key='',      
	    environment='asia-southeast1-gcp-free'      
      )      
      index = pinecone.Index('mypodcastindex')

      logging.info("start embedding")

      embeddings = OpenAIEmbeddings(deployment="text-embedding-ada-002")

      identity_counter = 5475


      # list slicing for testing purposes

      vectors = []

      for chunk in data:
         
        match = re.search(r'/(\d{4})/(\d{4})_(.*?)_', chunk['source'])
        match_title = re.search(r'(?<=\d{4}_\d{8}_)(.*?)(?=\s\d{3}_\d{2}min_Transcriptions\.txt)',chunk['source'])
        if match:
          year = match.group(1)
          episode = match.group(2)
          date = match.group(3).replace('_', ' ')
        else:
           
          episode = ""
          date = ""

        if match_title:
          title = match_title.group(1)
        else:
          title = ""

       
        curr = {
           "id": "vec" + str(identity_counter),
           "values" : embeddings.embed_query(chunk['text']),
           "metadata": {
            "source": chunk['source'], 
            "year" : year,
            "date" : date, 
            "episode" : episode,
            "title": title,
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
   
      
      

