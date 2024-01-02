import logging
from common import get_latest_blob_from_storage, upload_blob_to_storage
import json
import traceback
from langchain.embeddings import OpenAIEmbeddings
import time
import numpy as np
import pandas as pd
from azure.storage.blob import BlobServiceClient


import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
   

     # getting the prepared json file from our blob storgae
    try:

      blob = get_latest_blob_from_storage('podcastfiles')

      data = json.loads(blob.content_as_text())
   
    except Exception as e:
      # If an exception occurs, capture the error traceback
      error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
      return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
       


   #-----------------------------------------------------------------
   # now we are gonna embed the data chunks out of the storage and 

    try:

      logging.info("start embedding")

      embeddings = OpenAIEmbeddings(deployment="text-embedding-ada-002")

      identity_counter = 1

      # list slicing for testing purposes

      for chunk in data:
         
         #embed the text chunk via the OpenAI API 
         chunk['vector'] = str(embeddings.embed_query(chunk['text'])) # warte mal hier könnten die umlaute das problem sein
         chunk['identity'] = identity_counter

         identity_counter += 1

         #andererseits würden wir den sever überlasten, weil wir zu viele token pro minute schicken
         time.sleep(0.1)


      
      logging.info("for loop finished")


    except Exception as e:
       
       # If an exception occurs, capture the error traceback
      error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
      return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
   

   # now instead of storing it into a database, we  will use store it into an CSV file in our blob storage

    df_to_store = pd.read_json(json.dumps(data))

    csv_file = df_to_store.to_csv()


   # now we are gonna write it into our blob storage
    try:
      
      upload_blob_to_storage('vectordatabase', csv_file, 'vectorDB.csv')

      return func.HttpResponse("VectorDatabase was successfully written to a CSV file")

    except Exception as e:
      
       # If an exception occurs, capture the error traceback
      error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
      return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
   
      
      

