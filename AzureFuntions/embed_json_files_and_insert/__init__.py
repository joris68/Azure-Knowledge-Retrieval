import logging
from common import upload_blob_to_storage
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
   

    try:

      connection_string = os.environ['my_storage']
        
      blob_service_client = BlobServiceClient.from_connection_string(connection_string)

      
      container_client = blob_service_client.get_container_client('cleartextchunks')

      blob_list = container_client.list_blobs(name_starts_with= 'Angebissen - der Angelpodcast/2023')


      logging.info("got the Blob List")
      
      # initialize Pinecone
      pinecone.init(      
	    api_key='',      
	    environment='asia-southeast1-gcp-free'      
      )      
      index = pinecone.Index('mypodcastindex')
 
      
      logging.info("start embedding")

      embeddings = OpenAIEmbeddings(deployment="text-embedding-ada-002")

      # list slicing for testing purposes


      for blob in blob_list:
        vectors = []

        blob_client = container_client.get_blob_client(blob)
            
        data = blob_client.download_blob()

        json_data = json.loads(data.content_as_text())

        # update identity
        stats_response = index.describe_index_stats()

        # calculate identity counter
        identity_counter = stats_response['total_vector_count'] +1

        chunk_counter = 1

        for chunk in json_data:

         
          match_sequence = re.search(r"/(\d+)_", chunk['blob_name_short'])
          if match_sequence:
            episode = match_sequence.group(1)
          else:
            episode = ""


          match_date = re.search(r"(\d{4})(\d{2})(\d{2})", chunk['blob_name_short'])
          if match_date:
            date = f"{match_date.group(1)}{match_date.group(2)}{match_date.group(3)}"
          else:
            date = ""


          match_title = re.search(r"/(\d+_\d+_)(.+?)_", chunk['blob_name_short'])
          if match_title:
            title = match_title.group(2).replace("_", " ")
          else:
            title = ""

        
          curr = {
            "id": "vec" + str(identity_counter),
            "values" : embeddings.embed_query(chunk['text']),
            "metadata": {
                "blob_url": chunk['blob_url'], 
                "year" : '2023',
                "date" : date, 
                "episode" : episode,
                "title": title,
                "clear_text": chunk['text'],
                "blob_name": chunk['blob_name_short'],
                "chunk_counter": chunk_counter,
                "total_doc_chunks" : len(json_data)
            }        
          }

          identity_counter += 1
          chunk_counter +=1


          vectors.append(curr)

          # insert Vector + Metadata into DB
          index.upsert(vectors=[curr], namespace="angebissen")
          #------innere For schleife fertig----------------

        json_to_return = {
        "vectors": vectors,

        "namespace": "angebissen"

        }
         
        upload_blob_to_storage('embeddedchunks', json.dumps(json_to_return), data.name[:-5] +'_upserted.json')
        # --------- äußere for schleife fertig ------------------------

      return func.HttpResponse("VectorDatabase was successfully written to a json file", status_code=200)

    except Exception as e:
      
       # If an exception occurs, capture the error traceback
      error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
      return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
   
      
      

