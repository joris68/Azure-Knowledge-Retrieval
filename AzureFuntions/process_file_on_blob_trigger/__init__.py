import logging
from common import tiktoken_len, upload_blob_to_storage
from langchain.text_splitter import RecursiveCharacterTextSplitter
import os
from azure.storage.blob import BlobServiceClient
from langchain.embeddings import OpenAIEmbeddings
import re
import pinecone
import json
import traceback

import azure.functions as func


# textsplitting the file
#   1. chunking
#   3. Embedding the json 
#   4. inserting to pinecone
#   5. storing the json into blob storage

def main(myblob: func.InputStream):

    return func.HttpResponse("Unnötiger Blob Trigger")
    
    try:


        connection_string = os.environ['storage_PodcastExplorer']
            
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        container_client = blob_service_client.get_container_client('transcriptions/Angebissen - der Angelpodcast/'+ myblob.year )

        blob_client = container_client.get_blob_client(myblob.name)

        data = blob_client.download_blob()

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=400,
            chunk_overlap=20,  # number of tokens overlap between chunks
            length_function=tiktoken_len,
            separators=['\n\n', '\n', ' ', '']
            )

        text_chunks_with_meta_data = []

        chunks = text_splitter.split_text(data.context_as_text())


        for txt in chunks:

            a = {
            
                        'text' : txt,
                        'source': myblob.uri

                }

            text_chunks_with_meta_data.append(a)


        pinecone.init(      
            api_key='41fe2d0f-afb6-42e7-adbc-e8b60baf9aed',      
            environment='asia-southeast1-gcp-free'      
            )      
        index = pinecone.Index('mypodcastindex')


        # now we take the list embed it and insert into 

        embeddings = OpenAIEmbeddings(deployment="text-embedding-ada-002")

        vectors = []
        identity_counter = index.describe_index_stats()['totalVectorCount'] +1


        for obj in text_chunks_with_meta_data:

            match = re.search(r'/(\d{4})/(\d{4})_(.*?)_', obj['source'])
            match_title = re.search(r'(?<=\d{4}_\d{8}_)(.*?)(?=\s\d{3}_\d{2}min_Transcriptions\.txt)',obj['source'])
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
            "values" : embeddings.embed_query(obj['text']),
            "metadata": {
                "source": obj['source'], 
                "year" : year,
                "date" : date, 
                "episode" : episode,
                "title": title,
                "clear_text": obj['text'],
                "blob_name": myblob.name # ganz wichtig und alle Vektoren, acuh später wieder löschen zu können
            }        
            }

            identity_counter += 1

            vectors.append(curr)

            index.upsert(vectors=[curr], namespace="angebissen")

        json_to_return = {
            "vectors": vectors,

            "namespace": "angebissen"

        }


        upload_blob_to_storage('vectordatabase' , json.dumps(json_to_return) ,  myblob.name + '_chunked_' +'.json')

        return func.HttpResponse("VectorDatabase was successfully written to a json file", status_code=200)

    except Exception as e:
     # If an exception occurs, capture the error traceback
      error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
      return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
            