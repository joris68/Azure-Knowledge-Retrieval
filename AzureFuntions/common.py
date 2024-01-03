from azure.storage.blob import BlobServiceClient, generate_container_sas, BlobSasPermissions,UserDelegationKey
import os
import tiktoken
import logging
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import pinecone
import datetime
import re


#tokenizer = tiktoken.get_encoding('p50k_base')
tokenizer = tiktoken.get_encoding('cl100k_base')

# create the length function
def tiktoken_len(text):

    try:
        tokens = tokenizer.encode(
            text,
            disallowed_special=()
        )
        return len(tokens)
    
    except:
        logging.error("Something went wrong in the tiktoken_len")
        raise Exception("Something went wrong in the tiktoken_len")

def get_blob_list(Container):

    connection_string = os.environ['storage_PodcastExplorer']
        
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

      
    container_client = blob_service_client.get_container_client(Container)

    return  container_client.list_blobs()

def get_latest_blob_from_storage(container_name, folder_name = None):

    try:

        connection_string = os.environ['my_storage']
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

      
        container_client = blob_service_client.get_container_client(container_name )

        


        blob_client = container_client.get_blob_client('vectorDB.csv')

        logging.info("The latest blob was successfully from storage retrieved ")
        
        return blob_client.download_blob()
    
    except Exception as e:

        logging.info("Something went wrong in the in the process of retrieving the the latest Blob")

        raise Exception("Something went wrong in the in the process of retrieving the the latest Blob")
    




def upload_blob_to_storage(container_path, blob_data, blob_name) :

    try:

        app_setting = 'my_storage'

        connection_string = os.environ[app_setting] 

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        container_client = blob_service_client.get_container_client(container_path) 
        
        output_blob = container_client.get_blob_client(blob_name)

        output_blob.upload_blob(blob_data,overwrite=True)

        logging.info("Blob was successfully uploaded to Blob Storage")

    except:
        logging.error("Something went wrong with the Blob upload to the Staging Blob-Storage")
        raise Exception("Something went wrong with the Blob upload to the Staging Blob-Storage")



def perform_cosine_similarity_search(df, question_embedding ,k_nearest_neighbors = 5):
    #vectors = df.vector.apply(eval).apply(np.array)
    # extract the vectors
    df['vector'] = df['vector'].apply(eval).apply(np.array)
    df['Similarity'] = df['vector'].apply(lambda x: cosine_similarity([question_embedding], [x])[0][0])


    df = df.sort_values(by='Similarity', ascending=False)

    # Specify the number of most similar embeddings to retrieve

    top_k = df.head(k_nearest_neighbors)

    list_to_return =  []

    for index, row in top_k.iterrows():
        
        list_to_return.append((row['text'], row['source'] ))

    
    return list_to_return


# we have a list of tuples as an input
def generate_content_for_prompt(query_response):
    try:

        logging.info("now we are concatenating the input")


        string_to_return = ""
        for i in query_response['matches']:

            string_to_return += str(i['metadata']['clear_text']) + " aus der Folge " + str(i['metadata']['episode']) + " aus der Quelle " + str(i['metadata']['source']) + "."


        return string_to_return
    
    except:
        logging.error("Something went wrong fetching the Data")
        raise Exception("Something went wrong fetching the Data")



def check_for_answer_capacity_in_tokens(prompt):

    models_max_token_capacity = 4097

    prompt_tokens = tiktoken_len(prompt)

    logging.info("this is the lenght for the finished promt :" + str(prompt_tokens))

    x= models_max_token_capacity - prompt_tokens

    logging.info("this is the calculated output lenghth: "  + str(x)) 
 
    return x


def generate_prompt(context, question):
    with open('prompt_template.txt') as f:
        prompt = f.read()
    
    prompt = prompt.replace("<<Context>>", context)
    prompt = prompt.replace("<<Frage>>", question)

    return prompt


def search_pinecone(question_embedding, top_k = 5):

    pinecone.init(      
	    api_key='41fe2d0f-afb6-42e7-adbc-e8b60baf9aed',      
	    environment='asia-southeast1-gcp-free'      
        )      
    index = pinecone.Index('mypodcastindex')      

    query_response = index.query(
            top_k=top_k,
            include_values=False,
            include_metadata=True,
            vector=question_embedding,
            namespace='angebissen'
        )
    
    return query_response


# hier aufpassen wegen dem ordner Joris APIs vielleicht?
def generate_SAS_urls_for_sources(query_response):

    try:

        # extractthe relative path out of the blob
        source_list = []

        start_point = "https://stpodcastexplorer.blob.core.windows.net"

        for x in query_response['matches']:
            pattern = r"/transcriptions/Angebissen - der Angelpodcast/.*_Transcriptions.txt"

            # Use re.search to find the match
            match = re.search(pattern, x['metadata']['source'])

            if match:
                extracted_sequence = match.group()
                logging.info("Extracted Sequence:" + extracted_sequence)
                if extracted_sequence not in source_list:

                    source_list.append(extracted_sequence)
                else:
                    pass

            else:

                pass


        # now we are going to 

        start_time = datetime.datetime.now(datetime.timezone.utc)
        expiry_time = start_time + datetime.timedelta(days=1)

        app_setting = 'storage_PodcastExplorer'

        connection_string = os.environ[app_setting] 

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client('transcriptions')

        sas_token = generate_container_sas(
            account_name=container_client.account_name,
            container_name=container_client.container_name,
            permission=BlobSasPermissions(read=True),
            account_key=container_client.credential.account_key, 
            expiry=expiry_time,
            start=start_time
        )
        
        logging.info('Das ist das Token: ' +str(sas_token))

        # now concatenate
        finished_links = []

        for x in source_list:

            a = start_point + x + '?' + str(sas_token)

            finished_links.append(a)

        logging.info("Everything went well for generating the " +str(finished_links))

        return finished_links
    
    except Exception as e:

        logging.error('SAS token genration hat nicht so gut funktioniert')
        raise Exception('SAS token genration hat nicht so gut funktioniert')

    



