from azure.storage.blob import BlobServiceClient, generate_container_sas, BlobSasPermissions,UserDelegationKey
import os
import tiktoken
import logging
import pinecone
import datetime
import re
import openai


tokenizer = tiktoken.get_encoding('p50k_base')

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


# we have a list of tuples as an input
def generate_content_for_prompt(query_response):
    try:

        logging.info("now we are concatenating the input")


        string_to_return = ""
        for i in query_response['matches']:

            # hier berechnen wir uns die Stelle , wo es im Dokument vorkommt
            a = i['metadata']['chunk_counter'] /i['metadata']['total_doc_chunks']

            if a >= 0.75:
                meta_zeit = 'Am Ende'
            elif a < 0.75 and a > 0.25:
                meta_zeit = "In der Mitte"
            else:
                meta_zeit = 'Am Anfang'

            string_to_return += str(i['metadata']['clear_text']) + meta_zeit +" der Folge " + str(i['metadata']['episode']) + " mit dem Name " + str(i['metadata']['title']) + "."


        return string_to_return
    
    except:
        logging.error("Something went wrong genrating the Content for the Prompt")
        raise Exception("Something went wrong genrating the Content for the Prompt")



def check_for_answer_capacity_in_tokens(prompt):

    models_max_token_capacity = 4097

    prompt_tokens = tiktoken_len(prompt)

    logging.info("this is the lenght for the finished promt :" + str(prompt_tokens))

    x= models_max_token_capacity - prompt_tokens

    logging.info("this is the calculated output lenghth: "  + str(x)) 
 
    return x


def generate_prompt_input(context, question):
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



def generate_SAS_urls_for_sources(query_response):

    try:


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
        

        # now concatenate
        finished_links = []

        for x in query_response['matches']:

            a =  x['metadata']['blob_url'] + '?' + str(sas_token)

            finished_links.append(a)

        return finished_links
    
    except Exception as e:

        logging.error('SAS token genration hat nicht so gut funktioniert')
        raise Exception('SAS token genration hat nicht so gut funktioniert')


def generate_JSON_respone(completion, query_response, question):

    return {
        "question": question,
    
        "GTP_answer": completion.choices[0].text,

        "query_response": str(query_response['matches']),

        "SAS_links": generate_SAS_urls_for_sources(query_response)
    }


def ask_GTP_api(query_response, question):

    context = generate_content_for_prompt(query_response)


    logging.info("SAS List hat funktioniert")

    prompt = generate_prompt_input(context, question)

    openai.api_key = os.environ['OPENAI_API_KEY']
    openai.api_base= os.environ['OPENAI_API_BASE']
    openai.api_type = os.environ['OPENAI_API_TYPE']
    openai.api_version =os.environ['OPENAI_API_VERSION']

    max_tokens = check_for_answer_capacity_in_tokens(prompt)


    completion = openai.Completion.create(engine="text-davinci-003", prompt=prompt, temperature=0, max_tokens=max_tokens)

    return completion


