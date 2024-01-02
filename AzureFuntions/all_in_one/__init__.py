import logging
import azure.functions as func
from langchain.embeddings import OpenAIEmbeddings
import traceback
from support_allInOne import  search_pinecone, generate_JSON_respone, ask_GTP_api
import json



def main(req: func.HttpRequest) -> func.HttpResponse:

    try:
        
        

        # get the Question out of the Request Body

        req_body = req.get_json()


        question = req_body['question']

        # embed the query to a vector


        embeddings = OpenAIEmbeddings(deployment="text-embedding-ada-002")

        question_embedding = embeddings.embed_query(question)


        # ------------------ now query the database

        query_response = search_pinecone(question_embedding, top_k=5)

        # ---------------- now generate the answer --------------------

     
        response = generate_JSON_respone(ask_GTP_api(query_response, question), query_response, question)
        

        logging.info("Generated answer , sending it back")

        return func.HttpResponse(json.dumps(response), status_code=200)

    except Exception as e:

        error_traceback = traceback.format_exc()

        return func.HttpResponse("Irgendwas ist schiefgelaufen:" + str(e) + " --------" + error_traceback , status_code=400)
