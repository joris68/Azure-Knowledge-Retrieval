import pinecone

pinecone.init(      
	    api_key='41fe2d0f-afb6-42e7-adbc-e8b60baf9aed',      
	    environment='asia-southeast1-gcp-free'      
)      
index = pinecone.Index('mypodcastindex')
index.delete(delete_all=True, namespace="angebissen")

res =index.describe_index_stats()

print(str(res))