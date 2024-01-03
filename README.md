# Azure-Knowledge-Retrieval

This is a simple example of a Knowledge Retrieval System Based on ChatGPT and your own Data in a 'Knowledge Base'. It is run on Azure.
Azure Functions will be the backend, Pinecone the Vectordatabase. An Azure Blob Storage will be the Knowledge Base and contains the Information to process and store in the Vectordatabase.

Processing of the files will be done in two steps:
1. The files will be chunked in cleartext and saved with metadata in Blob storage in storage blob
2. The files will be retrived and embedded via the OpenAI-Api and inserted into the Vectordatabase added with metadata

The querying of the database:
1. Question from the User will be embedded and used to derive the top k most similar matches
2. The answers will be used to generate a prompt and query the OpenAI API
3. This answered will be rendered in a viable format und used to generate SAS links to the corresponding files in the Knowledge Base

