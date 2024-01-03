

resource "azurerm_resource_group" "knowledge_retrieval" {
  name     = "Knowledge_Retrieval"
  location = "East US"
}

resource "azurerm_storage_account" "example" {
  name                     = "examplestorageaccount"
  resource_group_name      = azurerm_resource_group.knowledge_retrieval.name
  location                 = azurerm_resource_group.knowledge_retrieval.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

# Pinecone will be used via the API

# Azure Function App without App Service Plan
resource "azurerm_function_app" "example_function" {
  name                      = "example-function-app"
  location                  = azurerm_resource_group.knowledge_retrieval.location
  resource_group_name       = azurerm_resource_group.knowledge_retrieval.name
  storage_account_name      = azurerm_storage_account.example.name
  storage_account_access_key = azurerm_storage_account.example.primary_access_key
  version                   = "~3"

  app_settings = {
    "my_storage" = azurerm_storage_account.example.primary_connection_string
    // Other app settings can be added here
  }
}
