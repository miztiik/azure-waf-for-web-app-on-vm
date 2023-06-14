// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-05-20'
  owner: 'miztiik@github'
}

param deploymentParams object
param funcParams object
param tags object
param logAnalyticsWorkspaceId string
param enableDiagnostics bool = true

param uami_name_func string

param saName string
param funcSaName string

param blobContainerName string

param svc_bus_ns_name string
param svc_bus_q_name string


param cosmos_db_accnt_name string
param cosmos_db_name string
param cosmos_db_container_name string

// Get Storage Account Reference
resource r_sa 'Microsoft.Storage/storageAccounts@2021-06-01' existing = {
  name: saName
}

@description('Get function Storage Account Reference')
resource r_sa_1 'Microsoft.Storage/storageAccounts@2021-06-01' existing = {
  name: funcSaName
}

resource r_blob_Ref 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-04-01' existing = {
  name: '${saName}/default/${blobContainerName}'
}

@description('Get function existing User-Assigned Managed Identity')
resource r_uami_func 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: uami_name_func
}

@description('Get Cosmos DB Account Ref')
resource r_cosmos_db_accnt 'Microsoft.DocumentDB/databaseAccounts@2022-08-15' existing = {
  name: cosmos_db_accnt_name
}

@description('Get Service Bus Namespace Reference')
resource r_svc_bus_ns_ref 'Microsoft.ServiceBus/namespaces@2022-01-01-preview' existing = {
  name: svc_bus_ns_name
}

resource r_fnHostingPlan 'Microsoft.Web/serverfarms@2022-03-01' = {
  name: '${funcParams.funcAppPrefix}-fn-plan-${deploymentParams.global_uniqueness}'
  location: deploymentParams.location
  tags: tags
  kind: 'linux'
  sku: {
    // https://learn.microsoft.com/en-us/azure/azure-resource-manager/resource-manager-sku-not-available-errors
    name: funcParams.skuName
    tier: funcParams.funcHostingPlanTier
    family: 'Y'
  }
  properties: {
    reserved: true
  }
}

var r_fn_app_name = replace('${deploymentParams.enterprise_name_suffix}-${funcParams.funcAppPrefix}-${deploymentParams.loc_short_code}-fn-app-${deploymentParams.global_uniqueness}', '_', '-')

resource r_fn_app 'Microsoft.Web/sites@2021-03-01' = {
  name: r_fn_app_name
  location: deploymentParams.location
  kind: 'functionapp,linux'
  tags: tags
  identity: {
    // type: 'SystemAssigned'
    type: 'UserAssigned'
      userAssignedIdentities: {
        '${r_uami_func.id}': {}
      }
  }
  properties: {
    enabled: true
    reserved: true
    serverFarmId: r_fnHostingPlan.id
    clientAffinityEnabled: true
    httpsOnly: true
    siteConfig: {
      linuxFxVersion: 'Python|3.10' //az webapp list-runtimes --linux || az functionapp list-runtimes --os linux -o table
      // ftpsState: 'FtpsOnly'
      ftpsState: 'Disabled'
      minTlsVersion: '1.2'
    }
  }
  dependsOn: [
    r_applicationInsights
  ]
}

resource r_fn_app_settings 'Microsoft.Web/sites/config@2021-03-01' = {
  parent: r_fn_app
  name: 'appsettings' // Reservered Name
  properties: {
    AzureWebJobsStorage: 'DefaultEndpointsProtocol=https;AccountName=${funcSaName};EndpointSuffix=${environment().suffixes.storage};AccountKey=${r_sa_1.listKeys().keys[0].value}'
    // AzureWebJobsStorage__accountName: funcSaName
    // AzureWebJobsStorage__clientId: r_uami_func.properties.clientId
    // AzureWebJobsStorage__credential: 'managedidentity'
    AzureFunctionsJobHost__logging__LogLevel__Default: funcParams.funcLogLevel
    FUNCTION_APP_EDIT_MODE: 'readwrite'
    WEBSITE_CONTENTAZUREFILECONNECTIONSTRING: 'DefaultEndpointsProtocol=https;AccountName=${funcSaName};EndpointSuffix=${environment().suffixes.storage};AccountKey=${r_sa_1.listKeys().keys[0].value}'
    WEBSITE_CONTENTSHARE: toLower(funcParams.funcNamePrefix)
    APPINSIGHTS_INSTRUMENTATIONKEY: r_applicationInsights.properties.InstrumentationKey
    // APPINSIGHTS_INSTRUMENTATIONKEY: '@Microsoft.KeyVault(VaultName=${keyVault.name};SecretName=${keyVault::appInsightsInstrumentationKeySecret.name})'
    FUNCTIONS_WORKER_RUNTIME: 'python'
    FUNCTIONS_EXTENSION_VERSION: '~4'
    // ENABLE_ORYX_BUILD: 'true'
    // SCM_DO_BUILD_DURING_DEPLOYMENT: 'true'

    WAREHOUSE_STORAGE: 'DefaultEndpointsProtocol=https;AccountName=${saName};EndpointSuffix=${environment().suffixes.storage};AccountKey=${r_sa.listKeys().keys[0].value}'
    WAREHOUSE_STORAGE_CONTAINER: blobContainerName

    // SETTINGS FOR MANAGED IDENTITY AUTHENTICAION
    // https://github.com/microsoft/azure-container-apps/issues/442#issuecomment-1272352665
    AZURE_CLIENT_ID: r_uami_func.properties.clientId


    // SETTINGS FOR STORAGE ACCOUNT
    SA_CONNECTION__accountName: saName
    SA_CONNECTION__clientId: r_uami_func.properties.clientId
    SA_CONNECTION__credential: 'managedidentity'
    SA_CONNECTION__serviceUri: 'https://${saName}.blob.${environment().suffixes.storage}'
    SA_CONNECTION__blobServiceUri: 'https://${saName}.blob.${environment().suffixes.storage}' // Producer - https://warehousejwnff5001.blob.core.windows.net
    // SA_CONNECTION__queueServiceUri: 'https://${saName}.queue.${environment().suffixes.storage}'
    BLOB_SVC_ACCOUNT_URL: r_sa.properties.primaryEndpoints.blob
    SA_NAME: r_sa.name
    BLOB_NAME: blobContainerName

    // SETTINGS FOR SERVICE BUS
    SVC_BUS_CONNECTION__fullyQualifiedNamespace: '${svc_bus_ns_name}.servicebus.windows.net'
    SVC_BUS_CONNECTION__credential: 'managedidentity'
    SVC_BUS_CONNECTION__clientId: r_uami_func.properties.clientId
    
    SVC_BUS_FQDN: '${svc_bus_ns_name}.servicebus.windows.net'
    SVC_BUS_Q_NAME: svc_bus_q_name
    // SVC_BUS_TOPIC_NAME: svc_bus_topic_name
    // SALES_EVENTS_SUBSCRIPTION_NAME: sales_events_subscriber_name


    // SETTINGS FOR COSMOS DB
    COSMOS_DB_CONNECTION__accountEndpoint: r_cosmos_db_accnt.properties.documentEndpoint
    COSMOS_DB_CONNECTION__credential: 'managedidentity'
    COSMOS_DB_CONNECTION__clientId: r_uami_func.properties.clientId


    COSMOS_DB_URL: r_cosmos_db_accnt.properties.documentEndpoint
    COSMOS_DB_NAME: cosmos_db_name
    COSMOS_DB_CONTAINER_NAME: cosmos_db_container_name

    // EVENT HUB CONNECTION SETTINGS
    // EVENT_HUB_CONNECTION__fullyQualifiedNamespace: '${event_hub_ns_name}.servicebus.windows.net'
    // EVENT_HUB_CONNECTION__credential: 'managedidentity'
    // EVENT_HUB_CONNECTION__clientId: r_uami_func.properties.clientId
    // EVENT_HUB_FQDN: '${event_hub_ns_name}.servicebus.windows.net'
    // EVENT_HUB_NAME: event_hub_name
    // EVENT_HUB_SALE_EVENTS_CONSUMER_GROUP_NAME: event_hub_sale_events_consumer_group_name

  }
  dependsOn: [
    r_sa
    r_sa_1
  ]
}

resource r_fn_app_logs 'Microsoft.Web/sites/config@2021-03-01' = {
  parent: r_fn_app
  name: 'logs'
  properties: {
    applicationLogs: {
      azureBlobStorage: {
        level: 'Error'
        retentionInDays: 10
        // sasUrl: ''
      }
    }
    httpLogs: {
      fileSystem: {
        retentionInMb: 100
        enabled: true
      }
    }
    detailedErrorMessages: {
      enabled: true
    }
    failedRequestsTracing: {
      enabled: true
    }
  }
  dependsOn: [
    r_fn_app_settings
  ]
}

// Add permissions to the Function App identity
// Azure Built-In Roles Ref: https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles

var storageBlobDataContributor_RoleDefinitionId = resourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')

resource r_attach_blob_contributor_perms_to_role 'Microsoft.Authorization/roleAssignments@2020-10-01-preview' = {
  name:  guid('r_storageBlobDataContributorRoleAssignment', r_blob_Ref.id, storageBlobDataContributor_RoleDefinitionId)
  scope: r_blob_Ref
  properties: {
    roleDefinitionId: storageBlobDataContributor_RoleDefinitionId
    principalId: r_uami_func.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

/*
param blobOwnerRoleId string = 'b7e6dc6d-f1e8-4753-8033-0f276bb0955b'
var blobPermsConditionStr= '((!(ActionMatches{\'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/read\'}) AND !(ActionMatches{\'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write\'}) ) OR (@Resource[Microsoft.Storage/storageAccounts/blobServices/containers:name] StringEquals \'${blobContainerName}\'))'


// Refined Scope with conditions
// https://learn.microsoft.com/en-us/azure/templates/microsoft.authorization/roleassignments?pivots=deployment-language-bicep

resource r_attach_blob_owner_perms_to_role 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('r_attach_blob_owner_perms_to_role', r_uami_func.id, blobOwnerRoleId)
  scope: r_blob_Ref
  properties: {
    description: 'Blob Owner Permission to ResourceGroup scope'
    roleDefinitionId: resourceId('Microsoft.Authorization/roleDefinitions', blobOwnerRoleId)
    principalId: r_uami_func.properties.principalId
    conditionVersion: '2.0'
    condition: blobPermsConditionStr
    principalType: 'ServicePrincipal'
    // https://learn.microsoft.com/en-us/azure/role-based-access-control/troubleshooting?tabs=bicep#symptom---assigning-a-role-to-a-new-principal-sometimes-fails
  }
}
*/

// Assign the Comos Data Plane Owner role to the user-assigned managed identity
var cosmosDbDataContributor_RoleDefinitionId = resourceId('Microsoft.DocumentDB/databaseAccounts/sqlRoleDefinitions', r_cosmos_db_accnt.name, '00000000-0000-0000-0000-000000000002')

resource r_customRoleAssignmentToUsrIdentity 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2021-04-15' = {
  name:  guid(r_uami_func.id, r_cosmos_db_accnt.id, cosmosDbDataContributor_RoleDefinitionId, r_sa.id)
  parent: r_cosmos_db_accnt
  properties: {
    // roleDefinitionId: r_cosmodb_customRoleDef.id
    roleDefinitionId: cosmosDbDataContributor_RoleDefinitionId
    scope: r_cosmos_db_accnt.id
    principalId: r_uami_func.properties.principalId
  }
  dependsOn: [
    r_uami_func
  ]
}

// Azure Service Bus Owner

var svcBusRoleId='090c5cfd-751d-490a-894a-3ce6f1109419'

resource r_attach_svc_bus_owner_perms_to_tole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('r_attach_svc_bus_owner_perms_to_tole', r_fn_app.id, svcBusRoleId)
  scope: r_svc_bus_ns_ref
  properties: {
    description: 'Azure Service Owner Permission to Service Bus Namespace scope'
    roleDefinitionId: resourceId('Microsoft.Authorization/roleDefinitions', svcBusRoleId)
    principalId: r_uami_func.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

// Azure Event Hub Data Owner

var event_hub_owner_role_id='f526a384-b230-433a-b45c-95f59c4a2dec'

resource r_attach_event_hub_owner_perms_to_tole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('r_attach_event_hub_owner_perms_to_tole', r_fn_app.id, event_hub_owner_role_id)
  properties: {
    description: 'Azure Event Hub Owner Permission to Event Hub Namespace scope'
    roleDefinitionId: resourceId('Microsoft.Authorization/roleDefinitions', event_hub_owner_role_id)
    principalId: r_uami_func.properties.principalId
    principalType: 'ServicePrincipal'
  }
}


// Function App Binding
resource r_fn_app_binding 'Microsoft.Web/sites/hostNameBindings@2022-03-01' = {
  parent: r_fn_app
  name: '${r_fn_app.name}.azurewebsites.net'
  properties: {
    siteName: r_fn_app.name
    hostNameType: 'Verified'
  }
}

// Adding Application Insights
resource r_applicationInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: '${funcParams.funcNamePrefix}-fnAppInsights-${deploymentParams.global_uniqueness}'
  location: deploymentParams.location
  tags: tags
  kind: 'web'
  properties: {
    Application_Type: 'web'
    Request_Source: 'rest'
    WorkspaceResourceId: logAnalyticsWorkspaceId
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}

// Enabling Diagnostics for the Function
resource r_fnLogsToAzureMonitor 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (enableDiagnostics) {
  name: '${funcParams.funcNamePrefix}-logs-${deploymentParams.global_uniqueness}'
  scope: r_fn_app
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logs: [
      {
        category: 'FunctionAppLogs'
        enabled: true
      }
    ]
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
      }
    ]
  }
}

// OUTPUTS
output module_metadata object = module_metadata

//FunctionApp Outputs
output fn_app_name string = r_fn_app.name

// Function Outputs
// output fnName string = r_fn_1.name
// output fnIdentity string = r_fn_app.identity.principalId
output fn_app_url string = r_fn_app.properties.defaultHostName
output fn_url string = r_fn_app.properties.defaultHostName
