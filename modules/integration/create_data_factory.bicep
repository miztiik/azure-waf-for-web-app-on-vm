// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-05-23'
  owner: 'miztiik@github'
}

param deploymentParams object
param data_factory_params object
param tags object

param logAnalyticsWorkspaceId string

param uami_data_factory string

param saName string
param blobContainerName string

param cosmos_db_accnt_name string


@description('Get function existing User-Assigned Managed Identity')
resource r_uami_data_factory 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: uami_data_factory
}

@description('Get Storage Account Reference')
resource r_sa 'Microsoft.Storage/storageAccounts@2021-06-01' existing = {
  name: saName
}

@description('Get Cosmos DB Account Ref')
resource r_cosmos_db_accnt 'Microsoft.DocumentDB/databaseAccounts@2022-08-15' existing = {
  name: cosmos_db_accnt_name
}

var data_factory_name = replace('${data_factory_params.name_prefix}-${deploymentParams.loc_short_code}-data-factory-${deploymentParams.enterprise_name_suffix}-${deploymentParams.global_uniqueness}', '_', '-')


resource r_data_factory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: data_factory_name
  tags: tags
  location: deploymentParams.location
  identity: {
    // type: 'SystemAssigned'
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${r_uami_data_factory.id}': {}
    }
  }
}

resource r_data_factory_link_to_sa 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: r_data_factory
  name: '${data_factory_name}_link_to_sa'
  properties: {
    type: 'AzureBlobStorage'
    description: 'Miztiik Link to Azure Storage Account'
    typeProperties: {
      serviceEndpoint: 'https://${r_sa.name}.blob.${environment().suffixes.storage}'
      // connectionString: 'DefaultEndpointsProtocol=https;AccountName=${r_sa.name};AccountKey=${r_sa.listKeys().keys[0].value}'
      azureCloudType: 'AzurePublic'
      accountKind: 'StorageV2'
      credential: {
        referenceName: r_data_factory_credential_uami.name
        type: 'CredentialReference'
      }
    }
  }
}


resource r_data_factory_data_set_in 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  parent: r_data_factory
  name: 'data_set_store_events_in'
  properties: {
    linkedServiceName: {
      referenceName: r_data_factory_link_to_sa.name
      type: 'LinkedServiceReference'
    }
    type: 'Json'
    // type: 'Binary'
    typeProperties: {
      location: {
        type: 'AzureBlobStorageLocation'
        container: blobContainerName
        folderPath: 'store_events/raw'
        fileName: '*.json'
      }
    }
    schema: [
      { name: 'id', type: 'string' }
      { name: 'store_id', type: 'integer' }
      { name: 'store_fqdn', type: 'string' }
      { name: 'store_ip', type: 'string' }
      { name: 'cust_id', type: 'integer' }
      { name: 'category', type: 'string' }
      { name: 'sku', type: 'integer' }
      { name: 'price', type: 'number' }
      { name: 'qty', type: 'integer' }
      { name: 'discount', type: 'integer' }
      { name: 'gift_wrap', type: 'boolean' }
      { name: 'variant', type: 'string' }
      { name: 'priority_shipping', type: 'boolean' }
      { name: 'payment_method', type: 'string' }
      { name: 'ts', type: 'string' }
      { name: 'contact_me', type: 'string' }
      { name: 'is_return', type: 'boolean' }
      { name: 'event_type', type: 'string' }
      { name: 'dt', type: 'string' }
      { name: 'bad_msg', type: 'boolean' }
    ]
  }
}

resource r_data_factory_data_set_out 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  parent: r_data_factory
  name: 'data_set_store_events_out'
  properties: {
    linkedServiceName: {
      referenceName: r_data_factory_link_to_sa.name
      type: 'LinkedServiceReference'
    }
    // type: 'Binary'
    type: 'Parquet'
    typeProperties: {
      location: {
        type: 'AzureBlobStorageLocation'
        container: blobContainerName
        folderPath: 'output'
      }
    }
  }
}

resource r_data_factory_pipeline 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  parent: r_data_factory
  name: '${data_factory_name}-pipeline'
  properties: {
    // activities: [ json(loadTextContent('../content/adfPipeline.json')) ]
    activities: [
      {
        name: 'MiztiikDataProcessorActivity'
        type: 'Copy'
        typeProperties: {
          source: {
            type: 'BinarySource'
            storeSettings: {
              type: 'AzureBlobStorageReadSettings'
              recursive: true
            }
          }
          sink: {
            type: 'BinarySink'
            storeSettings: {
              type: 'AzureBlobStorageWriteSettings'
            }
          }
          enableStaging: false
        }
        inputs: [
          {
            referenceName: r_data_factory_data_set_in.name
            type: 'DatasetReference'
          }
        ]
        outputs: [
          {
            referenceName: r_data_factory_data_set_out.name
            type: 'DatasetReference'
          }
        ]
      }
    ]
  }
}


// Create the credentials
resource r_data_factory_credential_uami 'Microsoft.DataFactory/factories/credentials@2018-06-01' = {
  name: r_uami_data_factory.name
  parent: r_data_factory
  properties: {
    type: 'ManagedIdentity'
    typeProperties: {
      resourceId: r_uami_data_factory.id
    }
  }
}

/*
resource r_data_factory_trigger 'Microsoft.DataFactory/factories/triggers@2018-06-01' = {
  name: '${data_factory_name}-trigger-1'
  dependsOn: [
    r_data_factory_pipeline
  ]
  properties: {
    pipelines: [
      {
        pipelineReference: {
          referenceName: 'pipeline-1'
          type: 'PipelineReference'
        }
        parameters: {}
      }
    ]
    type: 'BlobEventsTrigger'
    typeProperties: {
      blobPathBeginsWith: '/testdata/blobs/'
      ignoreEmptyBlobs: true
      scope: '/subscriptions/7a8fb3e5-9699-4869-93b4-c011fb7fc532/resourceGroups/contoso-data/providers/Microsoft.Storage/storageAccounts/bi2sz4tdlw5e4'
      events: [
        'Microsoft.Storage.BlobCreated'
      ]
    }
  }
}
*/

resource dataFactoryDataflowAggregateUsageDetailsRes 'Microsoft.DataFactory/factories/dataflows@2018-06-01' = {
  parent: r_data_factory
  name: 'DailyStoreAggregatesSales'
  properties: {
    type: 'MappingDataFlow'
    typeProperties: {
      sources: [
        {
          dataset: {
            referenceName: r_data_factory_data_set_in.name
            type: 'DatasetReference'
          }
          name: 'storeEventsRaw'
          description: 'Raw Events for all stores'
        }
      ]
      sinks: [
        {
          dataset: {
            referenceName: r_data_factory_data_set_out.name
            type: 'DatasetReference'
          }
          name: 'storeEventsProcessed'
          description: 'Processed events all stores'
        }
      ]
      transformations: [
        {
          name: 'notNULLstoreid'
          description: 'Removing records without store id'
        }
        {
          name: 'saleRevenue'
          description: 'Revenue for that event computed for qty, price and discount'
        }
      ]
      scriptLines: [
        'source(output('
        '		id as string,'
        '		store_id as integer,'
        '		store_fqdn as string,'
        '		store_ip as string,'
        '		cust_id as integer,'
        '		category as string,'
        '		sku as integer,'
        '		price as double,'
        '		qty as integer,'
        '		discount as integer,'
        '		gift_wrap as boolean,'
        '		variant as string,'
        '		priority_shipping as boolean,'
        '		payment_method as string,'
        '		ts as string,'
        '		contact_me as string,'
        '		is_return as boolean,'
        '		event_type as string,'
        '		dt as string,'
        '		bad_msg as boolean'
        '	),'
        '	allowSchemaDrift: true,'
        '	validateSchema: false,'
        '	ignoreNoFilesFound: false,'
        '	documentForm: \'singleDocument\') ~> stagedData'
        'stagedData filter(!isNull(store_id)) ~> notNULLstoreid'
        'notNULLstoreid derive(sale_revenue = qty*price*(discount/100)) ~> saleRevenue'
        'saleRevenue sink(allowSchemaDrift: true,'
        '	validateSchema: false,'
        '	format: \'parquet\','
        '	skipDuplicateMapInputs: true,'
        '	skipDuplicateMapOutputs: true) ~> storeEventsProcessed'
      ]
      // script: 'source(output(\n\t\tid as string,\n\t\tdate as string,\n\t\tresourceId as string,\n\t\tresourceName as string,\n\t\tlocation as string,\n\t\tmeterId as string,\n\t\tusageQuantity as double,\n\t\tpretaxCost as double,\n\t\tcurrency as string,\n\t\tisEstimated as boolean,\n\t\tsubscriptionGuid as string,\n\t\tresourceTypeName as string,\n\t\tresourceTypeGuid as string,\n\t\tofferId as string\n\t),\n\tallowSchemaDrift: false,\n\tvalidateSchema: false,\n\twildcardPaths:[\'usage-details.json\']) ~> blobData\nmappedData aggregate(groupBy(partitionKey,\n\t\tdate),\n\tusageQuantity = sum(usageQuantity),\n\t\tpretaxCost = sum(pretaxCost),\n\t\teach(match(!in([\'partitionKey\',\'date\',\'usageQuantity\',\'pretaxCost\'],name)), $$ = first($$))) ~> aggregatedData\nblobData derive(id = lower(toString(byName(\'id\'))),\n\t\tdate = toString(byName(\'date\')),\n\t\tresourceId = toString(byName(\'resourceId\')),\n\t\tresourceName = lower(toString(byName(\'resourceName\'))),\n\t\tlocation = toString(byName(\'location\')),\n\t\tmeterId = lower(toString(byName(\'meterId\'))),\n\t\tusageQuantity = toDouble(byName(\'usageQuantity\')),\n\t\tpretaxCost = toDouble(byName(\'pretaxCost\')),\n\t\tcurrency = toString(byName(\'currency\')),\n\t\tisEstimated = toBoolean(byName(\'isEstimated\')),\n\t\tsubscriptionGuid = lower(toString(byName(\'subscriptionGuid\'))),\n\t\tresourceTypeName = toString(byName(\'resourceTypeName\')),\n\t\tresourceTypeGuid = lower(toString(byName(\'resourceTypeGuid\'))),\n\t\tofferId = toString(byName(\'offerId\')),\n\t\tresourceGroupName = lower(regexExtract(byName(\'resourceId\'), \'\\\\/(?i)resourceGroups\\\\/(.*?)\\\\/\', 1)),\n\t\tpartitionKey = lower(concat(byName(\'resourceName\'), \':\', byName(\'meterId\')))) ~> mappedData\naggregatedData sink(input(\n\t\tpartitionKey as string,\n\t\tdate as string,\n\t\tusageQuantity as string,\n\t\tpretaxCost as string,\n\t\tid as string,\n\t\tresourceId as string,\n\t\tresourceName as string,\n\t\tlocation as string,\n\t\tmeterId as string,\n\t\tcurrency as string,\n\t\tisEstimated as string,\n\t\tsubscriptionGuid as string,\n\t\tresourceTypeName as string,\n\t\tresourceTypeGuid as string,\n\t\tofferId as string,\n\t\tresourceGroupName as string\n\t),\n\tallowSchemaDrift: true,\n\tvalidateSchema: false,\n\tpartitionFileNames:[\'usage-details.csv\'],\n\tpartitionBy(\'hash\', 1),\n\tskipDuplicateMapInputs: true,\n\tskipDuplicateMapOutputs: true) ~> tableData'
    }
  }
}


///////////////////////////////////////////
//                                       //
//   Attach Permissions to the Identity  //
//                                       //
///////////////////////////////////////////

// Azure Built-In Roles Ref: https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles

var cosmosDbDataContributor_RoleDefinitionId = resourceId('Microsoft.DocumentDB/databaseAccounts/sqlRoleDefinitions', r_cosmos_db_accnt.name, '00000000-0000-0000-0000-000000000002')

@description('Assign the Cosmos Data Plane Owner role to the user-assigned managed identity')
resource r_customRoleAssignmentToUsrIdentity 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2021-04-15' = {
  name: guid(r_data_factory.id, r_cosmos_db_accnt.id, cosmosDbDataContributor_RoleDefinitionId, r_sa.id)
  parent: r_cosmos_db_accnt
  properties: {
    // roleDefinitionId: r_cosmodb_customRoleDef.id
    roleDefinitionId: cosmosDbDataContributor_RoleDefinitionId
    scope: r_cosmos_db_accnt.id
    principalId: r_uami_data_factory.properties.principalId
  }
}

var builtInRoleNames = [
  {
    name: 'Owner'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '8e3af657-a8ff-443c-a75c-2fe8c4bcb635')
  }
  {
    name: 'Contributor'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'b24988ac-6180-42a0-ab88-20f7382dd24c')
  }
  {
    name: 'Reader'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'acdd72a7-3385-48ef-bd42-f606fba81ae7')
  }
  {
    name: 'Storage Blob Data Contributor'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
  }
  {
    name: 'Azure Service Bus Data Owner'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '090c5cfd-751d-490a-894a-3ce6f1109419')
  }
  {
    name: 'Azure Sentinel Automation Contributor'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'f4c81013-99ee-4d62-a7ee-b3f1f648599a')
  }
  {
    name: 'Log Analytics Contributor'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '92aaf0da-9dab-42b6-94a3-d43ce8d16293')
  }
  {
    name: 'Data Factory Contributor'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '673868aa-7521-48a0-acc6-0f60742d39f5')
  }
  {
    name: 'Logic App Contributor'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '87a39d53-fc1b-424a-814c-f7e04687dc9e')
  }
  {
    name: 'Logic App Operator'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '515c2055-d9d4-4321-b1b9-bd0c9a0f79fe')
  }
  {
    name: 'Managed Application Contributor Role'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '641177b8-a67a-45b9-a033-47bc880bb21e')
  }
  {
    name: 'Managed Application Operator Role'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'c7393b34-138c-406f-901b-d8cf2b17e6ae')
  }
  {
    name: 'Managed Applications Reader'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'b9331d33-8a36-4f8c-b097-4f54124fdb44')
  }
  {
    name: 'Monitoring Contributor'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '749f88d5-cbae-40b8-bcfc-e573ddc772fa')
  }
  {
    name: 'Monitoring Metrics Publisher'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '3913510d-42f4-4e42-8a64-420c390055eb')
  }
  {
    name: 'Monitoring Reader'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '43d0d8ad-25c7-4714-9337-8ba259a9fe05')
  }
  {
    name: 'Resource Policy Contributor'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '36243c78-bf99-498c-9df9-86d9f8d28608')
  }
  {
    name: 'User Access Administrator'
    id: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '18d7d88d-d35e-4fb5-a5c3-7773c20a72d9')
  }
]

@description('Assign the Permissions to Role')
resource roleAssignment 'Microsoft.Authorization/roleAssignments@2020-04-01-preview' = [for role in builtInRoleNames: {
  name: guid(r_data_factory.id, r_uami_data_factory.id, role.name)
  properties: {
    roleDefinitionId: role.id
    principalId: r_uami_data_factory.properties.principalId
  }
}]

////////////////////////////////////////////
//                                        //
//         Diagnostic Settings            //
//                                        //
////////////////////////////////////////////



// Stream Analytics Diagnostic Settings
resource r_data_factory_diag 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: '${data_factory_name}_diag'
  scope: r_data_factory
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logs: [
      {
        categoryGroup: 'allLogs'
        enabled: true
        retentionPolicy: {
          days: 90
          enabled: true
        }
      }
    ]
    metrics: [
      {
        category: 'AllMetrics'
        timeGrain: 'PT5M'
        enabled: true
        retentionPolicy: {
          enabled: false
          days: 0
        }
      }
    ]
  }
}

// OUTPUTS
output module_metadata object = module_metadata
