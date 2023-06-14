// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-06-04'
  owner: 'miztiik@github'
}
param deploymentParams object
param streamAnalyticsParams object
param tags object

param saName string
param blobContainerName string

param cosmos_db_accnt_name string

param event_hub_ns_name string
param event_hub_name string
param event_hub_sale_events_consumer_group_name string

param svc_bus_ns_name string
param svc_bus_q_name string


param uami_name_stream_analytics string

param logAnalyticsPayGWorkspaceId string

@description('Get function existing User-Assigned Managed Identity')
resource r_uami_stream_analytics 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: uami_name_stream_analytics
}

@description('Get Storage Account Reference')
resource r_sa 'Microsoft.Storage/storageAccounts@2021-06-01' existing = {
  name: saName
}

@description('Get Cosmos DB Account Ref')
resource r_cosmos_db_accnt 'Microsoft.DocumentDB/databaseAccounts@2022-08-15' existing = {
  name: cosmos_db_accnt_name
}

// Get Event Hub Namespace Ref
resource r_event_hub_ns_ref 'Microsoft.EventHub/namespaces@2022-01-01-preview' existing = {
  name: event_hub_ns_name
}

@description('Get Service Bus Namespace Reference')
resource r_svc_bus_ns_ref 'Microsoft.ServiceBus/namespaces@2022-01-01-preview' existing = {
  name: svc_bus_ns_name
}


var stream_analytics_job_name =  replace('${streamAnalyticsParams.JobNamePrefix}-${deploymentParams.loc_short_code}-job-${deploymentParams.enterprise_name_suffix}-${deploymentParams.global_uniqueness}', '_', '-')

resource r_stream_analytics_job 'Microsoft.StreamAnalytics/streamingjobs@2021-10-01-preview' = {
  name: stream_analytics_job_name
  location: deploymentParams.location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${r_uami_stream_analytics.id}': {}
    }
  }
  properties: {
    sku: {
      name: 'Standard'
    }
    jobType: 'Cloud'
    // jobStorageAccount: {
    //     authenticationMode: 'Msi'
    //     accountName: saName
    // }
    compatibilityLevel: '1.2'
    // outputErrorPolicy: 'Stop'
    // outputStartMode: 'JobStartTime'
    eventsOutOfOrderPolicy: 'Adjust'
    eventsLateArrivalMaxDelayInSeconds: 240
    eventsOutOfOrderMaxDelayInSeconds: 240
  }
}


var stream_analytics_job_input_name =  replace('${streamAnalyticsParams.JobNamePrefix}-input-${deploymentParams.global_uniqueness}', '_', '-')

resource r_stream_analytics_job_input 'Microsoft.StreamAnalytics/streamingjobs/inputs@2020-03-01' = {
  name: stream_analytics_job_input_name
  parent: r_stream_analytics_job
  properties: {
    serialization: {
      type: 'Json'
      properties: {
        encoding: 'UTF8'
      }
    }
    type: 'Stream'
    datasource: {
      type: 'Microsoft.EventHub/EventHub'
      properties: {
        authenticationMode: 'Msi'
        serviceBusNamespace: event_hub_ns_name
        eventHubName: event_hub_name
        consumerGroupName: event_hub_sale_events_consumer_group_name
      }
    }
  }
}

var r_output_to_svc_bus_name =  replace('${streamAnalyticsParams.JobNamePrefix}-output-${deploymentParams.global_uniqueness}', '_', '-')
resource r_output_to_svc_bus 'Microsoft.StreamAnalytics/streamingjobs/outputs@2021-10-01-preview' = {
  name: r_output_to_svc_bus_name
  parent: r_stream_analytics_job
  properties: {
    datasource: {
      type: 'Microsoft.ServiceBus/Queue'
      properties: {
        authenticationMode: 'Msi'
        propertyColumns: [
          'event_type'
        ]
        serviceBusNamespace: svc_bus_ns_name
        queueName: svc_bus_q_name
      }
    }
    serialization: {
      type: 'Json'
      properties: {
        encoding: 'UTF8'
        format: 'Array'
      }
    }
  }
}

var find_suspicous_events_query_with_vars = '''
SELECT 
  store_id,
  COLLECT(id) as ids,
  System.Timestamp() as WindowEnd,
  DATEADD(minute, -5, System.Timestamp()) as WindowStart
INTO [OutputAlias_1]
FROM [EVENT_HUB_NAME]
WHERE 
  [discount] > 50
  AND [priority_shipping] = 1
  AND [event_type] = 'sale_event'
GROUP BY 
  store_id, TumblingWindow(minute, 5)

SELECT 
  store_id,
  COLLECT(id) as ids,
  System.Timestamp() as WindowEnd,
  DATEADD(minute, -5, System.Timestamp()) as WindowStart
INTO [OutputAlias_2]
FROM [EVENT_HUB_NAME]
WHERE 
  [discount] > 50
  AND [priority_shipping] = 1
  AND [event_type] = 'sale_event'
GROUP BY 
  store_id, TumblingWindow(minute, 5)'''

var find_suspicous_events_query_input_replace =  replace(find_suspicous_events_query_with_vars, '[EVENT_HUB_NAME]', '[${r_stream_analytics_job_input.name}]')
var find_suspicous_events_query_output_replace =  replace(replace(find_suspicous_events_query_input_replace, '[OutputAlias_1]', '[${r_output_to_svc_bus.name}]'), '[OutputAlias_2]', '[${r_output_to_blob.name}]')
var find_suspicous_events_query = find_suspicous_events_query_output_replace

resource stream_analytics_job_transformation 'Microsoft.StreamAnalytics/streamingjobs/transformations@2020-03-01' = {
  name: '${stream_analytics_job_name}-transformation-${deploymentParams.global_uniqueness}'
  parent: r_stream_analytics_job
  properties: {
    query: find_suspicous_events_query
    streamingUnits: 3
    validStreamingUnits: [
      1
    ]
  }
}



var r_output_to_blob_name =  replace('${streamAnalyticsParams.JobNamePrefix}-output-to-blob-${deploymentParams.global_uniqueness}', '_', '-')

resource r_output_to_blob 'Microsoft.StreamAnalytics/streamingjobs/outputs@2021-10-01-preview' = {
  name: r_output_to_blob_name
  parent: r_stream_analytics_job
  properties: {
    datasource: {
      type: 'Microsoft.Storage/Blob'
      properties: {
        storageAccounts: [
          {
            accountName: r_sa.name
          }
        ]
        container: blobContainerName
        pathPattern: 'suspicious-events/{date}/{time}'
        dateFormat: 'yyyy-MM-dd'
        timeFormat: 'HH-mm'
        blobWriteMode: 'Once'
        authenticationMode: 'Msi'
      }
    }
    serialization: {
      type: 'Json'
      properties: {
        encoding: 'UTF8'
        format: 'Array'
        // format: 'LineSeparated'
      }
    }
  }
}




/*
var r_output_to_cosmos_name =  replace('${streamAnalyticsParams.JobNamePrefix}-output-to-cosmos-${deploymentParams.global_uniqueness}', '_', '-')

resource r_output_to_cosmos 'Microsoft.StreamAnalytics/streamingjobs/outputs@2021-10-01-preview' = {
  name: r_output_to_cosmos_name
  parent: r_stream_analytics_job
  properties: {
    datasource: {
      type: 'Microsoft.Storage/DocumentDB'
      properties: {
        authenticationMode: 'Msi'
        accountId: cosmosAccountName
        accountKey: '@Microsoft.KeyVault(VaultName=${kv.name};SecretName=${cosmosPrimaryKey})'
        database: cosmosDatabaseName
        collectionNamePattern: cosmosContainerName
        partitionKey: cosmosPartialKey
        documentId: ''
      }
    }
    serialization: {
      type: 'Json'
      properties: {
        encoding: 'UTF8'
      }
    }
  }
}

*/


///////////////////////////////////////////
//                                       //
//   Attach Permissions to the Identity  //
//                                       //
///////////////////////////////////////////


// Azure Built-In Roles Ref: https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles

var storageBlobDataContributor_RoleDefinitionId = resourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')

resource r_attach_blob_contributor_perms_to_role 'Microsoft.Authorization/roleAssignments@2020-10-01-preview' = {
  name:  guid('r_storageBlobDataContributorRoleAssignment', r_sa.id, storageBlobDataContributor_RoleDefinitionId)
  scope: r_sa
  properties: {
    roleDefinitionId: storageBlobDataContributor_RoleDefinitionId
    principalId: r_uami_stream_analytics.properties.principalId
    principalType: 'ServicePrincipal'
  }
}


// Assign the Cosmos Data Plane Owner role to the user-assigned managed identity
var cosmosDbDataContributor_RoleDefinitionId = resourceId('Microsoft.DocumentDB/databaseAccounts/sqlRoleDefinitions', r_cosmos_db_accnt.name, '00000000-0000-0000-0000-000000000002')

resource r_customRoleAssignmentToUsrIdentity 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2021-04-15' = {
  name:  guid(r_uami_stream_analytics.id, r_cosmos_db_accnt.id, cosmosDbDataContributor_RoleDefinitionId, r_sa.id)
  parent: r_cosmos_db_accnt
  properties: {
    // roleDefinitionId: r_cosmodb_customRoleDef.id
    roleDefinitionId: cosmosDbDataContributor_RoleDefinitionId
    scope: r_cosmos_db_accnt.id
    principalId: r_uami_stream_analytics.properties.principalId
  }
  dependsOn: [
    r_uami_stream_analytics
  ]
}

// Azure Service Bus Owner

var svcBusRoleId='090c5cfd-751d-490a-894a-3ce6f1109419'

resource r_attach_svc_bus_owner_perms_to_tole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('r_attach_svc_bus_owner_perms_to_tole', r_stream_analytics_job.id, svcBusRoleId)
  scope: r_svc_bus_ns_ref
  properties: {
    description: 'Azure Service Owner Permission to Service Bus Namespace scope'
    roleDefinitionId: resourceId('Microsoft.Authorization/roleDefinitions', svcBusRoleId)
    principalId: r_uami_stream_analytics.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

// Azure Event Hub Data Owner

var event_hub_owner_role_id='f526a384-b230-433a-b45c-95f59c4a2dec'

resource r_attach_event_hub_owner_perms_to_tole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('r_attach_event_hub_owner_perms_to_tole', r_stream_analytics_job.id, event_hub_owner_role_id)
  scope: r_event_hub_ns_ref
  properties: {
    description: 'Azure Event Hub Owner Permission to Event Hub Namespace scope'
    roleDefinitionId: resourceId('Microsoft.Authorization/roleDefinitions', event_hub_owner_role_id)
    principalId: r_uami_stream_analytics.properties.principalId
    principalType: 'ServicePrincipal'
  }
}


////////////////////////////////////////////
//                                        //
//         Diagnostic Settings            //
//                                        //
////////////////////////////////////////////



// Stream Analytics Diagnostic Settings
resource stream_analytics_job_name_diag 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: '${stream_analytics_job_name}_diag'
  scope: r_stream_analytics_job
  properties: {
    workspaceId: logAnalyticsPayGWorkspaceId
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
