// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2023-06-04'
  owner: 'miztiik@github'
}

param deploymentParams object
param logicAppParams object
param tags object

param uami_name_logic_app string

param logAnalyticsWorkspaceId string

param saName string

param cosmos_db_accnt_name string

param svc_bus_ns_name string
param svc_bus_q_name string

param fn_app_name string

@description('Get Storage Account Reference')
resource r_sa 'Microsoft.Storage/storageAccounts@2021-06-01' existing = {
  name: saName
}

@description('Get Cosmos DB Account Ref')
resource r_cosmos_db_accnt 'Microsoft.DocumentDB/databaseAccounts@2022-08-15' existing = {
  name: cosmos_db_accnt_name
}


@description('Get function existing User-Assigned Managed Identity')
resource r_uami_logic_app 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: uami_name_logic_app
}

var logic_app_name = replace('${logicAppParams.namePrefix}-${deploymentParams.loc_short_code}-logic-app-${deploymentParams.enterprise_name_suffix}-${deploymentParams.global_uniqueness}', '_', '-')

resource r_logic_app_conn_svc_bus 'Microsoft.Web/connections@2016-06-01' = {
  name: 'conn_svc_bus'
  location: deploymentParams.location
  tags: tags
  properties: {
    displayName: 'conn_svc_bus'
    api: {
      id: subscriptionResourceId('Microsoft.Web/locations/managedApis', deploymentParams.location, 'servicebus')
    }
    parameterValues: {
      connectionString: listKeys(resourceId('Microsoft.ServiceBus/namespaces/authorizationRules', svc_bus_ns_name, 'RootManageSharedAccessKey'), '2017-04-01').primaryConnectionString
    }
  }
}

resource r_logic_app 'Microsoft.Logic/workflows@2019-05-01' = {
  name: logic_app_name
  location: deploymentParams.location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${r_uami_logic_app.id}': {}
    }
  }
  properties: {
    state: 'Enabled'
    definition: {
      '$schema': 'https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#'
      contentVersion: '1.0.0.0'
      parameters: {
        '$connections': {
          defaultValue: {}
          type: 'Object'
        }
      }
      triggers: {
        // manual: {
        //   type: 'Request'
        //   kind: 'Http'
        //   inputs: {
        //     schema: {
        //       properties: {
        //         event_type: {
        //           type: 'string'
        //         }
        //       }
        //       type: 'object'
        //     }
        //   }
        // }
        msg_in_q: {
          type: 'ApiConnection'
          recurrence: {
            frequency: 'Second'
            interval: 5
          }
          evaluatedRecurrence: {
            frequency: 'Minute'
            interval: 3
          }
          inputs: {
            method: 'get'
            host: {
              connection: {
                name: '@parameters(\'$connections\')[\'servicebus\'][\'connectionId\']'
              }
            }
            path: '/@{encodeURIComponent(encodeURIComponent(\'${svc_bus_q_name}\'))}/messages/head'
            queries: {
              queueType: 'Main'
            }
          }
        }
        // connectionReferences: {
        //   servicebus: {
        //     "api": {
        //       "id": "/subscriptions/58379947-56e0-477a-bbe3-8e671aadab83/providers/Microsoft.Web/locations/northeurope/managedApis/servicebus"
        //     },
        //     "connection": {
        //       "id": "/subscriptions/58379947-56e0-477a-bbe3-8e671aadab83/resourceGroups/Miztiik_Enterprises_ne_event_processor_003/providers/Microsoft.Web/connections/servicebus"
        //     },
        //     "connectionName": "servicebus",
        //     "connectionProperties": {
        //       "authentication": {
        //         "type": "ManagedServiceIdentity",
        //         "identity": "/subscriptions/58379947-56e0-477a-bbe3-8e671aadab83/resourceGroups/Miztiik_Enterprises_ne_event_processor_003/providers/Microsoft.ManagedIdentity/userAssignedIdentities/uami_event_processor_003_logic_apps"
        //       }
        //     }
        //   }
        // },
      }
      actions: {
        Condition: {
          type: 'If'
          runAfter: {}
          expression: {
            and: [
              {
                equals: [
                  '@triggerBody()?[\'Properties\']?[\'priority_shipping\']'
                  'True'
                ]
              }
            ]
          }
          actions: {
            'trigger-shipping-process-consumer-fn': {
              inputs: {
                body: '@triggerBody()'
                function: {
                  id: '/subscriptions/${subscription().subscriptionId }/resourceGroups/${resourceGroup().name}/providers/Microsoft.Web/sites/${fn_app_name}/functions/store-events-consumer-fn'
                }
              }
              type: 'Function'
              runAfter: {}
            }
          }
          // else:{
          //   actions:{
          //     'do-nothing':{
          //       type: 'DoNothing'
          //       runAfter: {}
          //     }
          //   }
          // }
        }
      }
    }
    parameters: {
      '$connections': {
        value: {
          servicebus: {
            connectionId: r_logic_app_conn_svc_bus.id
            connectionName: r_logic_app_conn_svc_bus.name
            id: subscriptionResourceId('Microsoft.Web/locations/managedApis', deploymentParams.location, 'servicebus')
          }
        }
      }
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
  name: guid(r_uami_logic_app.id, r_cosmos_db_accnt.id, cosmosDbDataContributor_RoleDefinitionId, r_sa.id)
  parent: r_cosmos_db_accnt
  properties: {
    // roleDefinitionId: r_cosmodb_customRoleDef.id
    roleDefinitionId: cosmosDbDataContributor_RoleDefinitionId
    scope: r_cosmos_db_accnt.id
    principalId: r_uami_logic_app.properties.principalId
  }
  dependsOn: [
    r_uami_logic_app
  ]
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
  scope: r_logic_app
  name: guid(r_logic_app.id, r_uami_logic_app.id, role.name)
  properties: {
    roleDefinitionId: role.id
    principalId: r_uami_logic_app.properties.principalId
  }
}]

////////////////////////////////////////////
//                                        //
//         Diagnostic Settings            //
//                                        //
////////////////////////////////////////////

// Stream Analytics Diagnostic Settings
resource logic_app_diag 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: '${logic_app_name}-diag'
  scope: r_logic_app
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
