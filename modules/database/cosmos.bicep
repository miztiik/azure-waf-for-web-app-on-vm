// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-05-29'
  owner: 'miztiik@github'
}

param deploymentParams object
param tags object

param cosmosDbParams object


var cosmos_db_accnt_name = replace('${deploymentParams.enterprise_name_suffix}-${deploymentParams.loc_short_code}-db-account-${deploymentParams.global_uniqueness}', '_', '-')

// Create CosmosDB Account
resource r_cosmos_db_account 'Microsoft.DocumentDB/databaseAccounts@2022-08-15' = {
  name: cosmos_db_accnt_name
  location: deploymentParams.location
  kind: 'GlobalDocumentDB'
  tags: tags
  properties: {
    publicNetworkAccess: 'Enabled'
    databaseAccountOfferType: 'Standard'
    enableAutomaticFailover: true
    consistencyPolicy: {
      defaultConsistencyLevel: 'Session'
    }
    locations: [
      {
        locationName: deploymentParams.location
        isZoneRedundant: false
      }
    ]
    
    backupPolicy: {
      type: 'Continuous'
    }
    capabilities: [
      {
        name: 'EnableServerless'
      }
    ]
  }
}

// Create CosmosDB Database
var databaseName =  '${cosmosDbParams.cosmosDbNamePrefix}-db-${deploymentParams.global_uniqueness}'

resource r_cosmos_db 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2021-06-15' = {
  parent: r_cosmos_db_account
  name: databaseName
  properties: {
    resource: {
      id: databaseName
    }
  }
}

var containerName = '${cosmosDbParams.cosmosDbNamePrefix}-container-${deploymentParams.global_uniqueness}'

// Create CosmosDB Container
resource r_cosmos_db_container 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2022-08-15' = {
  name: containerName
  parent: r_cosmos_db
  properties: {
    resource: {
      id: containerName
      partitionKey: {
        paths: [
          '/id'
        ]
        kind: 'Hash'
      }
      indexingPolicy: {
        indexingMode: 'consistent'
        includedPaths: [
          {
            path: '/*'
          }
        ]
        excludedPaths: [
          {
            path: '/_etag/?'
          }
        ] 
      }
      conflictResolutionPolicy: {
        mode: 'LastWriterWins'
        conflictResolutionPath: '/_ts'
      }
    }
  }
}

// OUTPUTS
output module_metadata object = module_metadata

output cosmos_db_accnt_name string = r_cosmos_db_account.name
output cosmos_db_name string = r_cosmos_db.name
output cosmos_db_container_name string = r_cosmos_db_container.name

