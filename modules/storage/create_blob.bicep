// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-05-23'
  owner: 'miztiik@github'
}

param deploymentParams object
param storageAccountParams object
param storageAccountName string
param enableDiagnostics bool = false
param logAnalyticsWorkspaceId string

param storageAccountName_1 string

// Get reference of SA
resource r_sa 'Microsoft.Storage/storageAccounts@2021-06-01' existing = {
  name: storageAccountName
}

// Create a blob storage container in the storage account
resource r_blobSvc 'Microsoft.Storage/storageAccounts/blobServices@2021-06-01' = {
  parent: r_sa
  name: 'default'
  properties:{
    cors: {
      corsRules: []
    }
  }
}

resource r_blobContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-04-01' = {
  parent: r_blobSvc
  name: '${storageAccountParams.blobNamePrefix}-blob-${deploymentParams.global_uniqueness}'
  properties: {
    publicAccess: 'None'
  }
}

// Enabling Diagnostics for the storage account
resource storageDataPlaneLogs 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (enableDiagnostics) {
  name: '${storageAccountName}-Diaglogs'
  scope: r_sa
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logs: [
      {
        category: 'StorageWrite'
        enabled: true
      }
    ]
    metrics: [
      {
        category: 'Transaction'
        enabled: true
      }
    ]
  }
}


// OUTPUTS
output module_metadata object = module_metadata

output blobContainerId string = r_blobContainer.id
output blobContainerName string = r_blobContainer.name


// Get reference of SA
resource r_sa_1 'Microsoft.Storage/storageAccounts@2021-06-01' existing = {
  name: storageAccountName_1
}

// Create a blob storage container in the storage account
resource r_blobSvc_1 'Microsoft.Storage/storageAccounts/blobServices@2021-06-01' = {
  parent: r_sa_1
  name: 'default'
  properties:{
    cors: {
      corsRules: []
    }
  }
}

// resource r_blobContainer_1 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-04-01' = {
//   parent: r_blobSvc_1
//   name: '${storageAccountParams.blobNamePrefix}-blob-${deploymentParams.global_uniqueness}'
//   properties: {
//     publicAccess: 'None'
//   }
// }
// output blobContainerId_1 string = r_blobContainer_1.id
// output blobContainerName_1 string = r_blobContainer_1.name
