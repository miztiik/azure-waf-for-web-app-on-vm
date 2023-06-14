// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-05-26'
  owner: 'miztiik@github'
}

param deploymentParams object
param storageAccountParams object
param funcParams object
param tags object = resourceGroup().tags

// var = uniqStr2 = guid(resourceGroup().id, "asda")
var uniqStr = substring(uniqueString(resourceGroup().id), 0, 6)
var saName = '${storageAccountParams.storageAccountNamePrefix}${deploymentParams.loc_short_code}${uniqStr}${deploymentParams.global_uniqueness}'

resource r_sa 'Microsoft.Storage/storageAccounts@2022-05-01' = {
  name: saName
  location: deploymentParams.location
  tags: tags
  sku: {
    name: '${storageAccountParams.fault_tolerant_sku}'
  }
  kind: '${storageAccountParams.kind}'
  properties: {
    minimumTlsVersion: '${storageAccountParams.minimumTlsVersion}'
    allowBlobPublicAccess: storageAccountParams.allowBlobPublicAccess
    defaultToOAuthAuthentication: true
    supportsHttpsTrafficOnly: true
    networkAcls: {
      bypass: 'AzureServices'
      defaultAction: 'Allow'
    }
    encryption:  {
      services: {
        file: {
          keyType: 'Account'
          enabled: true
        }
        blob: {
          keyType: 'Account'
          enabled: true
        }
      }
      keySource: 'Microsoft.Storage'
    }

    accessTier: 'Hot'
  }
}


// var = uniqStr2 = guid(resourceGroup().id, "asda")
var uniqStr_1 = substring(uniqueString(resourceGroup().id), 0, 6)
var saName_1 = '${funcParams.funcStorageAccountNamePrefix}${uniqStr_1}${deploymentParams.global_uniqueness}'


// Storage Account for Warehouse
resource r_sa_1 'Microsoft.Storage/storageAccounts@2022-05-01' = {
  name: saName_1 
  location: deploymentParams.location
  tags: tags
  sku: {
    name: '${storageAccountParams.fault_tolerant_sku}'
  }
  kind: '${storageAccountParams.kind}'
  properties: {
    minimumTlsVersion: '${storageAccountParams.minimumTlsVersion}'
    allowBlobPublicAccess: storageAccountParams.allowBlobPublicAccess
    defaultToOAuthAuthentication: true
    supportsHttpsTrafficOnly: true
    networkAcls: {
      bypass: 'AzureServices'
      defaultAction: 'Allow'
    }
    encryption: {
      services: {
        file: {
          keyType: 'Account'
          enabled: true
        }
        blob: {
          keyType: 'Account'
          enabled: true
        }
      }
      keySource: 'Microsoft.Storage'
    }

    accessTier: 'Hot'
  }
}

// OUTPUTS
output module_metadata object = module_metadata

output saName string = r_sa.name
output saPrimaryEndpointsBlob string = r_sa.properties.primaryEndpoints.blob
output saPrimaryEndpoints object = r_sa.properties.primaryEndpoints

output saName_1 string = r_sa_1.name
