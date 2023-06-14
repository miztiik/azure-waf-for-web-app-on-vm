// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-05-19'
  owner: 'miztiik@github'
}

param deploymentParams object
param kvNamePrefix string
param tags object

param skuName string = 'standard'

resource r_kv 'Microsoft.KeyVault/vaults@2021-11-01-preview' = {
  name: '${kvNamePrefix}-${deploymentParams.loc_short_code}-kv-${deploymentParams.global_uniqueness}'
  location: deploymentParams.location
  tags: tags
  properties: {
    accessPolicies:[]
    enableRbacAuthorization: false
    enableSoftDelete: false
    enabledForDeployment: false
    enabledForDiskEncryption: false
    enabledForTemplateDeployment: false
    tenantId: subscription().tenantId
    sku: {
      name: skuName
      family: 'A'
    }
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
  }
}

// OUTPUTS
output module_metadata object = module_metadata

output kvName string = r_kv.name

