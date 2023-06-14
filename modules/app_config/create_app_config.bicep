// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-05-19'
  owner: 'miztiik@github'
}

param deploymentParams object
param appConfigParams object
param tags object

param addRandom string = toLower(substring(uniqueString(resourceGroup().id), 0, 12))

resource r_appConfig 'Microsoft.AppConfiguration/configurationStores@2023-03-01' = {
  name: '${appConfigParams.appConfigNamePrefix}-${deploymentParams.loc_short_code}-config-${addRandom}-${deploymentParams.global_uniqueness}'
  location: deploymentParams.location
  tags: tags
  sku: {
    name: appConfigParams.appConfigSku
  }
}


// OUTPUTS
output module_metadata object = module_metadata

output appConfigName string = r_appConfig.name
