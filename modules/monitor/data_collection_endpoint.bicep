// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-05-19'
  owner: 'miztiik@github'
}
param deploymentParams object
param dceParams object
param tags object
param osKind string


resource r_lin_dce 'Microsoft.Insights/dataCollectionEndpoints@2021-04-01' = {
  name: '${dceParams.endpointNamePrefix}-${deploymentParams.loc_short_code}-Dce-${deploymentParams.global_uniqueness}'
  location: deploymentParams.location
  tags: tags
  kind: osKind
  properties: {
    networkAcls: {
      publicNetworkAccess: 'Enabled'
    }
  }
}

// OUTPUTS
output module_metadata object = module_metadata

output linDataCollectionEndpointId string = r_lin_dce.id
output linDataCollectionEndpointName string = r_lin_dce.name
