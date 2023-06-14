// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-06-05'
  owner: 'miztiik@github'
}

param deploymentParams object
param identityParams object
param tags object

var _prebaked_uami_name_prefix = '${identityParams.namePrefix}_${deploymentParams.enterprise_name_suffix}_${deploymentParams.global_uniqueness}'

@description('Create User-Assigned Managed Identity - For VMs')
resource r_uami_vm 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${_prebaked_uami_name_prefix}_vm'
  location: deploymentParams.location
  tags: tags
}

@description('Create User-Assigned Managed Identity - For Functions')
resource r_uami_func 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${_prebaked_uami_name_prefix}_func'
  location: deploymentParams.location
  tags: tags
}

// @description('Create User-Assigned Managed Identity - For Stream Analytics')
// resource r_uami_stream_analytics 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
//   name: '${_prebaked_uami_name_prefix}_stream_analytics'
//   location: deploymentParams.location
//   tags: tags
// }

// @description('Create User-Assigned Managed Identity - For Data Factory')
// resource r_uami_data_factory 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
//   name: '${_prebaked_uami_name_prefix}_data_factory'
//   location: deploymentParams.location
//   tags: tags
// }


// @description('Create User-Assigned Managed Identity - For AKS')
// resource r_uami_aks 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
//   name: '${_prebaked_uami_name_prefix}_aks'
//   location: deploymentParams.location
//   tags: tags
// }


// @description('Create User-Assigned Managed Identity - For Logic App')
// resource r_uami_logic_app 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
//   name: '${_prebaked_uami_name_prefix}_logic_app'
//   location: deploymentParams.location
//   tags: tags
// }


// Output
output module_metadata object = module_metadata

output uami_name_vm string = r_uami_vm.name
output uami_name_func string = r_uami_func.name
// output uami_name_stream_analytics string = r_uami_stream_analytics.name
// output uami_data_factory string = r_uami_data_factory.name
// output uami_name_aks string = r_uami_aks.name
// output uami_name_logic_app string = r_uami_logic_app.name
