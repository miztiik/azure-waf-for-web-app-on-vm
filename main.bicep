// targetScope = 'subscription'

targetScope = 'resourceGroup'

// Parameters
param deploymentParams object
param identityParams object

param storageAccountParams object
param logAnalyticsWorkspaceParams object


param vnetParams object
param vmParams object

param applnGwParams object

param funcParams object
param cosmosDbParams object

param dceParams object
param brandTags object

param dateNow string = utcNow('yyyy-MM-dd-hh-mm')

param tags object = union(brandTags, {last_deployed:dateNow})


// Create Identity
module r_uami 'modules/identity/create_uami.bicep' = {
  name: '${deploymentParams.enterprise_name_suffix}_${deploymentParams.loc_short_code}_${deploymentParams.global_uniqueness}_uami'
  params: {
    deploymentParams:deploymentParams
    identityParams:identityParams
    tags: tags
  }
}

// Create Cosmos DB
module r_cosmosdb 'modules/database/cosmos.bicep' ={
  name: '${cosmosDbParams.cosmosDbNamePrefix}_${deploymentParams.loc_short_code}_${deploymentParams.global_uniqueness}_cosmos_db'
  params: {
    deploymentParams:deploymentParams
    cosmosDbParams:cosmosDbParams
    tags: tags
  }
}

// Create the Log Analytics Workspace
module r_logAnalyticsWorkspace 'modules/monitor/log_analytics_workspace.bicep' = {
  name: '${logAnalyticsWorkspaceParams.workspaceName}_${deploymentParams.loc_short_code}_${deploymentParams.global_uniqueness}_la'
  params: {
    deploymentParams:deploymentParams
    logAnalyticsWorkspaceParams: logAnalyticsWorkspaceParams
    tags: tags
  }
}


// Create Storage Account
module r_sa 'modules/storage/create_storage_account.bicep' = {
  name: '${storageAccountParams.storageAccountNamePrefix}_${deploymentParams.loc_short_code}_${deploymentParams.global_uniqueness}_sa'
  params: {
    deploymentParams:deploymentParams
    storageAccountParams:storageAccountParams
    funcParams: funcParams
    tags: tags
  }
}


// Create Storage Account - Blob container
module r_blob 'modules/storage/create_blob.bicep' = {
  name: '${storageAccountParams.storageAccountNamePrefix}_${deploymentParams.loc_short_code}_${deploymentParams.global_uniqueness}_blob'
  params: {
    deploymentParams:deploymentParams
    storageAccountParams:storageAccountParams
    storageAccountName: r_sa.outputs.saName
    storageAccountName_1: r_sa.outputs.saName_1
    logAnalyticsWorkspaceId: r_logAnalyticsWorkspace.outputs.logAnalyticsPayGWorkspaceId
    enableDiagnostics: false
  }
  dependsOn: [
    r_sa
    r_logAnalyticsWorkspace
  ]
}


// Create Data Collection Endpoint
module r_dataCollectionEndpoint 'modules/monitor/data_collection_endpoint.bicep' = {
  name: '${dceParams.endpointNamePrefix}_${deploymentParams.global_uniqueness}_Dce'
  params: {
    deploymentParams:deploymentParams
    dceParams: dceParams
    osKind: 'linux'
    tags: tags
  }
}



// Create the Data Collection Rule
module r_dataCollectionRule 'modules/monitor/data_collection_rule.bicep' = {
  name: '${logAnalyticsWorkspaceParams.workspaceName}_${deploymentParams.global_uniqueness}_Dcr'
  params: {
    deploymentParams:deploymentParams
    osKind: 'Linux'
    tags: tags

    storeEventsRuleName: 'storeEvents_Dcr'
    storeEventsLogFilePattern: '/var/log/miztiik*.json'
    storeEventscustomTableNamePrefix: r_logAnalyticsWorkspace.outputs.storeEventsCustomTableNamePrefix

    automationEventsRuleName: 'miztiikAutomation_Dcr'
    automationEventsLogFilePattern: '/var/log/miztiik-automation-*.log'
    automationEventsCustomTableNamePrefix: r_logAnalyticsWorkspace.outputs.automationEventsCustomTableNamePrefix
    
    managedRunCmdRuleName: 'miztiikManagedRunCmd_Dcr'
    managedRunCmdLogFilePattern: '/var/log/azure/run-command-handler/*.log'
    managedRunCmdCustomTableNamePrefix: r_logAnalyticsWorkspace.outputs.managedRunCmdCustomTableNamePrefix

    linDataCollectionEndpointId: r_dataCollectionEndpoint.outputs.linDataCollectionEndpointId
    logAnalyticsPayGWorkspaceName:r_logAnalyticsWorkspace.outputs.logAnalyticsPayGWorkspaceName
    logAnalyticsPayGWorkspaceId:r_logAnalyticsWorkspace.outputs.logAnalyticsPayGWorkspaceId

  }
  dependsOn: [
    r_logAnalyticsWorkspace
  ]
}


// Create the VNets
module r_vnet 'modules/vnet/create_vnet.bicep' = {
  name: '${vnetParams.vnetNamePrefix}_${deploymentParams.global_uniqueness}_Vnet'
  params: {
    deploymentParams:deploymentParams
    vnetParams:vnetParams
    tags: tags
  }
}

// Create Virtual Machine
module r_vm 'modules/vm/create_vm.bicep' = {
  name: '${vmParams.vmNamePrefix}_${deploymentParams.global_uniqueness}_Vm'
  params: {
    deploymentParams:deploymentParams
    uami_name_vm: r_uami.outputs.uami_name_vm

    saName: r_sa.outputs.saName
    blobContainerName: r_blob.outputs.blobContainerName
    saPrimaryEndpointsBlob: r_sa.outputs.saPrimaryEndpointsBlob

    vmParams: vmParams
    vnetName: r_vnet.outputs.vnetName

    logAnalyticsPayGWorkspaceId:r_logAnalyticsWorkspace.outputs.logAnalyticsPayGWorkspaceId

    linDataCollectionEndpointId: r_dataCollectionEndpoint.outputs.linDataCollectionEndpointId
    storeEventsDcrId: r_dataCollectionRule.outputs.storeEventsDcrId
    automationEventsDcrId: r_dataCollectionRule.outputs.automationEventsDcrId

    add_to_appln_gw: true
    appln_gw_name: r_application_gateway.outputs.appln_gw_name
    appln_gw_back_end_pool_name: r_application_gateway.outputs.appln_gw_back_end_pool_name

    tags: tags
  }
  dependsOn: [
    r_vnet
  ]
}

// Deploy Script on VM
module r_deploy_managed_run_cmd 'modules/bootstrap/run_command_on_vm.bicep'= {
  name: '${vmParams.vmNamePrefix}_${deploymentParams.global_uniqueness}_run_cmd'
  params: {
    deploymentParams:deploymentParams
    vmParams: vmParams
    vmNames: r_vm.outputs.vmNames
    repoName: brandTags.project

    tags: tags
  }
  dependsOn: [
    r_vm
  ]
}

// Create Application Gateway
module r_application_gateway 'modules/vm/create_appln_gw.bicep' = {
  name: '${applnGwParams.applnGwNamePrefix}_${deploymentParams.global_uniqueness}_ApplnGw'
  params: {
    deploymentParams:deploymentParams
    applnGwParams:applnGwParams
    tags: tags

    vnetName: r_vnet.outputs.vnetName

    logAnalyticsPayGWorkspaceId:r_logAnalyticsWorkspace.outputs.logAnalyticsPayGWorkspaceId
  }
  dependsOn: [
    r_vnet
  ]
}

// Attach VM to Application Gateway
module r_attach_vm_to_app_gw_pools 'modules/vm/attach_vm_to_app_gw_pools.bicep' = {
  name: '${vmParams.vmNamePrefix}_${deploymentParams.global_uniqueness}_attach_vm_to_app_gw_pools'
  params: {
    deploymentParams:deploymentParams
    appln_gw_name: r_application_gateway.outputs.appln_gw_name
    vmNames: r_vm.outputs.vmNames
    tags: tags
  }
  dependsOn: [
    r_vm
    r_application_gateway
  ]
}
