// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-05-19'
  owner: 'miztiik@github'
}

param deploymentParams object
param appln_gw_name string
param tags object

param vmNames array



resource deploymentScript 'Microsoft.Resources/deploymentScripts@2020-10-01' = {
  name: 'attachVMsToAppGatewayBackendPool'
  location: deploymentParams.location
  tags: tags
  kind: 'AzureCLI'
  properties: {
    azCliVersion: '2.37.0'
    timeout: 'PT30M'
    cleanupPreference: 'OnSuccess'
    retentionInterval: 'P1D'
    // forceUpdateTag: now()
    arguments: '-vmNames "${string(vmNames)}" -appln_gw_name "${appln_gw_name}" -resourceGroupName "${resourceGroup().name}"'
    scriptContent: '''
      #!/bin/bash

      while getopts "vmNames:appln_gw_name:resourceGroupName:" opt; do
        case $opt in
          vmNames) vmNames=$OPTARG;;
          appln_gw_name) appln_gw_name=$OPTARG;;
          resourceGroupName) resourceGroupName=$OPTARG;;
        esac
      done

      appGw=$(az network application-gateway show --name $appln_gw_name --resource-group $resourceGroupName --query "id" -o tsv)
      appGwPoolId=$(az network application-gateway address-pool show --gateway-name $appln_gw_name --name "appGatewayBackendPool" --resource-group $resourceGroupName --query "id" -o tsv)

      IFS=', ' read -r -a vmNamesArray <<< "$vmNames"
      for vmName in "${vmNamesArray[@]}"; do
        nicId=$(az vm show --name $vmName --resource-group $resourceGroupName --query "networkProfile.networkInterfaces[0].id" -o tsv)
        az network nic ip-config update --name "ipconfig1" --nic-name $nicId --application-gateway-backend-address-pools $appGwPoolId
      done
    '''

  }
}



// OUTPUTS
output module_metadata object = module_metadata


