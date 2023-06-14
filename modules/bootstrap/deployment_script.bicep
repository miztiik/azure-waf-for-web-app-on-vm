// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-05-19'
  owner: 'miztiik@github'
}
param deploymentParams object
param tags object

resource r_deploy_scripts_1 'Microsoft.Resources/deploymentScripts@2020-10-01' = {
  name: 'vm-bootstrapper-${deploymentParams.global_uniqueness}'
  location: deploymentParams.location
  kind: 'AzureCLI'
  tags: tags
  properties: {
    azCliVersion: '2.37.0'
    timeout: 'PT2H'
    retentionInterval: 'P1D'
    cleanupPreference: 'OnSuccess'
    // forceUpdateTag: now()
    scriptContent: loadTextContent('../vm/bootstrap_scripts/deploy_app.sh')
  }
}



// resource secrets 'Microsoft.Resources/deploymentScripts@2020-10-01' = {
//   name: 'secrets'
//   location: location
//   kind: 'AzureCLI'
//   properties: {
//     azCliVersion: '2.37.0'
//     timeout: 'PT2H'
//     retentionInterval: 'P1D'
//     cleanupPreference: 'OnSuccess'
//     scriptContent: '''
//       #/bin/bash -e
      
//       ssh-keygen -f "key"  -N ""

//       cat <<EOF >$AZ_SCRIPTS_OUTPUT_PATH
//       {
//         "adminSshPublicKey": "$(cat key.pub)",
//         "adminSshPrivateKey": "$(cat key)",
//         "adminPassword": "$(openssl rand -base64 24)",
//         "databaseAdminPassword": "$(openssl rand -base64 24)"
//       }
//       EOF

//     '''
//   }
// }

// output secrets object = {
//   adminSshPublicKey: reference('secrets').outputs.adminSshPublicKey
//   adminSshPrivateKey: reference('secrets').outputs.adminSshPrivateKey
//   adminPassword: reference('secrets').outputs.adminPassword
//   databaseAdminPassword: reference('secrets').outputs.databaseAdminPassword
// }

// OUTPUTS
output module_metadata object = module_metadata
