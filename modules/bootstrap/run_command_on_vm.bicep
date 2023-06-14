// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-05-19'
  owner: 'miztiik@github'
}

param vmNames array
param deploymentParams object
param repoName string
param tags object
param vmParams object

var no_of_vms = vmParams.vmCount

resource r_vms 'Microsoft.Compute/virtualMachines@2022-03-01' existing = [for (vmName, i) in vmNames: {
  name: vmName
}]

var command_to_clone_repo_with_vars = '''
REPO_NAME="REPO_NAME_VAR" && \
GIT_REPO_URL="https://github.com/miztiik/$REPO_NAME.git" && \
cd /var && \
rm -rf /var/$REPO_NAME && \
git clone $GIT_REPO_URL && \
cd /var/$REPO_NAME && \
chmod +x /var/$REPO_NAME/modules/vm/bootstrap_scripts/deploy_app.sh && \
bash /var/$REPO_NAME/modules/vm/bootstrap_scripts/deploy_app.sh &
'''

var command_to_clone_repo = replace(command_to_clone_repo_with_vars, 'REPO_NAME_VAR', repoName)

// Associate Automation Events DCR to VM
resource r_deploy_script_1 'Microsoft.Compute/virtualMachines/runCommands@2022-03-01' = [for i in range(0, no_of_vms): {
  parent: r_vms[i]
  name:   '${deploymentParams.enterprise_name_suffix}_${deploymentParams.global_uniqueness}_script_1'
  location: deploymentParams.location
  tags: tags
  properties: {
    asyncExecution: false
    source: {
        script: command_to_clone_repo
      }
  }
}]



/*
var script_to_execute_with_vars = '''
REPO_NAME="REPO_NAME_VAR" && \
export APP_CONFIG_NAME="APP_CONFIG_VAR_NAME" && \
python3 /var/$REPO_NAME/app/az_producer_for_cosmos_db.py &
'''

var script_to_execute = replace(replace(script_to_execute_with_vars, 'APP_CONFIG_VAR_NAME', appConfigName),'REPO_NAME_VAR', repoName)

resource r_deploy_script_2 'Microsoft.Compute/virtualMachines/runCommands@2022-03-01' = if (deploy_app_script) {
  parent: r_vm_1
    name:   '${deploymentParams.enterprise_name_suffix}_${deploymentParams.global_uniqueness}_script_2'
  location: deploymentParams.location
  tags: tags
  properties: {
    asyncExecution: true
    runAsUser: 'root'
    parameters: [
      {
        name: 'EVENTS_TO_PRODUCE'
        value: '1'
      }
    ]
    source: {
        script: script_to_execute
      }
      timeoutInSeconds: 600
  }
  dependsOn: [
    r_deploy_script_1
  ]
}
*/


// Troublshooting
/*
script_location = '/var/lib/waagent/run-command-handler/download/VM_NAME_script_deployment/0/script.sh'
output_location = '/var/lib/waagent/run-command-handler/download/m-web-srv-004_004_script_deployment/0'
*/



// OUTPUTS
output module_metadata object = module_metadata
