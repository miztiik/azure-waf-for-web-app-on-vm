// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2023-05-27'
  owner: 'miztiik@github'
}

param deploymentParams object
param tags object

param mysqlDbParams object

param dbSubnet01Id string

param logAnalyticsPayGWorkspaceId string

// Create MySQLDB Account

var mysql_db_name = replace('${mysqlDbParams.mysqlServerNamePrefix}-${deploymentParams.enterprise_name_suffix}-${deploymentParams.loc_short_code}-db-${deploymentParams.global_uniqueness}', '_', '-')

resource r_mysql_db_server 'Microsoft.DBforMySQL/servers@2017-12-01' = {
  name: mysql_db_name
  location: deploymentParams.location
  tags: tags
  sku: {
    name: mysqlDbParams.skuName
    tier: mysqlDbParams.skuTier
    capacity: mysqlDbParams.skuCapacity
    size: '${mysqlDbParams.skuSizeMB}' //a string is expected here but a int for the storageProfile...
    family: mysqlDbParams.skuFamily
  }
  properties: {
    createMode: 'Default'
    version: mysqlDbParams.mysqlVersion
    administratorLogin: mysqlDbParams.dbAdminLogin
    administratorLoginPassword: mysqlDbParams.dbAdminPass
    storageProfile: {
      storageMB: mysqlDbParams.skuSizeMB
      backupRetentionDays: 7
      geoRedundantBackup: 'Disabled'
    }
  }
  

  resource r_vnet_rule 'virtualNetworkRules@2017-12-01' = {
    name: '${mysql_db_name}-allow-vnet'
    properties: {
      virtualNetworkSubnetId: dbSubnet01Id
      ignoreMissingVnetServiceEndpoint: true
    }
  }
}

var db_fw_rules = [
  {
    Name: 'rule1'
    StartIpAddress: '0.0.0.0'
    EndIpAddress: '255.255.255.255'
  }
  {
    Name: 'rule2'
    StartIpAddress: '0.0.0.0'
    EndIpAddress: '255.255.255.255'
  }
]

@batchSize(1)
resource r_fw_rules 'Microsoft.DBforMySQL/servers/firewallRules@2017-12-01' = [for rule in db_fw_rules: {
  parent: r_mysql_db_server
  name: '${rule.Name}'
  properties: {
    startIpAddress: rule.StartIpAddress
    endIpAddress: rule.EndIpAddress
  }
}]

// Allow Access Azure Services
resource r_fw_rules_azureServicesFirewallRule 'Microsoft.DBforMySQL/servers/firewallRules@2017-12-01' = {
  parent: r_mysql_db_server
  name: 'AllowAllWindowsAzureIps'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}


// Create Diagnostic Settings for MySQL DB
resource mySqlDiagSetting 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'mySqlDiagSetting'
  scope: r_mysql_db_server
  properties: {
    workspaceId: logAnalyticsPayGWorkspaceId
    logs: [
      {
        category: 'MySqlSlowLogs'
        enabled: true
        retentionPolicy: {
          days: 0
          enabled: false
        }
      }
      {
        category: 'MySqlAuditLogs'
        enabled: true
        retentionPolicy: {
          days: 0
          enabled: false
        }
      }
    ]
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
        retentionPolicy: {
          days: 0
          enabled: false
        }
      }
    ]
  }
}


// OUTPUTS
output module_metadata object = module_metadata
