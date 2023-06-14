// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2023-05-27'
  owner: 'miztiik@github'
}

param deploymentParams object
param tags object

param mysqlFlexDbParams object

param r_usr_mgd_identity_name string

param create_replica bool = false
param replica_count int = 1

param private_access bool = false
param vnetName string

param flex_db_subnet_cidr string = '10.0.6.0/24'

param logAnalyticsPayGWorkspaceId string

// Get VNet Reference
resource r_vnet 'Microsoft.Network/virtualNetworks@2021-02-01' existing= {
  name: vnetName
}

// Reference existing User-Assigned Identity
resource r_userManagedIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: r_usr_mgd_identity_name
}

resource r_flex_db_subnet 'Microsoft.Network/virtualNetworks/subnets@2022-07-01' = {
  parent: r_vnet
  name: 'flex_db_subnet'
  properties: {
    addressPrefix: flex_db_subnet_cidr
    delegations: [
      {
        name: 'MySQLflexibleServers'
        properties: {
          serviceName: 'Microsoft.DBforMySQL/flexibleServers'
        }
      }
    ]
  }
}



// Create Flexible MySQL DB Account

var mysql_flex_db_name = replace('${mysqlFlexDbParams.mysqlServerNamePrefix}-${deploymentParams.enterprise_name_suffix}-${deploymentParams.loc_short_code}-db-${deploymentParams.global_uniqueness}', '_', '-')

resource r_mysql_flex_db_server 'Microsoft.DBforMySQL/flexibleServers@2021-12-01-preview' = {
  name: mysql_flex_db_name
  location: deploymentParams.location
  tags: tags
  sku: {
    name: 'Standard_D2ads_v5'
    tier: mysqlFlexDbParams.serverEdition
  }
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${r_userManagedIdentity.id}': {}
    }
  }
  properties: {
    version: mysqlFlexDbParams.mysqlVersion
    administratorLogin: mysqlFlexDbParams.dbAdminLogin
    administratorLoginPassword: mysqlFlexDbParams.dbAdminPass
    // availabilityZone: '2'
    // availabilityZone: deploymentParams.location
    highAvailability: {
      // mode: 'SameZone'
      // mode: 'ZoneRedundant'
      // standbyAvailabilityZone: '3'
    }
    storage: {
      storageSizeGB: mysqlFlexDbParams.storageSizeGB
      iops: mysqlFlexDbParams.storageIops
      autoGrow: 'Enabled'
    }
    network: private_access ? {
      privateDnsZoneResourceId: r_pvt_dns_for_flex_mysql_db.id
      delegatedSubnetResourceId: r_flex_db_subnet.id
    } : null
    backup: {
      backupRetentionDays: 7
      geoRedundantBackup: 'Disabled'
    }
  }
  dependsOn:  [
    privateDnsZoneMySqlVnetlink
  ]
}

resource r_db 'Microsoft.DBforMySQL/flexibleServers/databases@2021-12-01-preview' = {
  parent: r_mysql_flex_db_server
  name: mysqlFlexDbParams.dbName
  properties: {
    charset: 'utf8'
    collation: 'utf8_general_ci'
  }
}

// Allow Access Azure Services
// resource r_fw_rules_azureServicesFirewallRule 'Microsoft.DBforMySQL/servers/firewallRules@2017-12-01' = {
//   parent: r_mysql_flex_db_server
//   name: 'AllowAllWindowsAzureIps'
//   properties: {
//     startIpAddress: '0.0.0.0'
//     endIpAddress: '0.0.0.0'
//   }
// }


// Create Diagnostic Settings for MySQL DB
resource mySqlDiagSetting 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'mySqlDiagSetting'
  scope: r_mysql_flex_db_server
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


resource r_fw_rules_azureServicesFirewallRule 'Microsoft.DBforMySQL/flexibleServers/firewallRules@2021-12-01-preview' = {
  parent: r_mysql_flex_db_server
  name: 'AllowAzureIPs'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '255.255.255.255'
  }
}


resource r_fw_rule_AllowAnyHost 'Microsoft.DBforMySQL/flexibleServers/firewallRules@2021-05-01' = {
  name: 'allow_any_Host'
  parent: r_mysql_flex_db_server
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

// /!\ SECURITY Risk: Allow ANY HOST for local Dev/Test only

// Allow public access from any Azure service within Azure to this server
// This option configures the firewall to allow connections from IP addresses allocated to any Azure service or asset,
// including connections from the subscriptions of other customers.

//  resource fwRuleAllowAnyHost 'Microsoft.DBforMySQL/flexibleServers/firewallRules@2021-05-01' = {
//   name: 'Allow Any Host'
//   parent: mysqlserver
//   properties: {
//     startIpAddress: '0.0.0.0'
//     endIpAddress: '255.255.255.255'
//   }
// }




var mysql_pvt_dns_zone_name = 'pvt.mysql.database.azure.com'
@description('Private DNS Zone for Database')
resource r_pvt_dns_for_flex_mysql_db 'Microsoft.Network/privateDnsZones@2020-06-01' = if (private_access){
  name: mysql_pvt_dns_zone_name
  location: 'global'
}

@description('Link DNS Zone to VNet')
resource privateDnsZoneMySqlVnetlink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = if (private_access){
  parent: r_pvt_dns_for_flex_mysql_db
  name: '${r_vnet.name}-dns-link'
  location: 'global'
  properties: {
    virtualNetwork: {
      id: r_vnet.id
    }
    registrationEnabled: true
  }
}















// Create MySQL DB Replica
var mysql_flex_read_replica_db_name = replace('${mysqlFlexDbParams.mysqlServerNamePrefix}-read-replica-${deploymentParams.global_uniqueness}-', '_', '-')

resource r_mysql_flex_read_replica_db_server 'Microsoft.DBforMySQL/flexibleServers@2021-12-01-preview' =  [for i in range(0, replica_count): if (create_replica) {
  name: '${mysql_flex_read_replica_db_name}${i}'
  location: deploymentParams.location
  tags: tags
  sku: {
    name: 'Standard_D2ads_v5'
    tier: mysqlFlexDbParams.serverEdition
  }
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${r_userManagedIdentity.id}': {}
    }
  }
  properties: {
    sourceServerResourceId: r_mysql_flex_db_server.id
    createMode: 'Replica'
    replicationRole: 'Replica'
    storage: {
      storageSizeGB: mysqlFlexDbParams.storageSizeGB
      iops: mysqlFlexDbParams.storageIops
      autoGrow: 'Enabled'
    }
    backup: {
      backupRetentionDays: 7
      geoRedundantBackup: 'Disabled'
    }
  }
}]

resource r_fw_rules_azureServicesFirewallRule_to_replica 'Microsoft.DBforMySQL/flexibleServers/firewallRules@2021-12-01-preview' = [for i in range(0, replica_count): if (create_replica){
  parent: r_mysql_flex_read_replica_db_server[i]
  name: 'AllowAzureIPs'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '255.255.255.255'
  }
}]


resource r_fw_rule_AllowAnyHost_to_replica 'Microsoft.DBforMySQL/flexibleServers/firewallRules@2021-05-01' =  [for i in range(0, replica_count): if (create_replica) {
  name: 'allow_any_host_to_replica'
  parent: r_mysql_flex_read_replica_db_server[i]
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}]

// OUTPUTS
output module_metadata object = module_metadata
// output mysqlHostname string = '${serverName}.${dnszone.name}'
// output fqdn string = mySQL.properties.fullyQualifiedDomainName
// output fullyQualifiedDomainName string = mySQLServer.properties.fullyQualifiedDomainName
// output dbName string = dbName
// output serverName string = mySQLServerName
// output id string = mySQL.id
