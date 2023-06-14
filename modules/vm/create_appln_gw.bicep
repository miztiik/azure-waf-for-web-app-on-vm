// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-05-19'
  owner: 'miztiik@github'
}

param deploymentParams object
param applnGwParams object
param tags object

param logAnalyticsPayGWorkspaceId string


param vnetName string

var appln_gw_front_end_name = 'm-front-end'
var appln_gw_back_end_pool_name = 'm-back-end-pool'


// Get VNet Reference
resource r_vnet 'Microsoft.Network/virtualNetworks@2021-02-01' existing = {
  name: vnetName
}


var appln_gw_name = replace('${applnGwParams.applnGwNamePrefix}-${deploymentParams.loc_short_code}-appln-gw-${deploymentParams.enterprise_name_suffix}-${deploymentParams.global_uniqueness}', '_', '-')

resource r_appln_gw_public_ip 'Microsoft.Network/publicIPAddresses@2021-08-01' = {
  name: '${appln_gw_name}-pip'
  location: deploymentParams.location
  sku: {
    name: 'Standard'
  }
  properties: {
    publicIPAddressVersion: 'IPv4'
    publicIPAllocationMethod: 'Static'
  }
}

resource r_appln_gw 'Microsoft.Network/applicationGateways@2021-05-01' = {
  name: appln_gw_name
  location: deploymentParams.location
  tags: tags
  properties: {
    sku: {
      name: 'WAF_v2'
      tier: 'WAF_v2'
    }
    // webApplicationFirewallConfiguration: {
    //   enabled: true
    //   firewallMode: 'Prevention'
    //   ruleSetType: 'OWASP'
    //   ruleSetVersion: '3.2'
    // }
    gatewayIPConfigurations: [
      {
        name: 'appGatewayIpConfig'
        properties: {
          subnet: {
            id: resourceId('Microsoft.Network/virtualNetworks/subnets', r_vnet.name, applnGwParams.appGWSubnetName)
          }
        }
      }
    ]
    frontendIPConfigurations: [
      {
        name: appln_gw_front_end_name
        properties: {
          privateIPAllocationMethod: 'Dynamic'
          publicIPAddress: {
            id: r_appln_gw_public_ip.id
          }
        }
      }
    ]
    frontendPorts: [
      {
        name: 'port_80'
        properties: {
          port: 80
        }
      }
    ]
    backendAddressPools: [
      {
        name: appln_gw_back_end_pool_name
        properties: {}
      }
    ]
    backendHttpSettingsCollection: [
      {
        name: 'HTTPSetting'
        properties: {
          port: 80
          protocol: 'Http'
          cookieBasedAffinity: 'Disabled'
          pickHostNameFromBackendAddress: false
          requestTimeout: 20
        }
      }
    ]
    httpListeners: [
      {
        name: 'listener-http'
        properties: {
          frontendIPConfiguration: {
            id: resourceId('Microsoft.Network/applicationGateways/frontendIPConfigurations', appln_gw_name, appln_gw_front_end_name)
          }
          frontendPort: {
            id: resourceId('Microsoft.Network/applicationGateways/frontendPorts', appln_gw_name, 'port_80')
          }
          protocol: 'Http'
          requireServerNameIndication: false
        }
      }
    ]
    requestRoutingRules: [
      {
        name: 'm-RoutingRule'
        properties: {
          ruleType: 'Basic'
          httpListener: {
            id: resourceId('Microsoft.Network/applicationGateways/httpListeners', appln_gw_name, 'listener-http')
          }
          backendAddressPool: {
            id: resourceId('Microsoft.Network/applicationGateways/backendAddressPools', appln_gw_name, appln_gw_back_end_pool_name)
          }
          backendHttpSettings: {
            id: resourceId('Microsoft.Network/applicationGateways/backendHttpSettingsCollection', appln_gw_name, 'HTTPSetting')
          }
        }
      }
    ]
    probes: [
      {
        name: 'probe_80'
        properties: {
          protocol: 'Http'
          path: '/'
          interval: 30
          timeout: 30
          unhealthyThreshold: 3
          pickHostNameFromBackendHttpSettings: true
          minServers: 0
          match: {}
        }
      }
    ]
    enableHttp2: false
    autoscaleConfiguration: {
      minCapacity: 1
      maxCapacity: 10
    }
    firewallPolicy: {
      id: r_appln_gw_fw_policy.id
    }
  }
  dependsOn: [
    r_vnet
  ]
}

resource r_appln_gw_fw_policy 'Microsoft.Network/ApplicationGatewayWebApplicationFirewallPolicies@2021-08-01' = {
  name: '${appln_gw_name}-fw-policy'
  location: deploymentParams.location
  properties: {
    policySettings: {
      requestBodyCheck: true
      maxRequestBodySizeInKb: 128
      fileUploadLimitInMb: 100
      state: 'Enabled'
      mode: 'Detection'
    }
    customRules: [
      {
        name: 'miztRule19'
        priority: 5
        ruleType: 'MatchRule'
        action: 'Allow'
        matchConditions: [
          {
            matchVariables: [
              {
                variableName: 'RemoteAddr'
              }
            ]
            operator: 'IPMatch'
            negationConditon: true
            matchValues: [
              '10.10.10.0/24'
            ]
          }
        ]
      }
    ]

    managedRules: {
      managedRuleSets: [
        {
          ruleSetType: 'OWASP'
          ruleSetVersion: '3.1'
        }
      ]
    }
  }
}

// Load Balancer Diagnostic Settings
resource r_appln_gw_diag 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: '${appln_gw_name}_diag'
  scope: r_appln_gw
  properties: {
    workspaceId: logAnalyticsPayGWorkspaceId
    logs: [
      {
        category: 'ApplicationGatewayAccessLog'
        enabled: true
        retentionPolicy: {
          days: 90
          enabled: true
        }
      }
      {
        category: 'ApplicationGatewayPerformanceLog'
        enabled: true
        retentionPolicy: {
          days: 90
          enabled: true
        }
      }
      {
        category: 'ApplicationGatewayFirewallLog'
        enabled: true
        retentionPolicy: {
          days: 90
          enabled: true
        }
      }
    ]
    metrics: [
      {
        category: 'AllMetrics'
        timeGrain: 'PT5M'
        enabled: true
        retentionPolicy: {
          enabled: false
          days: 0
        }
      }
    ]
  }
}


// OUTPUTS
output module_metadata object = module_metadata

output appln_gw_name string = r_appln_gw.name

output appln_gw_front_end_name string = r_appln_gw.properties.frontendIPConfigurations[0].name

output appln_gw_back_end_pool_name string = r_appln_gw.properties.backendAddressPools[0].name
