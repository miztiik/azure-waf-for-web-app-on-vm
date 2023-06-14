// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-05-19'
  owner: 'miztiik@github'
}

param deploymentParams object
param lbParams object
param tags object

param logAnalyticsPayGWorkspaceId string

var lb_name = replace('${lbParams.lbNamePrefix}-${deploymentParams.loc_short_code}-lb-${deploymentParams.enterprise_name_suffix}-${deploymentParams.global_uniqueness}', '_', '-')
var lb_front_end_name = '${lbParams.lbNamePrefix}-lb-front-end'
var lb_front_end_outbound_name = '${lbParams.lbNamePrefix}-lb-front-end-outbound'
var lb_back_end_pool_name = '${lbParams.lbNamePrefix}-lb-back-end-pool'
var lb_back_end_pool_name_outbound = '${lbParams.lbNamePrefix}-lb-back-end-outbound'
var lb_probe_name = '${lbParams.lbNamePrefix}-lb-health-probe'

resource r_lb 'Microsoft.Network/loadBalancers@2021-08-01' = {
  name: lb_name
  location: deploymentParams.location
  tags: tags
  sku: {
    name: lbParams.lb_Sku
  }
  properties: {
    frontendIPConfigurations: [
      {
        name: lb_front_end_name
        properties: {
          publicIPAddress: {
            id: r_lb_public_ip.id
          }
        }
      }
      {
        name: lb_front_end_outbound_name
        properties: {
          publicIPAddress: {
            id: r_lb_public_ip_address_outbound.id
          }
        }
      }
    ]
    backendAddressPools: [
      {
        name: lb_back_end_pool_name
      }
      {
        name: lb_back_end_pool_name_outbound
      }
    ]
    loadBalancingRules: [
      {
        name: 'myHTTPRule'
        properties: {
          frontendIPConfiguration: {
            id: resourceId('Microsoft.Network/loadBalancers/frontendIPConfigurations', lb_name, lb_front_end_name)
          }
          backendAddressPool: {
            id: resourceId('Microsoft.Network/loadBalancers/backendAddressPools', lb_name, lb_back_end_pool_name)
          }
          frontendPort: 80
          backendPort: 80
          enableFloatingIP: false
          idleTimeoutInMinutes: 15
          protocol: 'Tcp'
          enableTcpReset: true
          loadDistribution: 'Default'
          disableOutboundSnat: true
          probe: {
            id: resourceId('Microsoft.Network/loadBalancers/probes', lb_name, lb_probe_name)
          }
        }
      }
    ]
    probes: [
      {
        name: lb_probe_name
        properties: {
          protocol: 'Tcp'
          port: 80
          intervalInSeconds: 5
          numberOfProbes: 2
        }
      }
    ]
    outboundRules: [
      {
        name: 'myOutboundRule'
        properties: {
          allocatedOutboundPorts: 10000
          protocol: 'All'
          enableTcpReset: false
          idleTimeoutInMinutes: 15
          backendAddressPool: {
            id: resourceId('Microsoft.Network/loadBalancers/backendAddressPools', lb_name, lb_back_end_pool_name)
          }
          frontendIPConfigurations: [
            {
              id: resourceId('Microsoft.Network/loadBalancers/frontendIPConfigurations', lb_name, lb_front_end_name)
            }
          ]
        }
      }
    ]
  }
}

resource r_lb_public_ip 'Microsoft.Network/publicIPAddresses@2021-08-01' = {
  name: '${lb_name}-pip'
  location: deploymentParams.location
  sku: {
    name: lbParams.lb_Sku
  }
  properties: {
    publicIPAddressVersion: 'IPv4'
    publicIPAllocationMethod: 'Static'
  }
}

resource r_lb_public_ip_address_outbound 'Microsoft.Network/publicIPAddresses@2021-08-01' = {
  name: '${lb_name}-pip-adress-name-outbound'
  location: deploymentParams.location
  sku: {
    name: lbParams.lb_Sku
  }
  properties: {
    publicIPAddressVersion: 'IPv4'
    publicIPAllocationMethod: 'Static'
  }
}

/*
// Load Balancer Diagnostic Settings
resource r_lb_diag 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: '${lb_name}_diag'
  scope: r_lb
  properties: {
    workspaceId: logAnalyticsPayGWorkspaceId
    logs: [
      {
        category: 'LoadBalancerAlertEvent'
        enabled: true
      }
      {
        category: 'LoadBalancerProbeHealthStatus'
        enabled: true
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
*/


// OUTPUTS
output module_metadata object = module_metadata

output lb_name string = lb_name
output lb_back_end_pool_name string = lb_back_end_pool_name
output lb_back_end_pool_name_outbound string = lb_back_end_pool_name_outbound
output lb_public_ip_address string = r_lb_public_ip.properties.ipAddress
