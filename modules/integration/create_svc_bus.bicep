// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-05-19'
  owner: 'miztiik@github'
}

param deploymentParams object
param serviceBusParams object
param tags object

var svc_bus_name = replace('${serviceBusParams.serviceBusNamePrefix}-${deploymentParams.loc_short_code}-svc-bus-ns-${deploymentParams.enterprise_name_suffix}-${deploymentParams.global_uniqueness}', '_', '-')

resource r_svc_bus_ns 'Microsoft.ServiceBus/namespaces@2022-01-01-preview' = {
  name: svc_bus_name
  location: deploymentParams.location
  tags: tags
  sku: {
    name: 'Standard'
    //name: 'Premium'
  }
  properties: {
    
  }
}


resource r_svc_bus_q 'Microsoft.ServiceBus/namespaces/queues@2022-01-01-preview' = {
  parent: r_svc_bus_ns
  name: '${serviceBusParams.serviceBusNamePrefix}-q-${deploymentParams.global_uniqueness}'
  properties: {
    lockDuration: 'PT5M'
    maxSizeInMegabytes: 1024
    requiresDuplicateDetection: false
    requiresSession: false
    // defaultMessageTimeToLive: 'P10675199DT2H48M5.4775807S'
    deadLetteringOnMessageExpiration: false
    duplicateDetectionHistoryTimeWindow: 'PT10M'
    maxDeliveryCount: 5
    // autoDeleteOnIdle: 'P10675199DT2H48M5.4775807S'
    enablePartitioning: false
    enableExpress: false
  }
}

// OUTPUTS
output module_metadata object = module_metadata

output svc_bus_ns_name string = r_svc_bus_ns.name
output svc_bus_q_name string = r_svc_bus_q.name
