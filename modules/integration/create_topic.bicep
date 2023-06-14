// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-05-21'
  owner: 'miztiik@github'
}

param deploymentParams object
param serviceBusParams object

param svc_bus_ns_name string


// Get Service Bus Namespace Reference
resource r_svc_bus_ns_ref 'Microsoft.ServiceBus/namespaces@2022-10-01-preview' existing = {
  name: svc_bus_ns_name
}


resource r_svc_bus_topic 'Microsoft.ServiceBus/namespaces/topics@2022-10-01-preview' = {
  name: '${serviceBusParams.serviceBusNamePrefix}-${deploymentParams.loc_short_code}-topic-${deploymentParams.global_uniqueness}'
  parent: r_svc_bus_ns_ref
  properties: {
    autoDeleteOnIdle: 'P10D'
    defaultMessageTimeToLive: 'P14D'
    duplicateDetectionHistoryTimeWindow: 'PT10M'
    enableBatchedOperations: false
    enableExpress: false
    enablePartitioning: false
    maxSizeInMegabytes: 1024
    requiresDuplicateDetection: false
    supportOrdering: false
    // forwardTo: 'string'
  }
}


// OUTPUTS
output module_metadata object = module_metadata

output svc_bus_topic_name string = r_svc_bus_topic.name
output svc_bus_topic_id string = r_svc_bus_topic.id
