// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2023-05-19'
  owner: 'miztiik@github'
}

param deploymentParams object
param serviceBusParams object

param svc_bus_ns_name string
param svc_bus_topic_name string

// Get Service Bus Namespace Reference
resource r_svc_bus_ns_ref 'Microsoft.ServiceBus/namespaces@2022-10-01-preview' existing = {
  name: svc_bus_ns_name
}

// Get Service Bus Topic Reference
resource r_svc_bus_topic_ref 'Microsoft.ServiceBus/namespaces/topics@2022-10-01-preview' existing = {
  parent: r_svc_bus_ns_ref
  name: svc_bus_topic_name
}

// ALL EVENT SUBSCRIBER
param all_events_subscriber string = 'all-events'
resource r_topic_subscriber_all_events 'Microsoft.ServiceBus/namespaces/topics/subscriptions@2022-10-01-preview' = {
  parent: r_svc_bus_topic_ref
  name: '${serviceBusParams.serviceBusNamePrefix}-${all_events_subscriber}-sub-${deploymentParams.global_uniqueness}'
  properties: {
    lockDuration: 'PT30S'
    defaultMessageTimeToLive: 'P7D'
    enableBatchedOperations: false
    maxDeliveryCount: 10
    autoDeleteOnIdle: 'P10D'
    // forwardTo: null
  }
}

resource r_topic_subscriber_all_events_filter_rule_1 'Microsoft.ServiceBus/namespaces/topics/subscriptions/rules@2022-10-01-preview' = {
  parent: r_topic_subscriber_all_events
  name: '${serviceBusParams.serviceBusNamePrefix}-${all_events_subscriber}-rule-${deploymentParams.global_uniqueness}'
  properties: {
    filterType: 'SqlFilter'
    sqlFilter: {
      sqlExpression: '1=1'
    }
  }
}

// INVENTORY EVENT SUBSCRIBER
param inventory_events_subscriber string = 'inventory-events'
resource r_topic_subscriber_inventory_events 'Microsoft.ServiceBus/namespaces/topics/subscriptions@2022-10-01-preview' = {
  parent: r_svc_bus_topic_ref
  name: '${serviceBusParams.serviceBusNamePrefix}-${inventory_events_subscriber}-sub-${deploymentParams.global_uniqueness}'
  properties: {
    lockDuration: 'PT30S'
    defaultMessageTimeToLive: 'P7D'
    enableBatchedOperations: false
    maxDeliveryCount: 10
    autoDeleteOnIdle: 'P10D'
    // forwardTo: null
  }
}

resource r_topic_subscriber_inventory_events_filter_rule_1 'Microsoft.ServiceBus/namespaces/topics/subscriptions/rules@2022-10-01-preview' = {
  parent: r_topic_subscriber_inventory_events
  name: '${serviceBusParams.serviceBusNamePrefix}-${inventory_events_subscriber}-rule-${deploymentParams.global_uniqueness}'
  properties: {
    filterType: 'CorrelationFilter'
    correlationFilter: {
      properties: {
        event_type: 'inventory_event'
      }
    }
  }
}

// SALE EVENT SUBSCRIBER
param sales_events_subscriber string = 'sale-events'
resource r_topic_subscriber_sales_events 'Microsoft.ServiceBus/namespaces/topics/subscriptions@2022-10-01-preview' = {
  parent: r_svc_bus_topic_ref
  name: '${serviceBusParams.serviceBusNamePrefix}-${sales_events_subscriber}-sub-${deploymentParams.global_uniqueness}'
  properties: {
    lockDuration: 'PT30S'
    defaultMessageTimeToLive: 'P7D'
    enableBatchedOperations: false
    maxDeliveryCount: 10
    autoDeleteOnIdle: 'P10D'
    // forwardTo: null
  }
}

resource r_topic_subscriber_sales_events_filter_rule_1 'Microsoft.ServiceBus/namespaces/topics/subscriptions/rules@2022-10-01-preview' = {
  parent: r_topic_subscriber_sales_events
  name: '${serviceBusParams.serviceBusNamePrefix}-${sales_events_subscriber}-rule-${deploymentParams.global_uniqueness}'
  properties: {
    filterType: 'CorrelationFilter'
    correlationFilter: {
      properties: {
        event_type: 'sale_event'
      }
    }
  }
}


// FRAUD DETECTION SUBSCRIBER
param fraud_detection_subscriber string = 'fraud-detection'
resource r_topic_subscriber_fraud_detection 'Microsoft.ServiceBus/namespaces/topics/subscriptions@2022-10-01-preview' = {
  parent: r_svc_bus_topic_ref
  name: '${serviceBusParams.serviceBusNamePrefix}-${fraud_detection_subscriber}-sub-${deploymentParams.global_uniqueness}'
  properties: {
    lockDuration: 'PT30S'
    defaultMessageTimeToLive: 'P7D'
    enableBatchedOperations: false
    maxDeliveryCount: 10
    autoDeleteOnIdle: 'P10D'
    // forwardTo: null
  }
}

resource r_topic_subscriber_fraud_detection_filter_rule_1 'Microsoft.ServiceBus/namespaces/topics/subscriptions/rules@2022-10-01-preview' = {
  parent: r_topic_subscriber_fraud_detection
  name: '${serviceBusParams.serviceBusNamePrefix}-${sales_events_subscriber}-fraud-detector-rule-${deploymentParams.global_uniqueness}'
  properties: {
    filterType: 'SqlFilter'
    sqlFilter: {
      sqlExpression: 'event_type = \'sale_event\' AND discount > 10'
    }
  }
}


// OUTPUTS
output module_metadata object = module_metadata

output all_events_subscriber string = r_topic_subscriber_all_events.name
output inventory_events_subscriber_name string = r_topic_subscriber_inventory_events.name
output sales_events_subscriber_name string = r_topic_subscriber_sales_events.name

