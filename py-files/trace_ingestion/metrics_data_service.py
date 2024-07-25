from metric_getdata import *

def process_metrics_data(data):
    try:
        json_obj = []
        if isinstance(data, str):
            data = json.loads(data)
        
        # Iterate through the list of data
        for service_data in data:
            servicename = None
            created_time = None
            

    
            for resource_metric in service_data.get("resourceMetrics", []):
                if 'resource' in resource_metric and 'attributes' in resource_metric['resource']:
                    for attribute in resource_metric['resource']['attributes']:
                        if attribute['key'] == 'service.name':
                            if 'value' in attribute and 'stringValue' in attribute['value']:
                                servicename=attribute['value']['stringValue']
                                
            # Extract service name
            # for attribute in service_data.get("resource", {}).get("attributes", []):
            #     if attribute.get("key") == "service.name":
            #         service_name = attribute.get("value", {}).get("stringValue", "")
            
            # Extract metrics data
            for resource_metric in service_data.get("resourceMetrics", []):
                scope_metrics = resource_metric.get("scopeMetrics", [])
                for scope_metric in scope_metrics:
                    scope_name = scope_metric.get("scope", {}).get("name", "")

                    if scope_name.startswith("io.opentelemetry.runtime-telemetry"):
                        metrics = scope_metric.get("metrics", [])
                    
                        cpu_usage = 0.0
                        memory_usage = 0

                        for metric in metrics:
                            metric_name = metric.get("name", "")
                            if metric_name == "jvm.cpu.recent_utilization":
                                for dataPt in metric.get('gauge', {}).get('dataPoints', []):
                                    cpu_usage += float(dataPt.get('asDouble', 0))
                            if metric_name == "jvm.memory.used":
                                for dataPt in metric.get('sum', {}).get('dataPoints', []):
                                    memory_usage += int(dataPt.get('asInt', 0))
                        
                        
                        created_time = dataPt.get('startTimeUnixNano', 0)
                        
                        if created_time:
                            # Convert created_time from nanoseconds to a readable datetime format
                            created_time = datetime.fromtimestamp(int(created_time) / 1e9).isoformat()
                        else:
                            created_time = "N/A" 

                        # Create the final JSON object
                        metric_json = {
                            "serviceName": servicename,
                            "createdTime": created_time,
                            "cpuUsage": cpu_usage,
                            "memoryUsage": memory_usage
                        }
                        json_obj.append(metric_json)
        return json_obj
    except Exception as e:
        print(f"Error processing metrics data: {e}")
        return []

  # You can set this to None to get data without filtering by service name

# Retrieve and process data
data1 = final_data(from_date_str, to_date_str, service_name)
data2 = process_metrics_data(data1)


print(json.dumps(data2, indent=2))