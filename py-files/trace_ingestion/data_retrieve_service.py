import json

from data_retrive import *
import json

def process_data(data):
    response_data = {
        "totalErrorCalls": 0,
        "totalSuccessCalls": 0,
        "serviceName": set() 
    }

    try:
        if isinstance(data, str):
            data = json.loads(data)
        
        for item in data:
            for resource_span in item.get("resourceSpans", []):
                for scope_span in resource_span.get("scopeSpans", []):
                    for span in scope_span.get("spans", []):
                        span_name = span.get("name", "")
                        
                        http_status_str = next(
                            (attr.get("value", {}).get("intValue") for attr in span.get("attributes", []) 
                             if attr.get("key") == "http.response.status_code"), 
                            None
                        )
                        
                       
                        http_status = int(http_status_str) if http_status_str is not None else None
                        
                        if http_status is not None:
                            if http_status >= 300:
                                response_data["totalErrorCalls"] += 1
                            else:
                                response_data["totalSuccessCalls"] += 1
                        
                      
                        service_name = next(
                            (attr.get("value", {}).get("stringValue") for attr in resource_span.get("resource", {}).get("attributes", [])
                             if attr.get("key") == "service.name"), 
                            None
                        )
                        if service_name:
                            response_data["serviceName"].add(service_name)
   
      
        response_data["serviceName"] = list(response_data["serviceName"])
        
    except Exception as e:
        print(f"Error processing data: {e}")
    
    return response_data

data2 = final_data(from_date_str, to_date_str)    
processed_data = process_data(data2)
print(json.dumps(processed_data, indent=2))
