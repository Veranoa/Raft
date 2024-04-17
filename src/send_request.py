import requests

def send_request(method, host, local_url, data=None, timeout=10):
    """
    Sends an HTTP request to the specified service URL on the hostname.
    """
    local_url = f'http://{host}/{local_url}'
    
    if method.upper() == 'GET':
        response = requests.get(local_url, timeout=timeout)
    elif method.upper() == 'POST':
        response = requests.post(local_url, json=data, timeout=timeout)
    elif method.upper() == 'PUT':
        response = requests.put(local_url, json=data, timeout=timeout)
    else:
        raise ValueError("Invalid method.")

    if response.status_code not in [200, 201]:
        raise Exception(f"Error: {response.status_code}, {response.text}")

    return response.json()

if __name__ == '__main__':
    try:
        print(send_request('PUT', 'localhost:4321', 'messageQueue/message', {}))
        print(send_request('GET', 'localhost:4321', 'messageQueue/message'))
        print(send_request('POST', 'localhost:4321', 'heartbeats/heartbeat', {}))
    except Exception as e:
        print(f"Request failed: {e}")

