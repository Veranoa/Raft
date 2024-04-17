import json

from send_request import send_request

def find_leader(hosts):
    """
    Iterates through the hosts to find the leader of the cluster.
    """
    for host in hosts:
        try:
            response = send_request('GET', host, 'status', timeout=1)
            if response['role'] == 'Leader':
                return host
        except Exception as e:
            print(f'Node {host} failed with {e}.')
    raise Exception('No leader found!')


def create_topic(host, topic):
    """
    Creates a topic on the specified host.
    """
    message = {'topic': topic}
    response = send_request('PUT', host, 'topic', message, timeout=1)
    print(response)
    return


def get_topics(host):
    """
    Retrieves the list of topics from the specified host.
    """
    response = send_request('GET', host, 'topic', timeout=1)
    print(response)
    return


def put_message(host, topic, message):
    """
    Puts a message into a specified topic on the host.
    """
    message = {'topic': topic, 'message': message}
    response = send_request('PUT', host, 'message', message, timeout=1)
    print(response)
    return


def get_message(host, topic):
    """
    Retrieves a message from a specified topic on the host.
    """
    response = send_request('GET', host, 'message/' + topic, timeout=1)
    print(response)
    return


if __name__ == '__main__':
    with open('../config/config.json', 'r') as config_file:
        server_config = json.load(config_file)['addresses']
        hosts = [f"{server['ip'].removeprefix('http://')}:{server['port']}" for server in server_config]

    print('hosts:', hosts)
    leader = find_leader(hosts)
    print('leader:', leader)

    topics = ['topic1', 'topic2', 'topic3']
    for topic in topics:
        create_topic(leader, topic)
    
    get_topics(leader)

    messages = [('topic9', 'msg1'), ('topic1', 'msg2'), ('topic2', 'msg3'), ('topic3', 'msg4'), ('topic1', 'msg5')]
    for topic, msg in messages:
        put_message(leader, topic, msg)

    for topic in topics:
        get_message(leader, topic)
