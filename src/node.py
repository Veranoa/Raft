import argparse
import json
import uuid

from flask import Flask
from flask import Response
from flask import request

import raft
from helpers import LogEntry, Operation, Command

from raft_server import RaftServer

app = Flask(__name__)

def get_uuid():
    """Generate and return a unique identifier."""
    return str(uuid.uuid4())

# Endpoint to create a new topic
@app.route('/topic', methods=['PUT'])
def put_topic():
    body = json.loads(request.get_data().decode('utf-8'))
    topic = body['topic']
    id = get_uuid()
    response = raftServer.append_log_and_wait(Command(id, Operation.PUT_TOPIC, topic))
    return response

# Endpoint to retrieve list of topics
@app.route('/topic', methods=['GET'])
def get_topics():
    id = get_uuid()
    response = raftServer.append_log_and_wait(Command(id, Operation.GET_TOPICS, ''))
    return response

# Endpoint to post a message to a topic
@app.route('/message', methods=['PUT'])
def put_message():
    message_data = request.json
    topic = message_data['topic']
    message = message_data['message']
    id = get_uuid()
    response = raftServer.append_log_and_wait(Command(id, Operation.PUT_MESSAGE, json.dumps({'topic': topic, 'message': message})))
    return response

# Endpoint to retrieve a message from a topic
@app.route('/message/<topic>', methods=['GET'])
def get_message(topic):
    id = get_uuid()
    response = raftServer.append_log_and_wait(Command(id, Operation.GET_MESSAGE, topic))
    return response

# Endpoint to get the current status of the node (leader, follower)
@app.route('/status', methods=['GET'])
def get_status():
    return {'role': node.role.value, 'term': node.term}

# Endpoint for a leader to sync logs with followers
@app.route('/logs/append', methods=['POST'])
def sync_logs():
    message = json.loads(request.get_data().decode('utf-8'))
    term = message['term']
    leader_id = message['leaderId']
    prev_log_term = message['prevLogTerm']
    prev_log_index = message['prevLogIndex']
    entry = LogEntry.json_decode(message['entry'])
    leader_commit = message['leaderCommit']
    output = {'term': node.term}
    if term < node.term:
        output['success'] = False
        return output
    node.reset_last_heartbeat()
    node.leader = leader_id
    raftServer.update_term_return_to_follower(term)
    if prev_log_index != -1 and (
            node.logs.log_size <= prev_log_index or node.logs.entries[prev_log_index].term != prev_log_term):
        output['success'] = False
        return output
    if node.logs.log_size > prev_log_index + 1 and node.logs.entries[prev_log_index + 1] != entry:
        node.logs.delete_entries_from(prev_log_index + 1)
    if entry:
        node.logs.append(entry)
    if leader_commit > node.committed_index:
        node.committed_index = min(leader_commit, node.logs.log_size - 1)
    output['success'] = True
    return output

# Endpoint for nodes to request votes during leader election
@app.route('/election/vote', methods=['POST'])
def vote_leader():
    message = json.loads(request.get_data().decode('utf-8'))
    requester_term = message['term']
    candidate_id = message['candidateId']
    last_log_idx = message['lastLogIndex']
    last_log_term = message['lastLogTerm']

    if node.logs.entries and (node.logs.entries[-1].term > last_log_term or (
            node.logs.entries[-1].term == last_log_term and node.logs.log_size > last_log_idx + 1)):
        output = {'vote': False, 'term': node.term}
        return Response(json.dumps(output), status=200, mimetype='application/json')

    if node.term == requester_term and node.is_leader():
        output = {'vote': False, 'term': node.term}
        return Response(json.dumps(output), status=200, mimetype='application/json')

    if requester_term > node.term:
        raftServer.update_term_return_to_follower(requester_term)
        node.voted_for = candidate_id
        output = {'vote': True, 'term': node.term}
        return Response(json.dumps(output), status=200, mimetype='application/json')
    
    elif requester_term == node.term and (not node.voted_for or node.voted_for == candidate_id):
        raftServer.update_term_return_to_follower(requester_term)
        node.voted_for = candidate_id
        output = {'vote': True, 'term': node.term}
        return Response(json.dumps(output), status=200, mimetype='application/json')

    output = {'vote': False, 'term': node.term}
    return Response(json.dumps(output), status=200, mimetype='application/json')


if __name__ == '__main__':
    global raftServer
    parser = argparse.ArgumentParser()
    parser.add_argument("path_to_config", type=str, help="path to server config file")
    parser.add_argument("index", type=int, help="index of the server to start")
    args = parser.parse_args()
    with open(args.path_to_config, 'r') as config_file:
        server_config = json.load(config_file)['addresses']
        nodes = [(server['ip'].removeprefix('http://'), server['port']) for server in server_config]
        other_nodes = [f'{node[0]}:{node[1]}' for idx, node in enumerate(nodes) if idx != args.index]
        
        node = raft.Node(args.index, other_nodes)
        topic_queues = dict()
        results = dict()
        
        raftServer = RaftServer(node, topic_queues, results)
        
        print(
            f'Starting server {args.index} on {nodes[args.index][0]}:{nodes[args.index][1]} with other nodes: '
            f'{other_nodes}.')

        app.run(host=nodes[args.index][0], port=nodes[args.index][1])
