import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from helpers import LogEntry, Operation, Command
from state_machine import StateMachine
from send_request import send_request
from raft import Role

class RaftServer:
    def __init__(self, node, topic_queues, results):
        self.node = node
        self.topic_queues = topic_queues
        self.results = results
        self.scheduler_interval = 0.01
        self.background_thread = threading.Thread(target=self.run_background_tasks, daemon=True)
        self.background_thread.start()
    
    def run_background_tasks(self):
        """Continuously run background tasks for leader, follower, and log application."""
        while True:
            self.perform_leader_tasks()
            self.perform_follower_tasks()
            self.apply_state_machine()
            time.sleep(self.scheduler_interval)

    def perform_leader_tasks(self):
        """Perform tasks specific to the leader role."""
        if self.node.is_leader():
            self.apped_entries_to_followers()
            self.update_committed_index()

    def perform_follower_tasks(self):
        """Check for leader timeouts and initiate leader election if necessary."""
        if self.node.check_heartbeat_timeout():
            self.initiate_leader_election()

    def apply_state_machine(self):
        """Apply committed log entries to the state machine."""
        StateMachine(self.topic_queues, self.results, self.node).apply_state_machine()
    
    def apped_entries_to_followers(self):
        """Send append entry requests to all follower nodes to replicate the leader's log entries."""
        print(f'Sending append messages to the follower nodes.')
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {}
            for other_server in self.node.other_nodes:
                data = self.construct_entry_data(other_server)
                future = executor.submit(send_request, 'POST', other_server, 'logs/append', data, timeout=0.1)
                futures[future] = other_server
                
            for future in as_completed(futures):
                try:
                    response = future.result()
                    server = futures[future]
                    self.process_append_response(server, response)
                except Exception as e:
                    print(f'Sending heartbeats to {other_server} failed: {e}.')
        return

    def construct_entry_data(self, server):
        """Constructs the data payload for an append entry request."""
        data = {
            'term': self.node.term,
            'leaderId': self.node.index,
            'prevLogTerm': -1,
            'prevLogIndex': -1,
            'entry': None,
            'leaderCommit': self.node.committed_index
        }
        current_index = self.node.next_index[server]
        if current_index > 0:
            prev_index = current_index - 1
            data['prevLogTerm'] = self.node.logs.entries[prev_index].term
            data['prevLogIndex'] = prev_index
        if current_index < self.node.logs.log_size:
            data['entry'] = self.node.logs.entries[current_index].json_encode()
        return data
    
    def process_append_response(self, server, response):
        """Processes the response from an append entry request."""
        curr_index = self.node.next_index[server]
        if response['term'] > self.node.term: 
            self.update_term_return_to_follower(response['term'])
            return
        if response['success']:
            self.node.match_index[server] = curr_index - 1
            if curr_index < self.node.logs.log_size:
                self.node.next_index[server] += 1
        else:
            self.node.next_index[server] -= 1

    def update_committed_index(self):
        """Update the commit index if a majority of followers have stored a newer entry."""
        if self.node.logs.log_size - 1 == self.node.committed_index:
            return
        value = self.node.committed_index + 1
        while value < self.node.logs.log_size:
            count = 1
            for val in self.node.match_index.values():
                if val >= value:
                    count += 1
            if count <= (self.node.total_nodes / 2):
                return
            if self.node.logs.entries[value].term == self.node.term:
                self.node.committed_index = value
            value += 1
        return
    
    def update_term_return_to_follower(self, term):
        """Update the current term and revert to follower role."""
        self.node.reset_last_heartbeat()
        self.node.term = term
        if self.node.role != Role.FOLLOWER:
            self.node.set_new_role(Role.FOLLOWER)
            print('Reverting to follower state due to higher term.')
            
    def initiate_leader_election(self):
        """Initiate a new leader election process, transitioning the node to a candidate role."""
        print(f'Initiating leader election.')
        self.node.set_new_role(Role.CANDIDATE)
        self.node.increment_term()
        self.node.voted_for = None

        votes_received = 1  
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {}
            data = {
                'term': self.node.term,
                'candidateId': self.node.index,
                'lastLogTerm': -1 if not self.node.logs.entries else self.node.logs.entries[-1].term,
                'lastLogIndex': self.node.logs.log_size - 1
            }
            for other_server in self.node.other_nodes:
                future = executor.submit(
                    send_request, 'POST', other_server, 'election/vote', data, timeout=0.01)
                futures[future] = other_server

            while futures and not self.node.check_heartbeat_timeout():
                retries = {}
                for future in as_completed(futures):
                    try:
                        response = future.result()
                        if response['vote']:
                            votes_received += 1
                        elif response['term'] > self.node.term:
                            self.update_term_return_to_follower(response['term'])
                            return
                    except Exception as e:
                        print(f'Vote to {other_server} failed with: {e}.')
                        retry = executor.submit(send_request, 'POST', futures[future], 'election/vote', data)
                        retries[retry] = other_server
                    print(f'Received Votes from {votes_received}/{self.node.total_nodes}.')
                    if votes_received > self.node.total_nodes / 2 and self.node.role == Role.CANDIDATE:
                        self.node.set_new_role(Role.LEADER)
                        for other_server in self.node.other_nodes:
                            self.node.next_index[other_server] = self.node.logs.log_size
                            self.node.match_index[other_server] = -1
                        return
                futures = retries

        if votes_received > self.node.total_nodes / 2 and self.node.role == Role.CANDIDATE:
            self.node.set_new_role(Role.LEADER)
            for other_server in self.node.other_nodes:
                self.node.next_index[other_server] = self.node.logs.log_size
                self.node.match_index[other_server] = -1
        else:
            self.node.set_new_role(Role.FOLLOWER)
        return
    
    def append_log_and_wait(self, command):
        """Appends a command to the log and waits for it to be applied."""
        log_entry = LogEntry(self.node.term, command)
        self.node.logs.append(log_entry)
        log_index = self.node.logs.log_size - 1
        while self.node.last_applied < log_index:
            time.sleep(0.01)
        return self.results[command.id]
        