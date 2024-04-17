import random
import threading
import time

from helpers import NodeLog, Role

def calculate_election_timeout():
    return random.uniform(500, 1000)  

class Node:
    def __init__(self, index, other_nodes):
        self.__thread_lock = threading.Lock()
        self.__index = index  
        self.__other_nodes = other_nodes
        
        self.__timeout = calculate_election_timeout()
        self.__leader = None 
        self.__voted_for = None 
        self.__term = -1  
        self.__role = Role.FOLLOWER
        
        self.__committed_index = -1
        self.__last_applied = -1
        self.__last_heartbeat = time.time() * 1000

        self.__logs = NodeLog()

        self.__next_index = {}
        self.__match_index = {}

    def increment_term(self):
        self.__term += 1
        return

    def set_new_role(self, role):
        self.__role = role
        return

    def is_leader(self):
        """Checks if the node is the leader."""
        return self.__role == Role.LEADER

    def prepare_for_leadership(self):
        """Prepares the node for leadership when it becomes the leader."""
        # Initializes next_index and match_index for all other nodes
        self.__match_index = {server: -1 for server in self.__other_nodes}
        self.__next_index = {server: self.__logs.log_size() for server in self.__other_nodes}

    def check_heartbeat_timeout(self):
        """Checks if the heartbeat timeout has elapsed indicating a potential leader failure."""
        if self.is_leader(): 
            return False
        cur_time = time.time() * 1000
        time_elapsed = cur_time - self.__last_heartbeat
        if time_elapsed >= self.__timeout:
            print(f'Heartbeat Timeout in {time_elapsed}.')
            self.reset_last_heartbeat()
            return True
        return False

    def reset_last_heartbeat(self):
        """Resets the last heartbeat time to the current time and recalculates the election timeout."""
        self.__last_heartbeat = time.time() * 1000
        self.__timeout = calculate_election_timeout() 
        

    @property
    def leader(self):
        return self.__leader

    @property
    def logs(self):
        return self.__logs

    @property
    def next_index(self):
        return self.__next_index

    @property
    def match_index(self):
        return self.__match_index

    @property
    def committed_index(self):
        return self.__committed_index

    @property
    def last_applied(self):
        return self.__last_applied

    @property
    def voted_for(self):
        return self.__voted_for

    @property
    def index(self):
        return self.__index

    @property
    def term(self):
        return self.__term

    @property
    def role(self):
        return self.__role

    @property
    def total_nodes(self):
        return len(self.__other_nodes) + 1

    @property
    def other_nodes(self):
        return self.__other_nodes

    @property
    def thread_lock(self):
        return self.__thread_lock
    
    
    @leader.setter
    def leader(self, leader):
        self.__leader = leader
        return

    @term.setter
    def term(self, term):
        self.__term = term
        return

    @voted_for.setter
    def voted_for(self, voted_for):
        self.__voted_for = voted_for
        return

    @committed_index.setter
    def committed_index(self, committed_index):
        self.__committed_index = committed_index
        return
    
    @last_applied.setter
    def last_applied(self, val):
        self.__last_applied = val
        return

if __name__ == '__main__':
    node = Node()
    while True:
        print(node.check_heartbeat_timeout())
        time.sleep(1)
