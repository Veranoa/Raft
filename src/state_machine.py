import json
from helpers import Operation


class StateMachine:
    """
    Implements the logic of a state machine that processes commands based on the Raft consensus algorithm.
    """
    def __init__(self, topic_queues, results, node):
        self.node = node
        self.topic_queues = topic_queues
        self.results = results
        self.handlers = {
            Operation.PUT_TOPIC: PutTopicHandler,
            Operation.GET_TOPICS: GetTopicsHandler,
            Operation.PUT_MESSAGE: PutMessageHandler,
            Operation.GET_MESSAGE: GetMessageHandler
        }
        
    def apply_state_machine(self):
        """
        Applies the next log entry in the state machine if there's an unapplied committed entry.
        """
        # Lock to ensure thread-safe operations on the node's log and state.
        with self.node.thread_lock:
            if self.node.committed_index > self.node.last_applied:
                next_idx = self.node.last_applied + 1
                log_entry = self.node.logs.entries[next_idx]
                command = log_entry.command
                handler_class = self.handlers.get(command.operation)
                if handler_class:
                    handler = handler_class(self, command)
                    try:
                        handler.apply()
                    except Exception as e:
                        self.results[command.id] = {'error_stack': str(e)}
                        print(f'Error applying {command.operation}: {e}')
                else:
                    print(f'Unknown command: {command.operation}')
                self.node.last_applied += 1


class OperationHandler:
    """
    Base class for operation handlers. Each specific operation should subclass this and implement the apply method.
    """
    def __init__(self, state_machine, command):
        self.state_machine = state_machine
        self.command = command

    def apply(self):
        raise NotImplementedError("Must be implemented by subclass.")


class PutTopicHandler(OperationHandler):
    def apply(self):
        topic = self.command.message
        if topic in self.state_machine.topic_queues.keys():
            self.state_machine.results[self.command.id] = {'success': False}
        else:
            self.state_machine.topic_queues[topic] = []
            self.state_machine.results[self.command.id] = {'success': True}


class GetTopicsHandler(OperationHandler):
    def apply(self):
        self.state_machine.results[self.command.id] = {
            'success': True, 'topics': list(self.state_machine.topic_queues.keys())}


class PutMessageHandler(OperationHandler):
    def apply(self):
        data = json.loads(self.command.message)
        topic, message = data['topic'], data['message']
        if topic not in self.state_machine.topic_queues:
            self.state_machine.results[self.command.id] = {'success': False}
            return
        self.state_machine.topic_queues[topic].append(message)
        self.state_machine.results[self.command.id] = {'success': True}


class GetMessageHandler(OperationHandler):
    def apply(self):
        topic = self.command.message
        if topic not in self.state_machine.topic_queues or not self.state_machine.topic_queues[topic]:
            self.state_machine.results[self.command.id] = {'success': False}
            return
        message = self.state_machine.topic_queues[topic].pop(0)
        self.state_machine.results[self.command.id] = {'success': True, 'message': message}
