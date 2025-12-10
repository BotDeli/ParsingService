class Node:
    def __init__(self, data):
        self.data = data
        self.next = None

class LinkedQueue:
    def __init__(self):
        self.head = None
        self.trail = None

    def push(self, data):
        new_node = Node(data)

        if self.head is None:
            self.head = new_node
            self.trail = self.head
            return
        
        self.trail.next = new_node
        self.trail = self.trail.next

    def is_next(self) -> bool:
        return not self.head is None
    
    def pop(self) -> any:
        if self.head is None:
            return None

        data = self.head.data
        self.head = self.head.next
        return data
    