# Fill the class given below for the first part of the assignment
# You can add new functions if necessary
# but don't change the signatures of the ones included

import threading
import random

class ConSet:
    def __init__(self):
        self.list = {}
        self.s = threading.Semaphore(0)
        self.m = threading.Semaphore(1)

    def insert(self, newItem):
        self.m.acquire()
        
        self.list[newItem] = True

        self.m.release()
        self.s.release()

    def pop(self):
        self.s.acquire()
        self.m.acquire()

        elements = [key for key, value in self.list.items() if value]

        random_elt = random.choice(elements)

        self.list[random_elt] = False

        self.m.release()
        return random_elt

    def printSet(self):
        with self.m:
            print(self.list)
