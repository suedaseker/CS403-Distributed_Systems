import random
import threading
from ConSet import ConSet

n = 4
barrier = threading.Barrier(n)
#mailboxes = [ConSet() for _ in range(n)]
mailboxes = []

def nodeWork(node_id, n):
    
    round_num = 1
    
    while True:
        random_number = random.randint(0, n**2)
        message = (node_id, random_number)

        barrier.wait()

        print(f"Node {node_id} proposes value {random_number} for round {round_num}.")

        for i in range(n):
            mailboxes[i].insert(message)
        
        incoming_messages = [mailboxes[node_id].pop() for _ in range(n)]

        max_number, counter, max_id = 0, 0, 0
        for id, number in incoming_messages:
                if number > max_number:
                    max_number = number
                    counter = 1
                    max_id = id
                
                elif number == max_number:
                    counter += 1

        if counter == 1:
            print(f"Node {node_id} decided {max_id} as the leader.")
            break
        else:
            round_num += 1
            print(f"Node {node_id} could not decide on the leader and moves to round {round_num}.")


for i in range(n):
    mailboxes.append(ConSet())

threads = []  

for id in range(n):
    thread = threading.Thread(target=nodeWork, args=(id, n))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()