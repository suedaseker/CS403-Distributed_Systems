from multiprocessing import Process, Barrier
import os
import time
import zmq
import numpy as np
import re
import sys
import random
import json

# sending a message without crashing
def sendMessage(body, from_id, to_node_id, push_socket):

    msg = {"body": body, "from": from_id, "to": to_node_id}
    push_socket.send_json(msg)

# broadcasting a message without crashing
def broadcastMessage(body, from_id, numProc, push_socket_d, exclude_self = False):
   
    for to_node_id in range(numProc):

        push_socket = push_socket_d[to_node_id]
        if exclude_self and to_node_id == from_id:
            continue

        sendMessage(body, from_id, to_node_id, push_socket)


# sending a message with crash probability prob
def sendFailure(body, from_id, to_node_id, prob, push_socket):
    
    is_crashed = np.random.choice([True, False], p = [prob, 1-prob])

    msg = {}
    if is_crashed:
        msg = {"body": f"CRASH {from_id}", "from": from_id, "to": to_node_id}
    
    else:
        msg = {"body": body, "from": from_id, "to": to_node_id}
    push_socket.send_json(msg)

# broadcasting a message with crash probability prob
def broadcastFailure(body, sender_id, numProc, prob, push_socket_d):
   
    for to_node_id in range(numProc):
        push_socket = push_socket_d[to_node_id]
        sendFailure(body, sender_id, to_node_id, prob, push_socket)


def PaxosNode(id, prob, numProc, val, numRounds, barrier):
    
    maxVotedRound = -1  # maximum round number this node has voted for
    maxVotedVal = None  # the corresponding value
    
    proposeVal = None  # the value proposed by this node in the latest round in which it is the proposer
    decision = None  # if majority, the decision becomes the proposeVal in this round

    context = zmq.Context()
    pull_socket = context.socket(zmq.PULL)
    pull_socket.bind(f"tcp://127.0.0.1:{5550 + id}")

    push_socket_d = {}
    for to_node_id in range(numProc):
        push_socket = context.socket(zmq.PUSH)
        push_socket.connect(f"tcp://127.0.0.1:{5550 + to_node_id}")
        
        push_socket_d[to_node_id] = push_socket

    time.sleep(0.2)

    # start the algorithm
    for round in range(numRounds):
        proposer = (round % numProc == id)

        if proposer:
            # broadcasts a START message
            print(f"ROUND {round} STARTED WITH INITIAL VALUE: {val}")
            time.sleep(0.2)
            broadcastFailure(body = "START", sender_id = id, numProc = numProc,
                prob = prob, push_socket_d = push_socket_d)
        
        # receiving and parsing incoming messages form proposer
        incoming_msg = pull_socket.recv_json()
        incoming_msg_body = incoming_msg["body"]
        incoming_msg_from = incoming_msg["from"]
        time.sleep(0.2)

        # Phase 1 ---
        count_join = 0
        do_propose = False

        if proposer:

            print(f"LEADER OF {id} RECEIVED IN JOIN PHASE: {incoming_msg_body}")

            # check if proposer crashed or not (received START)
            proposer_started = False
            if "START" in incoming_msg_body:
                proposer_started = True
                count_join += 1
            
            # receive N-1 responses from acceptors
            acceptor_maxVotedRound = -1
            acceptor_maxVotedVal = -1
            for _ in range(numProc-1):
                incoming_msg = pull_socket.recv_json()
                incoming_msg_body = incoming_msg["body"]
                incoming_msg_from = incoming_msg["from"]

                print(f"LEADER OF {id} RECEIVED IN JOIN PHASE: {incoming_msg_body}")

                if "JOIN" in incoming_msg_body:
                    count_join += 1
                    # JOIN {maxVotedRound} {maxVotedVal}
                    join_msg = incoming_msg_body.split(" ")
                    if int(join_msg[1]) > acceptor_maxVotedRound:
                        acceptor_maxVotedVal = int(join_msg[2])
                        acceptor_maxVotedRound = int(join_msg[1])
            
            # if majority of nodes has joined
            print(f"count join is: {count_join}")
            if count_join > int(numProc/2):
                do_propose = True

                # update proposeVal
                if proposer_started:
                    if maxVotedRound == -1:
                        proposeVal = val
                    else:
                        proposeVal = acceptor_maxVotedVal           
                else:
                    proposeVal = val
            # if majority is not reached
            else:
                do_propose = False

        # the node is not proposer
        elif not proposer:
            print(f"ACCEPTOR {id} RECEIVED IN JOIN PHASE: {incoming_msg_body}")
            time.sleep(0.2)

            if "START" in incoming_msg_body:
                # send JOIN message to the leader (proposer)
                sendFailure(body = f"JOIN {maxVotedRound} {maxVotedVal}", from_id=id,
                            to_node_id = incoming_msg_from, prob = prob,
                            push_socket = push_socket_d[incoming_msg_from])
        
            elif "CRASH" in incoming_msg_body:
                # if acceptor receives a CRASH message, it also sends a CRASH message
                sendMessage(body = f"CRASH {id}", from_id = id,
                            to_node_id = incoming_msg_from,
                            push_socket = push_socket_d[incoming_msg_from])
        barrier.wait()

        # Phase 2 ---
        if proposer:
            time.sleep(0.2)

            if do_propose:
                # then broadcast PROPOSE
                broadcastFailure(body = f"PROPOSE {proposeVal}", sender_id = id,
                                 numProc = numProc, prob = prob, 
                                 push_socket_d = push_socket_d)
            else:
                # then broadcast ROUNDCHANGE
                print(f"LEADER OF ROUND {round} CHANGED ROUND")
                broadcastMessage(body = "ROUNDCHANGE", from_id = id, 
                                 numProc = numProc, push_socket_d = push_socket_d,)
                # move to the next round
        

        # receiving and parsing incoming messages PROPOSE, CRASH or ROUNDCHANGE from the leader
        incoming_msg = pull_socket.recv_json()
        incoming_msg_body = incoming_msg["body"]
        incoming_msg_from = incoming_msg["from"]

        if proposer:

            # if proposer is proposing, then it will get N messages
            if do_propose:
                did_get_propose = False
                count_vote = 0

                if "ROUNDCHANGE" not in incoming_msg_body:
                    print(f"LEADER OF {id} RECEIVED IN VOTE PHASE: {incoming_msg_body}")

                    if "PROPOSE" in incoming_msg_body:
                        did_get_propose = True
                        count_vote += 1
                       
                    elif "CRASH" in incoming_msg_body:
                        pass
                
                    # get messages from acceptors JOIN or CRASH
                    for _ in range(numProc - 1):
                        incoming_msg = pull_socket.recv_json()
                        incoming_msg_body = incoming_msg["body"]
                        incoming_msg_from = incoming_msg["from"]

                        print(f"LEADER OF {id} RECEIVED IN VOTE PHASE: {incoming_msg_body}")

                        if "VOTE" in incoming_msg_body:
                            count_vote += 1
                    
                    if did_get_propose:
                            maxVotedRound = round
                            maxVotedVal = proposeVal

                    if count_vote > int(numProc/2):
                        decision = proposeVal
                        print(f"LEADER OF {id} DECIDED ON VALUE: {decision}")

            pass

        else:
            time.sleep(0.2)

            print(f"ACCEPTOR {id} RECEIVED IN VOTE PHASE: {incoming_msg_body}")

            if "CRASH" in incoming_msg_body:
                sendMessage(body = f"CRASH {id}", from_id = id,
                            to_node_id = incoming_msg_from,
                            push_socket = push_socket_d[incoming_msg_from])
            
            elif "PROPOSE" in incoming_msg_body:
                sendFailure(body = "VOTE", from_id = id, to_node_id = incoming_msg_from,
                            prob = prob, push_socket = push_socket_d[incoming_msg_from])
                
                maxVotedRound = round
                maxValue = int(incoming_msg_body.split(" ")[1])
                maxVotedVal = maxValue
            
            elif "ROUNDCHANGE" in incoming_msg_body:
                pass
        
            pass

        barrier.wait()
        time.sleep(0.2)
        # go to the next round
    pass

def main(args):
    numProc = int(args[1])
    prob = float(args[2])
    numRounds = int(args[3])

    barrier = Barrier(numProc)
    
    processes = []
    for id in range(numProc):
        val = random.randint(0, 1)
        p = Process(
            target = PaxosNode,
            args = (id, prob, numProc, val, numRounds, barrier))
        
        processes.append(p)
    
    for p in processes:
        p.start()

    for p in processes:
        p.join()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Invalid arguments")
    else:
        main(args=sys.argv)