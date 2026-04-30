from abc import ABC, abstractmethod
from multiprocessing import Process, Value, Array
import zmq
import json
import math
import os

 
class MapReduce(ABC):
 
    #constructer
    def __init__(self, num_worker):
        self.num_worker = num_worker
    
    #Partial Result
    @abstractmethod
    def Map(self, map_input):
        pass

    #Result
    @abstractmethod
    def Reduce(self, reduce_input):
        pass

    #private
    def _Producer(self, producer_input):
        context = zmq.Context()

        #divide data into almost equal-sized pieces
        piece_size = len(producer_input) // self.num_worker
        excess = len(producer_input) % self.num_worker

        start = 0
        data_pieces = []
        for worker_index in range(self.num_worker):
            end = start + piece_size + (1 if worker_index < excess else 0)
            data_pieces.append(producer_input[start:end])
            start = end

        i = 0
        for piece in data_pieces:
            work_message = {'data_piece' : piece}

            producer_socket = context.socket(zmq.PUSH)
            port = "tcp://127.0.0.1:51" + str(i).zfill(2)
            producer_socket.bind(port)
            producer_socket.send_json(work_message)

            producer_socket.close()
            i = i + 1
        
        '''
        for i in range(data_pieces):
            work_message = {'data_piece' : data_pieces[i]}

            producer_socket = context.socket(zmq.PUSH)
            port = "tcp://127.0.0.1:1" + str(i).zfill(3)
            producer_socket.bind(port)
            producer_socket.send_json(work_message)
            producer_socket.close()
        '''

    #private
    def _Consumer(self, consumer_input):

        context = zmq.Context()
        push_context = zmq.Context()
        PID = os.getpid()
        print ('Consumer PID:', PID)

        port = "tcp://127.0.0.1:51" + str(consumer_input).zfill(2)
        consumer_pull_socket = context.socket(zmq.PULL)
        consumer_pull_socket.connect(port)

        work_message = consumer_pull_socket.recv_json()
        data_piece = work_message["data_piece"]

        port = "tcp://127.0.0.1:52" + str(consumer_input).zfill(2)
        consumer_push_socket = push_context.socket(zmq.PUSH)
        consumer_push_socket.bind(port)

        partial_result = self.Map(data_piece)

        print(f"Map {PID} Input: {data_piece}")
        print(f"Map {PID} Result: {partial_result}")

        push_message = {'partial_result' : partial_result}
        consumer_push_socket.send_json(push_message)
        
        consumer_pull_socket.close()
        consumer_push_socket.close()

    #private
    def _ResultCollector(self, num_work):

        context = zmq.Context()
        PID = os.getpid()
        print ('ResultCollector PID:', PID) 

        partial_results = []

        for i in range(num_work):
            port = "tcp://127.0.0.1:52" + str(i).zfill(2) 
            result_socket = context.socket(zmq.PULL)
            result_socket.connect(port)
            work_message = result_socket.recv_json()

            partial_results.append({int(key): value for key, value in work_message["partial_result"].items()})
            
            result_socket.close()
            
        result = self.Reduce(partial_results)

        print(f"Reducer data: {partial_results}")
        print(f"Results: {result}")

        with open('results.txt', 'w') as file:
            json.dump(result, file)


    def start(self, filename):

        with open(filename, 'r') as file:
            input_data = [list(map(int, line.strip().split('\t'))) for line in file]
        
        print(f"Initial Data:{input_data}")

        producer = Process(target=self._Producer, args=(input_data,))
        Consumers = [Process(target=self._Consumer, args=(i,)) for i in range(self.num_worker)]
        collector = Process(target=self._ResultCollector, args=(self.num_worker,))

        producer.start()
        _ = [consumer.start() for consumer in Consumers]
        collector.start()

        producer.join()
        _ = [consumer.join() for consumer in Consumers]
        collector.join()
   

        
    
