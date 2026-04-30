from FindCyclicReferences import FindCyclicReferences
from FindCitations import FindCitations
import sys

def main():
    #check if the arguments are correct
    if len(sys.argv) != 4:
        print("Wrong arguments. Correct format: python main.py COUNT/CYCLE [1-10] <filename>")
        sys.exit(1)

    op_name = sys.argv[1]
    worker_num = int(sys.argv[2])
    file_name = sys.argv[3]

    if op_name != "COUNT" and op_name != "CYCLE":
        print("Wrong operation. Enter COUNT or CYCLE.")
        sys.exit(1)
    
    if not 1 <= worker_num <= 10:
        print("Wrong number of workers. Enter a number between 1 and 10.")
        sys.exit(1)
    
    #call requested operations
    if op_name == "COUNT":
        print("Find citations operation is called")
        map_reduce = FindCitations(worker_num)
        map_reduce.start(file_name)

    elif op_name == "CYCLE":
        print("Find cyclic references operation is called")
        map_reduce = FindCyclicReferences(worker_num)
        map_reduce.start(file_name)
    

if __name__ == "__main__":
    main()