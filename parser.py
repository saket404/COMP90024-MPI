from mpi4py import MPI
from json import JSONDecoder
from functools import partial
from collections import Counter
from utilities import preprocess, print_usage, check_ags, print_output
import getopt,sys,json,re,logging
import time


"""
Global variables.
"""
TAG_HT = 8080
TAG_LANG = 8081
MASTER_RANK = 0
LOG_LEVEL = logging.ERROR



"""
Main Paralellising Functions
"""

def gather_tweets(comm):
    logging.info("Process: 0 (Master) | Gathering all data")
    processes = comm.Get_size()
    ht_counts = Counter([])
    lang_counts = Counter([])
    #Now ask all processes except oursevles to return counts
    for i in range(processes-1):
        # Send request
        comm.send('send_data', dest=(i+1), tag=(i+1))
    for i in range(processes-1):
        # Receive data
        ht_counts += comm.recv(source=(i+1), tag=TAG_HT)
        lang_counts += comm.recv(source=(i+1), tag=TAG_LANG)
    return ht_counts,lang_counts

def process_tweets(rank, input_file, processes):
    ht_occurences = Counter([])
    lang_occurences = Counter([])

    with open(input_file) as f:
        logging.info(f"Process: {rank} | Initiating processing task.")
        try:
            for i, line in enumerate(f):
                line = line.replace(",\n","")
                if i%processes == rank:
                    try:
                        data = json.loads(line)
                        lang_occurences[data['doc']['lang']] += 1
                        hashtags = [preprocess(i['text']) for i in data['doc']['entities']['hashtags']]
                        for ht in hashtags:
                            ht_occurences[ht] += 1

                    except ValueError:
                        logging.info(f"Process: {rank} | Malformed JSON on line: {i}")
        except Exception:
            logging.error(f"Problem reading file.")

    logging.info(f"Process: {rank} | I am done Processing.")

    return ht_occurences,lang_occurences

def master_worker(comm, input_file):
    # Read our tweets
    rank = comm.Get_rank()
    size = comm.Get_size()

    
    if size > 1:
        logging.info(f'Process: {rank} | I am Master!')
        ht_counts,lang_counts = process_tweets(rank, input_file, size)
        ht_temp,lang_temp = gather_tweets(comm)
        logging.info(f"Process: 0 (Master) | Shutting Down slave(s)")
        ht_counts += ht_temp
        lang_counts += lang_temp
        # Turn everything off
        for i in range(size-1):
            comm.send('exit', dest=(i+1), tag=(i+1))

        return ht_counts.most_common(10),lang_counts.most_common(10)

    else:
        logging.info(f'Process: {rank} | I am processing alone!')
        ht_counts = Counter([])
        lang_counts = Counter([])
        with open(input_file) as f:
            logging.info(f"Process: {rank} | Initiating processing task.")
            try:
                for i, line in enumerate(f):
                    line = line.replace(",\n","")  
                    try:
                        data = json.loads(line)
                        lang_counts[data['doc']['lang']] += 1
                        hashtags = [preprocess(i['text']) for i in data['doc']['entities']['hashtags']]
                        for ht in hashtags:
                            ht_counts[ht] += 1
                    except ValueError:
                        logging.info(f"Process: {rank} | Malformed JSON on line: {i}")
            except Exception:
                logging.error(f"Problem reading file.")

        logging.info(f"Process: {rank} | I am done Processing.")

        return ht_counts.most_common(10),lang_counts.most_common(10)
    
def slave_worker(comm,input_file):
    rank = comm.Get_rank()
    size = comm.Get_size()
    logging.info(f'Process: {rank} | I am Slave!')
    ht_counts, lang_counts = process_tweets(rank, input_file, size)

    while True:
        in_comm = comm.recv(source=MASTER_RANK, tag=rank)
        # Check if command
        if isinstance(in_comm, str):
            if in_comm in ("send_data"):
                # Send data back
                logging.info(f"Process: {rank} | Returning processed data to Master.")
                comm.send(ht_counts, dest=MASTER_RANK, tag=TAG_HT)
                comm.send(lang_counts, dest=MASTER_RANK, tag=TAG_LANG)
            elif in_comm in ("exit"):
                logging.info(f"Process: {rank} | Shutting Down....")
                exit(0)

def main(argv):
    start_time = time.time()
    inputFile = check_ags(argv)
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    if rank == 0 :
        hashtags,languages = master_worker(comm, inputFile)
    else:
        slave_worker(comm, inputFile)

    total = time.time() - start_time
    print_output(hashtags,languages)
    print("\n\n")
    print(f'Total Time for task is {round(total,4)} seconds.')
    


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s - %(message)s',level=LOG_LEVEL)
    main(sys.argv[1:])
    