from mpi4py import MPI
from json import JSONDecoder
from functools import partial
from collections import Counter, defaultdict
from utilities import preprocess, print_usage, check_ags, print_output
from operator import itemgetter
import getopt,sys,json,re,logging
import time
from heapq import nlargest


"""
Global variables.
"""
MASTER_RANK = 0
LOG_LEVEL = logging.ERROR

def addCounter(counter1, counter2, datatype):
    counter1 += counter2
    return counter1
 
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

def main(argv):
    start_time = time.time()
    inputFile = check_ags(argv)
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank() 
    size = comm.Get_size()  

    counterSumOp = MPI.Op.Create(addCounter, commute=True)
    tot_ht_counts = Counter([])
    tot_lang_counts = Counter([])

    if rank == 0 :
        if size > 1:
            logging.info(f'Process: {rank} | I am Master!')
            ht_counts,lang_counts = process_tweets(rank, inputFile, size)
        else:
            logging.info(f'Process: {rank} | I am processing alone!')
            with open(inputFile) as f:
                logging.info(f"Process: {rank} | Initiating processing task.")
                try:
                    for i, line in enumerate(f):
                        line = line.replace(",\n","")  
                        try:
                            data = json.loads(line)
                            tot_lang_counts[data['doc']['lang']] += 1
                            hashtags = [preprocess(i['text']) for i in data['doc']['entities']['hashtags']]
                            for ht in hashtags:
                                tot_ht_counts[ht] += 1
                        except ValueError:
                            logging.info(f"Process: {rank} | Malformed JSON on line: {i}")
                except Exception:
                    logging.error(f"Problem reading file.")

            logging.info(f"Process: {rank} | I am done Processing.")

    else:
        logging.info(f'Process: {rank} | I am Master!')
        ht_counts,lang_counts = process_tweets(rank, inputFile, size)


    if size > 1:
        tot_ht_counts = comm.reduce(ht_counts, op=counterSumOp,root = 0)
        tot_lang_counts = comm.reduce(lang_counts, op=counterSumOp,root = 0)


    if rank == 0:

        top_ht = tot_ht_counts.most_common(10)
        top_lang = tot_lang_counts.most_common(10)

        total = time.time() - start_time
        print_output(top_ht,top_lang)
        print("\n\n")
        print(f'Total Time for task is {round(total,4)} seconds.')


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s - %(message)s',level=LOG_LEVEL)
    main(sys.argv[1:])