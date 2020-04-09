from mpi4py import MPI
from json import JSONDecoder
from functools import partial
from collections import Counter, defaultdict
from utilities import preprocess, print_usage, check_ags, print_output
from operator import itemgetter
import getopt,sys,json,re,logging
import time


"""
Global variables.
"""
MASTER_RANK = 0
LOG_LEVEL = logging.ERROR # Set Logging level to only report errors.

"""
Defining an MPI operation to add Counters for MPI Reduce
"""
def addCounter(counter1, counter2, datatype):
    counter1 += counter2
    return counter1
 
"""
Common Task processing function where each process (multi case) reads its own part of the file
line by line and returns a language and hashtag frequency table.
"""
def process_tweets(rank, input_file, processes):
    # Counter usage for hashable objects
    ht_occurences = Counter([])
    lang_occurences = Counter([])

    with open(input_file) as f:
        logging.info("Process: "+str(rank)+" | Initiating processing task.")
        try:
            for idx, tweet in enumerate(f):
                tweet = tweet.replace(",\n","")
                # Using modulo to ensure each process only reads its designated lines.
                if idx % processes == rank:
                    try:
                        data = json.loads(tweet)
                        lang_occurences[data['doc']['lang']] += 1
                        hashtags = [preprocess(i['text']) for i in data['doc']['entities']['hashtags']]
                        for ht in hashtags:
                            ht_occurences[ht] += 1

                    except ValueError:
                        logging.info("Process: "+str(rank)+" | Malformed JSON on line: "+str(idx))
        except Exception:
            logging.error("Problem reading file.")

    logging.info("Process: "+str(rank)+" | I am done Processing.")

    return ht_occurences,lang_occurences

"""
Main Function where tasks are defined and based on ranks(multi case) are assigned.
"""
def main(argv):
    
    # Start timing and define MPI variables
    start_time = MPI.Wtime()    
    inputFile = check_ags(argv)
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank() 
    size = comm.Get_size()  

    # Define new MPI operation to use with reduce for summing counters.
    counterSumOp = MPI.Op.Create(addCounter, commute=True)

    # Define Counter objects for Language and hashtags for MPI reduce (multi worker case) or counting (single worker case)
    tot_ht_counts = Counter([])
    tot_lang_counts = Counter([])

    # Master worker tasks
    if rank == 0 :

        # Multi worker case process using the line split algorithm.
        if size > 1:
            logging.info("Process: "+str(rank)+" | I am Master!")
            ht_counts,lang_counts = process_tweets(rank, inputFile, size)

        # Single worker case just process line by line normally.
        else:
            logging.info("Process: "+str(rank)+" | I am processing alone!")
            with open(inputFile) as f:
                logging.info("Process: "+str(rank)+" | Initiating processing task.")
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
                            logging.info("Process: "+str(rank)+" | Malformed JSON on line: "+str(i))
                except Exception:
                    logging.error("Problem reading file.")

            logging.info("Process: "+str(rank)+" | I am done Processing.")

    # If not Master worker perform its own processing
    else:
        logging.info("Process: "+str(rank)+" | I am not Master!")
        ht_counts,lang_counts = process_tweets(rank, inputFile, size)

    # If multi-worker case perform Reduce Operation to add counters from each process to Master worker.
    if size > 1:
        tot_ht_counts = comm.reduce(ht_counts, op=counterSumOp,root = 0)
        tot_lang_counts = comm.reduce(lang_counts, op=counterSumOp,root = 0)

    # If Master worker extract the top 10 for Hashtags and Languages and Return results and timing.
    if rank == 0:
        top_ht = tot_ht_counts.most_common(10)
        top_lang = tot_lang_counts.most_common(10)

        print_output(top_ht,top_lang)
        total = MPI.Wtime() - start_time
        print("\n\n")
        print("Total Time for task is "+str(round(total,4))+" seconds.")


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s - %(message)s',level=LOG_LEVEL)
    main(sys.argv[1:])