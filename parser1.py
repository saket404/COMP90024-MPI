from mpi4py import MPI
from json import JSONDecoder
from functools import partial
from collections import Counter
import getopt,sys,json,re,logging
import time

"""
Global variables.
"""
TAG_HT = 8080
TAG_LANG = 8081
MASTER_RANK = 0
LOG_LEVEL = logging.INFO


"""
Preprocessing functions
"""
def preprocess(text):
    text = text.lower()
    return text

def count_ht(text):
    text = preprocess(text)
    ht = re.findall(r"#(\w+)", text)
    count = Counter(ht)
    return count

"""
Arguement Check functions
"""
def print_usage():
    print ('usage is: parser.py -i <inputfile>')

def check_ags(argv):
    inputFile = ''
    try:
        opts, args = getopt.getopt(argv,"i:")
    except getopt.GetoptError as error:
        logging.error(error)
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-i"):
            inputFile = arg
    return inputFile

"""
Parallelising tweets processing functions and Output printing.
"""
def print_output(hashtags,languages):
    # Print Top 10 HashTags
    print("\n")
    print('----- Top 10 Hashtags -----')
    for i,ht in enumerate(hashtags):
        print(f'{i+1}. #{ht[0]}, {ht[1]}')

    print('----- Top 10 Languages -----')
    for i,lang in enumerate(languages):
        print(f'{i+1}. {lang[0]}, {lang[1]}')

def gather_tweets(comm):
    logging.info(f"Process: 0 (Master) | Gathering all data")
    processes = comm.Get_size()
    ht_counts = Counter([])
    lang_counts = Counter([])
    #Now ask all processes except oursevles to return counts
    for i in range(processes-1):
        # Send request
        comm.send('return_data', dest=(i+1), tag=(i+1))
    for i in range(processes-1):
        # Receive data
        ht_counts += comm.recv(source=(i+1), tag=TAG_HT)
        lang_counts += comm.recv(source=(i+1), tag=TAG_LANG)
    return ht_counts,lang_counts

def process_tweets(rank, input_file, processes):
    ht_occurences = Counter([])
    lang_occurences = Counter([])
    logging.info(f"Process: {rank} | Initiating processing task.")
    with open(input_file) as f:
        # Send tweets to slave processes
        try:
            for i, line in enumerate(f):
                line = line.replace(",\n","")
                if i%processes == rank:
                    try:
                        data = json.loads(line)
                        tweet = data['doc']['text']
                        lang_occurences += Counter([data['doc']['lang']])
                        ht_occurences += count_ht(tweet)
                    except ValueError:
                        logging.info(f"Process: {rank} | Malformed JSON on line: {i}")
                        
        except Exception:
            logging.error(f"Problem reading file.")

    logging.info(f"Process: {rank} | I am done Processing.")
    return ht_occurences,lang_occurences

def master_tweet_processor(comm, input_file):
    # Read our tweets
    rank = comm.Get_rank()
    size = comm.Get_size()

    log = "and I am alone!" if size == 1 else ""
    logging.info(f'Process: {rank} | I am Master! {log}')
    ht_counts,lang_counts = process_tweets(rank, input_file, size)
    if size > 1:
        ht_temp,lang_temp = gather_tweets(comm)
        logging.info(f"Process: 0 (Master) | Shutting Down slave(s)")
        ht_counts += ht_temp
        lang_counts += lang_temp
        # Turn everything off
        for i in range(size-1):
            # Receive data
            comm.send('exit', dest=(i+1), tag=(i+1))

    return ht_counts.most_common(10),lang_counts.most_common(10)

def slave_tweet_processor(comm,input_file):
    # We want to process all relevant tweets and send our counts back
    # to master when asked
    # Find my tweets
    rank = comm.Get_rank()
    size = comm.Get_size()
    logging.info(f'Process: {rank} | I am Slave!')
    ht_counts, lang_counts = process_tweets(rank, input_file, size)
    # Now that we have our counts then wait to see when we return them.
    while True:
        in_comm = comm.recv(source=MASTER_RANK, tag=rank)
        # Check if command
        if isinstance(in_comm, str):
            if in_comm in ("return_data"):
                # Send data back
                logging.info(f"Process: {rank} | Returning processed data to Master.")
                comm.send(ht_counts, dest=MASTER_RANK, tag=TAG_HT)
                comm.send(lang_counts, dest=MASTER_RANK, tag=TAG_LANG)
            elif in_comm in ("exit"):
                logging.info(f"Process: {rank} | Shutting Down....")
                exit(0)

def main(argv):
    inputFile = check_ags(argv)
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    if rank == 0 :
        # We are master
        hashtags,languages = master_tweet_processor(comm, inputFile)
        print_output(hashtags,languages)
    else:
        # We are slave
        slave_tweet_processor(comm, inputFile)


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s - %(message)s',level=LOG_LEVEL)
    start_time = time.time()
    main(sys.argv[1:])
    total = time.time() - start_time
    mins,secs = divmod(total,60)
    print("\n\n")
    print(f'Total Time for Execution is {mins} minutes and {round(secs,2)} seconds')
    