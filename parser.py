from mpi4py import MPI
from json import JSONDecoder
from functools import partial
from collections import Counter
import getopt,sys,json,re

TAG_HT = 8080
TAG_LANG = 8081
MASTER_RANK = 0


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
        print(error)
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-i"):
            inputFile = arg
    return inputFile

"""
Parallelising tweets processing functions
"""
def gather_tweets(comm):
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
    languages = []
    with open(input_file) as f:
        # Send tweets to slave processes
        try:
            for i, line in enumerate(f):
                line = line.replace(",\n","")
                if i%processes == rank:
                    try:
                        data = json.loads(line)
                        tweet = data['doc']['text']
                        languages.append(data['doc']['lang'])
                        ht_occurences += count_ht(tweet)
                    except ValueError:
                        print(f"Malformed JSON on line: {i}")
                        
        except Exception as e:
            print(f"Process {rank} has problem reading.")

    lang_occurences = Counter(languages)

    return ht_occurences,lang_occurences

def master_tweet_processor(comm, input_file):
    # Read our tweets
    rank = comm.Get_rank()
    size = comm.Get_size()

    ht_counts,lang_counts = process_tweets(rank, input_file, size)
    if size > 1:
        ht_temp,lang_temp = gather_tweets(comm)
        ht_counts += ht_temp
        lang_counts += lang_temp
        # Turn everything off
        for i in range(size-1):
            # Receive data
            comm.send('exit', dest=(i+1), tag=(i+1))

    # Print output
    print(ht_counts.most_common(10))
    print(lang_counts.most_common(10))

def slave_tweet_processor(comm,input_file):
    # We want to process all relevant tweets and send our counts back
    # to master when asked
    # Find my tweets
    rank = comm.Get_rank()
    size = comm.Get_size()

    ht_counts, lang_counts = process_tweets(rank, input_file, size)
    # Now that we have our counts then wait to see when we return them.
    while True:
        in_comm = comm.recv(source=MASTER_RANK, tag=rank)
        # Check if command
        if isinstance(in_comm, str):
            if in_comm in ("return_data"):
                # Send data back
                # print("Process: ", rank, " sending back ", len(counts), " items")
                comm.send(ht_counts, dest=MASTER_RANK, tag=TAG_HT)
                comm.send(lang_counts, dest=MASTER_RANK, tag=TAG_LANG)
            elif in_comm in ("exit"):
                exit(0)

def main(argv):
    inputFile = check_ags(argv)
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    if rank == 0 :
        # We are master
        master_tweet_processor(comm, inputFile)
    else:
        # We are slave
        slave_tweet_processor(comm, inputFile)


"""
Normal code without MPI
"""

def main_normal(argv):
    inputFile = check_ags(argv)
    print(inputFile)
    if inputFile == '':
        print_usage()
        sys.exit(2)

    languages = []
    ht_count = Counter([])
    with open(inputFile) as f:
        for i,line in enumerate(f):
            line = line.replace(",\n","")
            try:
                data = json.loads(line)
                languages.append(data['doc']['lang'])
                ht_count += count_ht(data['doc']['text'])
            except ValueError:
                print(f"Malformed JSON on line: {i}")
                pass

    lang_count = Counter(languages) 
    print(lang_count.most_common(10))
    print(ht_count.most_common(10))


if __name__ == "__main__":
    import time
    start_time = time.time()
    print("-----------START------------------")
    # main(sys.argv[1:])
    main_normal(sys.argv[1:])
    print("--- %s seconds ---" % (time.time() - start_time))
    