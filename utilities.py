from mpi4py import MPI
from json import JSONDecoder
from functools import partial
from collections import Counter
import getopt,sys,json,re,logging
import time

# Language Code dictionary for full form BCP-47
CC = {
    'am':'Amharic',
    'ar':'Arabic',
    'hy':'Armenian',
    'bn':'Bengali',
    'bg':'Bulgarian',
    'my':'Burmese',
    'zh':'Chinese',
    'cs':'Czech',
    'da':'Danish',
    'nl':'Dutch',
    'en':'English',
    'et':'Estonian',
    'fi':'Finnish',
    'fr':'French',
    'ka':'Georgian',
    'de':'German',
    'el':'Greek',
    'gu':'Gujarati',
    'ht':'Haitian',
    'iw':'Hebrew',
    'hi':'Hindi',
    'hu':'Hungarian',
    'is':'Icelandic',
    'in':'Indonesian',
    'it':'Italian',
    'ja':'Japanese',
    'kn':'Kannada',
    'km':'Khmer',
    'ko':'Korean',
    'lo':'Lao',
    'lv':'Latvian',
    'lt':'Lithuanian',
    'ml':'Malayalam',
    'dv':'Maldivian',
    'mr':'Marathi',
    'ne':'Nepali',
    'no':'Norwegian',
    'or':'Oriya',
    'pa':'Panjabi',
    'ps':'Pashto',
    'fa':'Persian',
    'pl':'Polish',
    'pt':'Portuguese',
    'ro':'Romanian',
    'ru':'Russian',
    'sr':'Serbian',
    'sd':'Sindhi',
    'si':'Sinhala',
    'sk':'Slovak',
    'sl':'Slovenian',
    'ckb':'Sorani Kurdish',
    'es':'Spanish',
    'sv':'Swedish',
    'tl':'Tagalog',
    'ta':'Tamil',
    'te':'Telugu',
    'th':'Thai',
    'bo':'Tibetan',
    'tr':'Turkish',
    'uk':'Ukrainian',
    'ur':'Urdu',
    'ug':'Uyghur',
    'vi':'Vietnamese',
    'cy':'Welsh',
    'und':'Undefined'
}


"""
Preprocessing functions for lowercasing the text.
"""
def preprocess(text):
    text = text.lower()
    return text
    

"""
Arugement Check for input file.
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
Print Results for Top 10 hashtags and Languages used with corresponding frequency.
"""
def print_output(hashtags,languages):
    # Print Top 10 HashTags
    print("\n")
    print('----- Top 10 Hashtags -----')
    for i,ht in enumerate(hashtags):
        print(str(i+1)+'. #'+ht[0]+', '+str(ht[1]))

    print('----- Top 10 Languages -----')
    for i,lang in enumerate(languages):
        try:
            print(str(i+1)+'. '+CC[lang[0]]+' ('+lang[0]+'), '+str(lang[1]))
        except Exception:
            # In case the languge code is not in the dictionary.
            print(str(i+1)+'. ('+lang[0]+'), '+str(lang[1]))


