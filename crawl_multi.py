import requests
from bs4 import BeautifulSoup
import csv
import codecs
import os
import time
import pandas as pd
import math
import numpy as np
import time, multiprocessing


count = 0
en_titles = 'enwiki-20230520-all-titles' # input path

base_url = 'https://en.wikipedia.org/wiki/'
ACCEPTED_LANGS = set(['en', 'bn', 'ta', 'te', 'mr', 'hi', 'gu', 'pa-guru', 'as', 'ur', 'kn', 'ml'])

def extract_lan(title):
  try:
    result = {'en': title}
    url = base_url + title
    page = requests.get(url)

    soup = BeautifulSoup(page.text, 'html.parser')
    menu = soup.find_all('li', {'class': 'interlanguage-link'})

    for item in menu:
      a = item.a
      if a['lang'] in ACCEPTED_LANGS:
        result[a['lang']] = a['title'].split('–')[0].strip()

    return result
  except Exception as e:
    #print(title)
    return {}

'''
with open(en_titles, 'r') as file:
    lines = file.read().split('\n')
'''   
#f_open = open('title.csv', 'a')
#f_open.write('en'+ '\t'+ 'bn'+ '\t'+ 'ta'+ '\t'+ 'te'+ '\t'+ 'mr'+ '\t'+ 'hi'+ '\t'+ 'gu'+ '\t'+ 'pa-guru'+ '\t'+ 'as'+ '\t'+ 'ur'+ '\t'+ 'kn'+ '\t'+ 'ml' + '\n')

def convert(seconds):
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
     
    return "%d:%02d:%02d" % (hour, minutes, seconds)

def work(lists, file_name):
  ct = time.time()
  proc = 0
  f_name = './csv/' + str(file_name) + '.csv'
  f_open = open(f_name, 'a')
  f_open.write('en'+ '\t'+ 'bn'+ '\t'+ 'ta'+ '\t'+ 'te'+ '\t'+ 'mr'+ '\t'+ 'hi'+ '\t'+ 'gu'+ '\t'+ 'pa-guru'+ '\t'+ 'as'+ '\t'+ 'ur'+ '\t'+ 'kn'+ '\t'+ 'ml' + '\n')
  for lis in lists:
    lang_Dict = {}
    results = extract_lan(lis)
    #print(results)
    for lang in ACCEPTED_LANGS:
      try:
        lang_Dict[lang] = results[lang]
      except KeyError:
        lang_Dict[lang] = ' '
    #break
    f_open.write(lang_Dict['en']+ '\t'+ lang_Dict['bn']+ '\t'+ lang_Dict['ta']+ '\t'+ lang_Dict['te']+ '\t'+ lang_Dict['mr']+ '\t'+ lang_Dict['hi']+ '\t'+ lang_Dict['gu']+ '\t'+ lang_Dict['pa-guru']+ '\t'+ lang_Dict['as']+ '\t'+ lang_Dict['ur']+ '\t'+ lang_Dict['kn']+ '\t'+ lang_Dict['ml'] + '\n')
    proc = proc + 1
    if proc%100==0:
      pt = time.time()
      te = round((pt-ct)/proc, 2)
      print('{} data processed so far for process {}. Time Taken: {} sec/data.'.format(proc, file_name, te))
  #f_open.close()



def split(list_a, chunk_size):
  for i in range(0, len(list_a), chunk_size):
    yield list_a[i:i + chunk_size]


file_list = []
chunk_size = 582110
p = []

if __name__ == "__main__":
  with open(en_titles, 'r') as file:
    lines = file.read().split('\n')
    
  for line in lines[1:]:
    if len(line.strip().split('\t')) > 1:
      #print(line.strip().split('\t'))
      count = count + 1
      if count%100000 == 0:
        print(count)
      file_list.append(line.strip().split('\t')[1])
   
  file_list_chunk = list(split(file_list, chunk_size))
  
  core = len(file_list_chunk)
  print('core: {}'.format(core))
  for k in range(core):
    p.append(multiprocessing.Process(target=work, args=(file_list_chunk[k], k )))
  
  for k in range(core):
    p[k].start()
  
  for k in range(core):
    p[k].join()
 
  
'''
for line in lines[1:]:

  count = count + 1
  lang_Dict = {}
  results = extract_lan(line.split('\t')[1])
  #print(results)
  for lang in ACCEPTED_LANGS:
    try:
      lang_Dict[lang] = results[lang]
    except KeyError:
      lang_Dict[lang] = ' '
  #break
  f_open.write(lang_Dict['en']+ '\t'+ lang_Dict['bn']+ '\t'+ lang_Dict['ta']+ '\t'+ lang_Dict['te']+ '\t'+ lang_Dict['mr']+ '\t'+ lang_Dict['hi']+ '\t'+ lang_Dict['gu']+ '\t'+ lang_Dict['pa-guru']+ '\t'+ lang_Dict['as']+ '\t'+ lang_Dict['ur']+ '\t'+ lang_Dict['kn']+ '\t'+ lang_Dict['ml'] + '\n')
  if count % 100 == 0:
    pt = time.time()
    te = round((pt-ct)/count, 2)
    print('{} data processed so far. Time Taken: {} sec/data. Estimated Time: {}'.format(count, te, convert(te*int(58210907))))
    print('--------------------------------------------------------------------')
'''
