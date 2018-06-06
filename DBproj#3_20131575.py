# -*- coding: utf-8 -*- 

import datetime
import time
import sys
import MeCab
import operator
from pymongo import MongoClient
from bson import ObjectId
from itertools import combinations

stop_word = {}
DBname = "db20131575"
DBpassword = "db20131575"
conn = MongoClient("dbpurple.sogang.ac.kr")
db = conn[DBname]
db.authenticate(DBname, DBpassword)


def make_stop_word():
    f = open("wordList.txt", "r")
    while True:
        line = f.readline()
        if not line: break
        stop_word[line.strip('\n')] = line.strip('\n')
    f.close()

def morphing(content):
    t = MeCab.Tagger('-d/usr/local/lib/mecab/dic/mecab-ko-dic')
    nodes = t.parseToNode(content.encode('utf-8'))
    MorpList = []
    while nodes:
        if nodes.feature[0] == 'N' and nodes.feature[1] == 'N':
            w = nodes.surface
            if not w in stop_word:
                try:
                    w = w.encode('utf-8')
                    MorpList.append(w)
                except:
                    pass
        nodes = nodes.next
    return MorpList

def printMenu():
    print "0. CopyData"
    print "1. Morph"
    print "2. print morphs"
    print "3. print wordset"
    print "4. frequent item set"
    print "5. association rule"

def p0():
    col1 = db['news']
    col2 = db['news_freq']

    col2.drop()

    for doc in col1.find():
        contentDic = {}
        for key in doc.keys():
            if key != "_id":
                contentDic[key] = doc[key]
        col2.insert(contentDic)

def p1():

    if 'news_freq' not in db.collection_names():
        p0()

    for doc in db['news_freq'].find():
        doc['morph'] = morphing(doc['content'])
        db['news_freq'].update({"_id":doc['_id']}, doc)


def p2(url):
    data = db['news_freq'].find_one({"url": url}, {"morph":1, "_id": 0})
    morphs = data['morph']

    for morph in morphs:
        print(morph)


def p3():

    if 'news_freq' not in db.collection_names():
        p0()

    col1 = db['news_freq']
    col2 = db['news_wordset']
    col2.drop()
    for doc in col1.find():
        new_doc = {}
        new_set = set()
        for w in doc['morph']:
            new_set.add(w.encode('utf-8'))
        new_doc['word_set'] = list(new_set)
        new_doc['url'] = doc['url']
        col2.insert(new_doc)

def p4(url):

    if 'news_wordset' not in db.collection_names():
        p3()

    data = db['news_wordset'].find_one({"url": url}, {"word_set":1, "_id": 0})
    wordset = data['word_set']
    for word in wordset:
        print(word)


def p5(length):

    if 'news_freq' not in db.collection_names():
        p0()
        p1()
    if 'news_wordset' not in db.collection_names():
        p3()

    db['candidate_L' + str(length)].drop()

    if length > 3:
        length = 3

    iter_count = length
    total_wordset = {}
    for doc in db['news_wordset'].find({},{"word_set":1, "_id": 0}):
        for word in doc['word_set']:
            if word not in total_wordset:
                total_wordset[frozenset([word])] = 0

    min_sup = db['news_freq'].count()* 0.1
    key_size = 1
    while iter_count:  
        new_wordset={}

        if key_size != 1:

            for key1 in total_wordset.keys():
                for key2 in total_wordset.keys():
                    if key1 != key2:
                        keyset =key1|key2
                        if len(keyset) == key_size:
                            new_wordset[keyset] = 0
                        else:
                            pass

            total_wordset = new_wordset
            

        for doc in db['news_freq'].find():
            for key in total_wordset.keys():
                if key.issubset(doc['morph']):
                    total_wordset[key] += 1

        insertMany = []
        for key,value in total_wordset.items():
            if value < min_sup:
                del total_wordset[key]
            else:
                insertMany.append({"item_set": list(key), "support": total_wordset[key]})
        
        iter_count -= 1
        key_size += 1


        
        if key_size-1 != length:
            continue

        db['candidate_L' + str(key_size-1)].insert_many(insertMany)
    


def p6(length):
    
    if length == 1:
        pass

    elif length == 2:
        if 'candidate_L2' not in db.collection_names():
            p5(2)
        if 'candidate_L1' not in db.collection_names():
            p5(1)

        for doc in db['candidate_L2'].find({}, {"item_set": 1, "support": 1, "_id": 0}):
            itemset  = doc['item_set']
            p_3 = doc['support']
            for p in db['candidate_L1'].find({'item_set': itemset[0]}, {"support": 1, "_id": 0}):
                item1 = itemset[0]
                p_1 = p['support']
            for p in db['candidate_L1'].find({'item_set': itemset[1]}, {"support": 1, "_id": 0}):
                item2 = itemset[1]
                p_2 = p['support']

            if float(p_3)/float(p_1) > 0.5:
                print("{} => {}   {}".format(item1, item2, str(float(p_3)/float(p_1))))
            if float(p_3)/float(p_2) > 0.5:
                print("{} => {}   {}".format(item2, item1, str(float(p_3)/float(p_2))))

    elif length == 3:
        if 'candidate_L3' not in db.collection_names():
            p5(3)
        if 'candidate_L2' not in db.collection_names():
            p5(2)


        for doc in db['candidate_L3'].find({}, {"item_set": 1, "support": 1, "_id": 0}):
            itemset  = doc['item_set']
            p_3 = doc['support']
            comb = combinations(itemset,2)
            itemset2 = set(itemset)
            for i in list(comb):
                j = itemset2-set(i)
                j = list(j) 
                for p in db['candidate_L2'].find({'item_set': i}, {"support": 1, "_id": 0}):
                    p_1 = p['support']

                if float(p_3)/float(p_1) > 0.5:
                    print("{}, {} => {}   {}".format(i[0], i[1], j[0], str(float(p_3)/float(p_1)))) 

            comb = combinations(itemset,1)
            
            for i in list(comb):
                j = itemset2 - set(i)
                j = list(j)
                for p in db['candidate_L1'].find({'item_set': i}, {"support": 1, "_id": 0}):
                    p_1 = p['support']

                if float(p_3)/float(p_1) > 0.5:
                    print("{} => {}, {}   {}".format(i[0], j[0], j[1], str(float(p_3)/float(p_1))))

    else:
        pass


if __name__ == "__main__":
    make_stop_word()
    printMenu()
    selector = input()
    
    if selector == 0:
        p0()
    elif selector == 1:
        p1()
        p3()
    elif selector == 2:
        url = str(raw_input("input news url:"))
        p2(url)
    elif selector == 3:
        url = str(raw_input("input news url:"))
        p4(url)
    elif selector == 4:
        length = int(raw_input("input length of the frequent item:"))
        p5(length)
    elif selector ==5:
        length = int(raw_input("input length of the frequent item:"))
        p6(length)
