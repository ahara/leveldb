#include <algorithm>
#include <cassert>
#include <ctime> // time_t
#include <fstream>
#include <iostream>
#include <math.h>
#include <sstream>
#include <stdio.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/write_batch.h"


using namespace std;

#define WORD_INDEX_MODULO_BYTES 5
#define WORD_INDEX_MODULO_BASE 254
#define UNUSED_WORD 2
#define Vector vector

typedef unsigned int WordIndex;


string to_string2(int x)
{
    stringstream ss;
    ss << x;
    return ss.str();
}


string vectorToStr(const Vector<WordIndex>& s)
{
    string str;

    for(int i = 0; i < s.size(); i++) {
        str += to_string2(s[i]) + " ";
    }

    return str;
}


string vectorToString(const Vector<WordIndex>& s)
{
    //return vectorToStr(s);

  Vector<WordIndex> str;
  for(int i = 0; i < s.size(); i++) {
    // Use WORD_INDEX_MODULO_BYTES bytes to encode index
    for(int j = WORD_INDEX_MODULO_BYTES - 1; j >= 0; j--) {
      str.push_back(1 + (s[i] / (int) pow(WORD_INDEX_MODULO_BASE, j) % WORD_INDEX_MODULO_BASE));
    }
  }

  string phrase(str.begin(), str.end());

  return phrase;
}


int findCount(leveldb::DB* db, const Vector<WordIndex>& s) {
    string value;
    int s_value = 0;
    string s_key = vectorToString(s);
    leveldb::Status result = db->Get(leveldb::ReadOptions(), s_key, &value);  // Read old src value

    if (result.ok()) {
        s_value = atoi(value.c_str());
    }

    return s_value;
}

leveldb::Status addEntry(leveldb::DB* db, const Vector<WordIndex>& v, int count) {
    int v_count = findCount(db, v) + count;

    leveldb::WriteBatch batch;
    batch.Put(vectorToString(v), to_string2(v_count));
    leveldb::Status s = db->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) cerr << s.ToString() << endl;
    
    return s;
}


Vector<WordIndex> getSrc(const Vector<WordIndex>& s)
{
    // Prepare s vector as (UNUSED_WORD, s)
    Vector<WordIndex> uw_s_vec;
    uw_s_vec.push_back(UNUSED_WORD);
    uw_s_vec.insert(uw_s_vec.end(), s.begin(), s.end());

    return uw_s_vec;
}


Vector<WordIndex> getTrgSrc(const Vector<WordIndex>& s, const Vector<WordIndex>& t)
{
    // Prepare (t,s) vector as (t, UNUSED_WORD, s)
    Vector<WordIndex> t_uw_s_vec = t;
    t_uw_s_vec.push_back(UNUSED_WORD);
    t_uw_s_vec.insert(t_uw_s_vec.end(), s.begin(), s.end());

    return t_uw_s_vec;
}


int getSrcPhrases(leveldb::DB* db, const Vector<WordIndex>& t) {
    Vector<WordIndex> start_vec(t);
    start_vec.push_back(UNUSED_WORD);

    Vector<WordIndex> end_vec(t);
    end_vec.push_back(3);

    string start_str = vectorToString(start_vec);
    string end_str = vectorToString(end_vec);

    leveldb::Slice start = start_str;
    leveldb::Slice end = end_str;

    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    
    int i = 0;
    for(it->Seek(start); it->Valid() && it->key().ToString() < end.ToString(); it->Next(), i++) {
        it->value().ToString();
    }
    assert(it->status().ok());  // Check for any errors found during the scan
    assert(i > 0);
    delete it;

    return i;
}


int main(int argc, char** argv) {
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    options.max_open_files = 4000;
    options.filter_policy = leveldb::NewBloomFilterPolicy(16);
    //leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
    leveldb::Status status = leveldb::DB::Open(options, "/tmp/t2", &db);
    cout << status.ToString() << endl;
    assert(status.ok());

    // Creating keys
    /*leveldb::Slice key1 = "hello";
    leveldb::Slice key2 = "world";
    int words[] = {0x39393939, 0x35363738, 50};  // INT has reversed byte order
    leveldb::Slice key3 = (char *) words;
    std::string str = key1.ToString();
    assert(str == std::string("hello"));

    std::string value("10");
    leveldb::Status s = db->Put(leveldb::WriteOptions(), key1, value);
    printf("Adding (%s, %s): %s\n", key1.ToString().c_str(), value.c_str(), s.ToString().c_str());
    s = db->Get(leveldb::ReadOptions(), key1, &value);
    if (s.ok()) s = db->Put(leveldb::WriteOptions(), key2, value);
    if (s.ok()) s = db->Put(leveldb::WriteOptions(), key3, to_string(22));
    //if (s.ok()) s = db->Delete(leveldb::WriteOptions(), key1);

    // Iterating over the keys
    cout << "Iterating: " << endl;
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
    }
    assert(it->status().ok());  // Check for any errors found during the scan
    delete it;

    // Find and iterate
    /*int i;
    for(i = 0; i < 5e7; i++) {
        leveldb::Slice k = to_string(i);
        if (s.ok()) s = db->Put(leveldb::WriteOptions(), k, to_string(i * i));
    }
    cout << i << endl;

    s = db->Get(leveldb::ReadOptions(), to_string(8), &value);
    cout << s.ToString() << value << endl;

    cout << "Iterating: " << endl;
    leveldb::Slice start = to_string(7);
    it = db->NewIterator(leveldb::ReadOptions());
    i = 0;
    //for(it->Seek(start); it->Valid() && it->key().ToString() < "999"; it->Next(), i++) {
    for (it->SeekToFirst(); it->Valid(); it->Next(), i++) {
        //cout << it->key().ToString() << "(: " << i << "): " << it->value().ToString() << endl;
    }
    cout << i << endl;
    assert(it->status().ok());  // Check for any errors found during the scan
    delete it;*/

    // Read ttable data
    //ifstream infile("/home/adam/Downloads/src_trg_100K.idttable");
    ifstream infile("/home/adam/Downloads/src_trg_20M.idttable");
    string line;
    int iter_cnt = 0;
    int max_word_index = -1;
    float cs, cst;
    vector<unsigned int> src, trg;
    vector<Vector<WordIndex> > perfTest;
    while(getline(infile, line) && iter_cnt < 1e8) {  //9e6
        iter_cnt++;
        //if (iter_cnt < 80e5)
        //    continue;
        src.clear();
        trg.clear();
        int n = 0;
        std::istringstream iss(line);
        string item;
        float count;

        while(iss >> item) {
            if (item == "|||") {
                n++;
            } else if (n == 0) {
                src.push_back(atoi(item.c_str()));
            } else if (n == 1) {
                int trg_index = atoi(item.c_str());
                trg.push_back(trg_index);
                max_word_index = max(max_word_index, trg_index);
            } else if (n == 2) {
                n++;
            } else if (n == 3) {
                count = atof(item.c_str());
            }
        }

        /*cout << "Item: ";
        for(int i = 0; i < src.size(); i++) {
            cout << src[i] << " ";
        }
        cout << endl;*/

        // Add src
        /*addEntry(db, getSrc(src), count);
        addEntry(db, trg, count);
        addEntry(db, getTrgSrc(src, trg), count);*/

        if (iter_cnt % 2500 == 0) {
            //cout << vectorToStr(trg) << endl;
            perfTest.push_back(trg);
        }
    }

    cout << "Max Word Index: " << max_word_index << endl;


    /*cout << "Iterating: " << endl;
    leveldb::Slice start = to_string(7);
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());*/
    int i = 0;
    //for(it->Seek(start); it->Valid() && it->key().ToString() < "999"; it->Next(), i++) {
    /*for (it->SeekToFirst(); it->Valid(); it->Next(), i++) {
        //cout << it->key().ToString() << "(: " << i << "): " << it->value().ToString() << endl;
    }
    cout << i << endl;
    assert(it->status().ok());  // Check for any errors found during the scan
    delete it;*/

    random_shuffle(perfTest.begin(), perfTest.end());
    int src_cnt = 0;
    clock_t begin = clock();

    /*leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    options.max_open_files = 4000;
    options.filter_policy = leveldb::NewBloomFilterPolicy(16);
    leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
    assert(status.ok());*/

    for(i = 0; i < perfTest.size(); i++) {
        int c = getSrcPhrases(db, perfTest[i]);
        //cout << i << ": " << c << " " << vectorToStr(perfTest[i]) << endl;
        src_cnt += c;
    }

    clock_t end = clock();
    double difference = double(end - begin);

    cout << "Performance: " << difference / (double) CLOCKS_PER_SEC << endl;
    cout << "Performance per sentence: " << difference / (double) CLOCKS_PER_SEC / perfTest.size() << endl;
    cout << "Test set: " << perfTest.size() << endl;
    cout << "Count of src: " << src_cnt << endl;

    delete db;
    delete options.filter_policy;

    printf("=== FINISHED TESTING ===\n");
    
    return 0;
}