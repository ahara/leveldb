#include <algorithm>
#include <cassert>
#include <ctime> // time_t
#include <fstream>
#include <iostream>
#include <math.h>
#include <map>
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


WordIndex getSymbolId(map<string, WordIndex> &vocab, string symbol) {
    map<string, WordIndex>::iterator it = vocab.find(symbol);

    WordIndex wi = -1;

    if(it == vocab.end()) {
        wi = vocab.size();
        vocab[symbol] = wi;
    } else {
        wi = it->second;
    }

    return wi;
}


int main(int argc, char** argv) {
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    options.max_open_files = 4000;
    options.filter_policy = leveldb::NewBloomFilterPolicy(16);
    leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
    cout << status.ToString() << endl;
    assert(status.ok());

    map<string, WordIndex> vocab;
    vocab["<unk>"] = 0;
    vocab["<s>"] = 1;
    vocab["</s>"] = 2;
    vocab["<sp_sym1>"] = 3;


    // Read ttable data
    ifstream infile("/home/adam/Downloads/trg.lm");
    string line;
    vector<string> line_vec;
    int iter_cnt = 0;
    float cs, cst;
    vector<WordIndex> src;
    vector<Vector<WordIndex> > perfTest;

    while(getline(infile, line) && iter_cnt < 5e8) {  //9e6
        iter_cnt++;
        src.clear();
        line_vec.clear();
        std::istringstream iss(line);
        string item;

        while(iss >> item) {
            line_vec.push_back(item);
        }

        for(size_t i = 0; i < line_vec.size() - 2; i++) {
            WordIndex wi = getSymbolId(vocab, line_vec[i]);
            src.push_back(wi);
        }

        float count = atof(line_vec[line_vec.size() - 1].c_str());

        // Add src
        addEntry(db, src, count);

        if (iter_cnt % 2500 == 0) {
            //cout << vectorToStr(src) << endl;
            perfTest.push_back(src);
        }
    }

    cout << "Vocab size: " << vocab.size() << endl;


    int i = 0;

    random_shuffle(perfTest.begin(), perfTest.end());
    int src_cnt = 0;
    clock_t begin = clock();

    for(i = 0; i < perfTest.size(); i++) {
        int c = findCount(db, perfTest[i]);
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