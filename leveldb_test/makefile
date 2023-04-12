leveldb_test:leveldb_test.cpp
	g++ -o leveldb_test leveldb_test.cpp -pthread -I./third_party/leveldb/include -L./third_party/leveldb/lib  -lleveldb -std=c++11

clear:
	rm -f leveldb_test
	rm -rf testdb
