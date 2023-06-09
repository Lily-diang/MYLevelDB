/*
 * @Author: Li_diang 787695954@qq.com
 * @Date: 2023-03-04 21:27:02
 * @LastEditors: Li_diang 787695954@qq.com
 * @LastEditTime: 2023-04-23 23:14:56
 * @FilePath: \leveldb\benchmarks\db_bench.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sys/types.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"
#include "leveldb/Remix.h"
#include "leveldb/RemixIterator.h"

// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//      fillseq       -- write N values in sequential key order in async mode
//      fillrandom    -- write N values in random key order in async mode
//      overwrite     -- overwrite N values in random key order in async mode
//      fillsync      -- write N/100 values in random key order in sync mode
//      fill100K      -- write N/1000 100K values in random order in async mode
//      deleteseq     -- delete N keys in sequential order
//      deleterandom  -- delete N keys in random order
//      readseq       -- read N times sequentially
//      readreverse   -- read N times in reverse order
//      readrandom    -- read N times in random order
//      readmissing   -- read N missing keys in random order
//      readhot       -- read N times in random order from 1% section of DB
//      seekrandom    -- N random seeks
//      seekordered   -- N ordered seeks
//      open          -- cost of opening a DB
//      crc32c        -- repeated crc32c of 4K of data
//   Meta operations:
//      compact     -- Compact the entire DB
//      stats       -- Print DB stats
//      sstables    -- Print sstable info
//      heapprofile -- Dump a heap profile (if supported by this port)

// 按指定顺序运行的以逗号分隔的操作列表
// 实际基准：
// fillseq -- 在异步模式下按顺序键顺序写入 N 个值
// fillrandom -- 在异步模式下以随机键顺序写入 N 个值
// overwrite -- 在异步模式下以随机键顺序覆盖 N 个值
// fillsync -- 在同步模式下以随机键顺序写入 N/100 个值
// fill100K -- 在异步模式下以随机顺序写入 N/1000 个 100K 值
// deleteseq -- 按顺序删除 N 个键
// deleterandom -- 以随机顺序删除 N 个键
// readseq -- 连续读取 N 次
// readreverse -- 倒序读取N次
// readrandom -- 随机读取 N 次
// readmissing--以随机顺序读取 N 个丢失的键
// readhot -- 从 DB 的 1% 部分以随机顺序读取 N 次
// seekrandom -- N 次随机搜索
// seekordered -- N 有序查找
// open -- 打开数据库的成本
// crc32c -- 4K数据的重复crc32c
// 元操作：
// compact -- 压缩整个数据库
// stats -- 打印数据库统计信息
// sstables -- 打印 sstable 信息
// heapprofile -- 转储堆配置文件（如果此端口支持）
static const char* FLAGS_benchmarks =
    "fillseq,"      // 顺序写方式创建数据库，只需将数据写入操作系统的缓冲区即可
    //"fillsync,"     // 每次写操作，均将数据同步写到磁盘中才算操作完成；
    //"fillrandom,"   // 以随机写方式创建数据库
    //"overwrite,"    // 以随机写方式更新数据库中的某些存在的key的数据
    "readrandom,"   // 以随机的方式进行查询读
    "readrandom,"  // Extra run to allow previous compactions to quiesce
    "50 seek+next op with leveldb,"
    "create view,"
    "50 seek+next op with Remix,"
    //"single seek+next op with leveldb,"
    //"create view,"
    //"single seek+next op with Remix,"
    //"readseq_Leveldb,"
    //"create view,"
    //"readseq_Remix,"    // 按正向顺序读
    // "readreverse,"  // 按逆向顺序读
    // "compact,"    
    // "readrandom,"
    // "readseq_Leveldb,"
    //"create view,"
    //"readseq_Remix,"
    // "readreverse,"
    // "fill100K,"
    // "crc32c,"
    // "snappycomp,"
    // "snappyuncomp,"
    ;

// Number of key/values to place in database
static int FLAGS_num = 1000;

// Number of read operations to do.  If negative, do FLAGS_num reads.
static int FLAGS_reads = -1;

// Number of concurrent threads to run.
static int FLAGS_threads = 1;

// Size of each value 每个value的大小（字节）
static int FLAGS_value_size = 1000;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
// 压缩比例
static double FLAGS_compression_ratio = 0.5;

// Print histogram of operation timings
// 打印操作时间的直方图
static bool FLAGS_histogram = false;

// Count the number of string comparisons performed
// 统计执行的字符串比较次数
static bool FLAGS_comparisons = false;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
static int FLAGS_write_buffer_size = 0;

// Number of bytes written to each file.
// (initialized to default value by "main")
static int FLAGS_max_file_size = 0;

// Approximate size of user data packed per block (before compression.
// (initialized to default value by "main")
static int FLAGS_block_size = 0;

// Number of bytes to use as a cache of uncompressed data.
// Negative means use default settings.
static int FLAGS_cache_size = -1;

// Maximum number of files to keep open at the same time (use default if == 0)
static int FLAGS_open_files = 0;

// Bloom filter bits per key.
// Negative means use default settings.
static int FLAGS_bloom_bits = -1;

// Common key prefix length.
// 公共前缀长度
static int FLAGS_key_prefix = 0;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
// 如果这个标志为True，那么请不要破坏现有的数据库，如果你设置了该标志，且用一个测试工具测试一个新的数据库，那测试程序会失败
static bool FLAGS_use_existing_db = false;

// If true, reuse existing log/MANIFEST files when re-opening a database.
static bool FLAGS_reuse_logs = false;

// If true, use compression.
static bool FLAGS_compression = true;

// Use the db with the following name.
static const char* FLAGS_db = nullptr;

static  int FLAGS_key_num_perseg = 5;

namespace leveldb {

namespace {
leveldb::Env* g_env = nullptr;

class CountComparator : public Comparator {
 public:
  CountComparator(const Comparator* wrapped) : wrapped_(wrapped) {}
  ~CountComparator() override {}
  int Compare(const Slice& a, const Slice& b) const override {
    count_.fetch_add(1, std::memory_order_relaxed);
    return wrapped_->Compare(a, b);
  }
  const char* Name() const override { return wrapped_->Name(); }
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    wrapped_->FindShortestSeparator(start, limit);
  }

  void FindShortSuccessor(std::string* key) const override {
    return wrapped_->FindShortSuccessor(key);
  }

  size_t comparisons() const { return count_.load(std::memory_order_relaxed); }

  void reset() { count_.store(0, std::memory_order_relaxed); }

 private:
  mutable std::atomic<size_t> count_{0};
  const Comparator* const wrapped_;
};

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }
  // 提取data_中pos_后的长度为len的字符串，并生成Slice
  Slice Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};

class KeyBuffer {
 public:
  KeyBuffer() {
    assert(FLAGS_key_prefix < sizeof(buffer_));
    memset(buffer_, 'a', FLAGS_key_prefix);
  }
  KeyBuffer& operator=(KeyBuffer& other) = delete;  //禁止使用该函数
  KeyBuffer(KeyBuffer& other) = delete;   //禁止使用该函数
  //设置MyString对象表示的以0结尾的C字符串。
  void Set(int k) {
    // 函数在格式化字符串时，可以避免缓冲区溢出。 如果格式化后的字符串的长度超过了 size-1 ，
    // 则 snprintf() 函数只会写入 size-1 个字符，并在字符串的末尾添加一个空字符（ \0 ）以表示字符串的结束。
    // 将k写入buffer中
    std::snprintf(buffer_ + FLAGS_key_prefix,
                  sizeof(buffer_) - FLAGS_key_prefix, "%016d", k);
  }

  Slice slice() const { return Slice(buffer_, FLAGS_key_prefix + 16); }

 private:
  char buffer_[1024];
};

#if defined(__linux)
static Slice TrimSpace(Slice s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    start++;
  }
  size_t limit = s.size();
  while (limit > start && isspace(s[limit - 1])) {
    limit--;
  }
  return Slice(s.data() + start, limit - start);
}
#endif

static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');   // 为什么要加‘  ’？？？
  }
  str->append(msg.data(), msg.size());
}

class Stats {
 private:
  double start_;
  double finish_;
  double seconds_;
  int done_;
  int next_report_;
  int64_t bytes_;
  double last_op_finish_;
  Histogram hist_;
  std::string message_;

 public:
  Stats() { Start(); }
  // 初始化
  void Start() {
    next_report_ = 100;
    hist_.Clear();
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    message_.clear();
    start_ = finish_ = last_op_finish_ = g_env->NowMicros();
  }

  void Merge(const Stats& other) {
    hist_.Merge(other.hist_);
    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = g_env->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) { AppendWithSpace(&message_, msg); }

  // 什么意思，完成一次操作？？？
  void FinishedSingleOp() {
    if (FLAGS_histogram) {
      double now = g_env->NowMicros();
      double micros = now - last_op_finish_;  // 微秒数
      hist_.Add(micros);
      if (micros > 20000) {
        std::fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
        std::fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_++;
    if (done_ >= next_report_) {
      if (next_report_ < 1000)
        next_report_ += 100;
      else if (next_report_ < 5000)
        next_report_ += 500;
      else if (next_report_ < 10000)
        next_report_ += 1000;
      else if (next_report_ < 50000)
        next_report_ += 5000;
      else if (next_report_ < 100000)
        next_report_ += 10000;
      else if (next_report_ < 500000)
        next_report_ += 50000;
      else
        next_report_ += 100000;
      std::fprintf(stderr, "... finished %d ops%30s\r", done_, "");
      std::fflush(stderr);
    }
  }

  void AddBytes(int64_t n) { bytes_ += n; }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    // 假装至少完成了一个OP，以防我们正在运行一个不会调用FinishedsingLeop()的基准测试。
    if (done_ < 1) done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      //速率是根据实际运行时间计算的，而不是每线程/运行时间的总和。
      double elapsed = (finish_ - start_) * 1e-6;
      char rate[100];
      std::snprintf(rate, sizeof(rate), "%6.1f MB/s",
                    (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);

    std::fprintf(stdout, "%-12s : %11.3f micros/op;%s%s\n",
                 name.ToString().c_str(), seconds_ * 1e6 / done_,
                 (extra.empty() ? "" : " "), extra.c_str());
    if (FLAGS_histogram) {
      std::fprintf(stdout, "Microseconds per op:\n%s\n",
                   hist_.ToString().c_str());
    }
    //冲洗流中的信息，该函数通常用于处理磁盘文件。 fflush()会强迫将缓冲区内的数据写回参数stream 指定的文件中。
    std::fflush(stdout);
  }
};

// State shared by all concurrent executions of the same benchmark.
//所有同时执行同一基准的State。
struct SharedState {
  port::Mutex mu;
  // 声明数据成员受给定功能保护。对数据的读取操作需要共享访问，而写入操作需要独占访问。
  port::CondVar cv GUARDED_BY(mu);
  int total GUARDED_BY(mu);

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  int num_initialized GUARDED_BY(mu);
  int num_done GUARDED_BY(mu);
  bool start GUARDED_BY(mu);

  SharedState(int total)
      : cv(&mu), total(total), num_initialized(0), num_done(0), start(false) {}
};

// Per-thread state for concurrent executions of the same benchmark.
//每个线程状态，用于并发执行同一基准。
struct ThreadState {
  int tid;      // 0..n-1 when running in n threads
  Random rand;  // Has different seeds for different threads
  Stats stats;
  SharedState* shared;

  ThreadState(int index, int seed) : tid(index), rand(seed), shared(nullptr) {}
};

}  // namespace

class Benchmark {
 private:
  Cache* cache_;
  const FilterPolicy* filter_policy_;
  DB* db_;
  int num_;
  int value_size_;
  int entries_per_batch_;
  WriteOptions write_options_;
  int reads_;
  int heap_counter_;
  CountComparator count_comparator_;
  int total_thread_count_;
  Remix *sorted_view_;

  void PrintHeader() {
    const int kKeySize = 16 + FLAGS_key_prefix;
    PrintEnvironment();
    std::fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
    std::fprintf(
        stdout, "Values:     %d bytes each (%d bytes after compression)\n",
        FLAGS_value_size,
        static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));  //static_cast是一个c++运算符，功能是把一个表达式转换为某种类型，但没有运行时类型检查来保证转换的安全性
    std::fprintf(stdout, "Entries:    %d\n", num_);
    std::fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
                 ((static_cast<int64_t>(kKeySize + FLAGS_value_size) * num_) /
                  1048576.0));
    std::fprintf(
        stdout, "FileSize:   %.1f MB (estimated)\n",
        (((kKeySize + FLAGS_value_size * FLAGS_compression_ratio) * num_) /
         1048576.0));
    std::fprintf(
        stdout, "Keys num per seg:   %d \n",
        ((FLAGS_key_num_perseg) /
         1048576.0));
    PrintWarnings();
    std::fprintf(stdout, "------------------------------------------------\n");
  }

  void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    std::fprintf(
        stdout, //警告：优化被禁用：基准测试不必要地慢；
        "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
    std::fprintf(
        stdout,
        "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
    // 查看snappy是否通过压缩可压缩字符来工作，且是否有效
    // See if snappy is working by attempting to compress a compressible string
    const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
    std::string compressed;
    if (!port::Snappy_Compress(text, sizeof(text), &compressed)) {
      std::fprintf(stdout, "WARNING: Snappy compression is not enabled\n");
    } else if (compressed.size() >= sizeof(text)) {
      std::fprintf(stdout, "WARNING: Snappy compression is not effective\n");
    }
  }

  void PrintEnvironment() {
    std::fprintf(stderr, "LevelDB:    version %d.%d\n", kMajorVersion,
                 kMinorVersion);

#if defined(__linux)
    time_t now = time(nullptr);
    std::fprintf(stderr, "Date:       %s",
                 ctime(&now));  // ctime() adds newline

    FILE* cpuinfo = std::fopen("/proc/cpuinfo", "r");
    if (cpuinfo != nullptr) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
        const char* sep = strchr(line, ':');
        if (sep == nullptr) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      std::fclose(cpuinfo);
      std::fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      std::fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

 public:
  Benchmark()
      : cache_(FLAGS_cache_size >= 0 ? NewLRUCache(FLAGS_cache_size) : nullptr),
        filter_policy_(FLAGS_bloom_bits >= 0
                           ? NewBloomFilterPolicy(FLAGS_bloom_bits)
                           : nullptr),
        db_(nullptr),
        num_(FLAGS_num),
        value_size_(FLAGS_value_size),
        entries_per_batch_(1),
        reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
        heap_counter_(0),
        count_comparator_(BytewiseComparator()),
        total_thread_count_(0) ,
        sorted_view_(nullptr){
    std::vector<std::string> files;
    g_env->GetChildren(FLAGS_db, &files);
    for (size_t i = 0; i < files.size(); i++) {
      if (Slice(files[i]).starts_with("heap-")) {
        g_env->RemoveFile(std::string(FLAGS_db) + "/" + files[i]);
      }
    }
    if (!FLAGS_use_existing_db) {
      DestroyDB(FLAGS_db, Options());
    }
  }

  ~Benchmark() {
    delete db_;
    delete cache_;
    delete filter_policy_;
  }

  void Run() {
    PrintHeader();
    Open();

    const char* benchmarks = FLAGS_benchmarks;
    while (benchmarks != nullptr) {
      // strchr（）用于查找字符串中的一个字符，并返回该字符在字符串中第一次出现的位置。
      const char* sep = strchr(benchmarks, ',');
      Slice name;
      if (sep == nullptr) {
        name = benchmarks;
        benchmarks = nullptr;
      } else {
        name = Slice(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }

      // Reset parameters that may be overridden below
      num_ = FLAGS_num;
      reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
      value_size_ = FLAGS_value_size;
      entries_per_batch_ = 1;
      write_options_ = WriteOptions();

      void (Benchmark::*method)(ThreadState*) = nullptr;  // 这是一个指针变量，指向一个函数，该函数一定在BenchMark类中，且参数一定是ThreadState*类型
      bool fresh_db = false;
      int num_threads = FLAGS_threads;

      if (name == Slice("open")) {  
        method = &Benchmark::OpenBench;
        num_ /= 10000;
        if (num_ < 1) num_ = 1;
      } else if (name == Slice("fillseq")) {
        fresh_db = true;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillbatch")) {
        fresh_db = true;
        entries_per_batch_ = 1000;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillrandom")) {
        fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("overwrite")) {
        fresh_db = false;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fillsync")) {
        fresh_db = true;
        num_ /= 1000;
        write_options_.sync = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fill100K")) {
        fresh_db = true;
        num_ /= 1000;
        value_size_ = 100 * 1000;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("readseq_Leveldb")) {   // 222222
        method = &Benchmark::ReadIter;
      } else if (name == Slice("50 seek+next op with leveldb")) {   // 222222
        method = &Benchmark::_50sn_Leveldb;
      } else if (name == Slice("50 seek+next op with Remix")) {   // 222222
        method = &Benchmark::_50sn_Remix;
      } else if (name == Slice("single seek+next op with leveldb")) {   // 222222
        method = &Benchmark::Single_sn_Leveldb;
      } else if (name == Slice("single seek+next op with Remix")) {   // 222222
        method = &Benchmark::Single_sn_Remix;
      }else if (name == Slice("readseq_Remix")) {
        method = &Benchmark::ReadSequential;
      } else if (name == Slice("create view")) {
        method = &Benchmark::CreateView;
      } else if (name == Slice("readreverse")) {
        method = &Benchmark::ReadReverse;
      } else if (name == Slice("readrandom")) {
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("readmissing")) {
        method = &Benchmark::ReadMissing;
      } else if (name == Slice("seekrandom")) {
        method = &Benchmark::SeekRandom;
      } else if (name == Slice("seekordered")) {
        method = &Benchmark::SeekOrdered;
      } else if (name == Slice("readhot")) {
        method = &Benchmark::ReadHot;
      } else if (name == Slice("readrandomsmall")) {
        reads_ /= 1000;
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("deleteseq")) {
        method = &Benchmark::DeleteSeq;
      } else if (name == Slice("deleterandom")) {
        method = &Benchmark::DeleteRandom;
      } else if (name == Slice("readwhilewriting")) {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileWriting;
      } else if (name == Slice("compact")) {
        method = &Benchmark::Compact;
      } else if (name == Slice("crc32c")) {
        method = &Benchmark::Crc32c;
      } else if (name == Slice("snappycomp")) {
        method = &Benchmark::SnappyCompress;
      } else if (name == Slice("snappyuncomp")) {
        method = &Benchmark::SnappyUncompress;
      } else if (name == Slice("heapprofile")) {
        HeapProfile();
      } else if (name == Slice("stats")) {
        PrintStats("leveldb.stats");
      } else if (name == Slice("sstables")) {
        PrintStats("leveldb.sstables");
      } else {
        if (!name.empty()) {  // No error message for empty name
          std::fprintf(stderr, "unknown benchmark '%s'\n",
                       name.ToString().c_str());
        }
      }

      if (fresh_db) {
        if (FLAGS_use_existing_db) {
          std::fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                       name.ToString().c_str());
          method = nullptr;
        } else {
          delete db_;
          db_ = nullptr;
          DestroyDB(FLAGS_db, Options());
          Open();
        }
      }

      if (method != nullptr) {
        RunBenchmark(num_threads, name, method);
      }
    }
  }

 private:
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;  //所有同时执行同一基准的State。
    ThreadState* thread; // 每个线程状态，用于并发执行同一基准。
    void (Benchmark::*method)(ThreadState*);  // 该线程对应的任务
  };

// 999
  static void ThreadBody(void* v) {
    // reinterpret_cast运算符是用来处理无关类型之间的转换；它会产生一个新的值，这个值会有与原始参数（expressoin）有完全相同的比特位。
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }
      while (!shared->start) {
        shared->cv.Wait();
      }
    }

    thread->stats.Start();
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      // 这是为什么？？？做完了居然还要SignalALL？？？
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
  }
  // 产生n个线程，每个线程运行method所指的函数，
  void RunBenchmark(int n, Slice name,
                    void (Benchmark::*method)(ThreadState*)) {
    SharedState shared(n);

    ThreadArg* arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      ++total_thread_count_;
      // Seed the thread's random state deterministically based upon thread
      // creation across all benchmarks. This ensures that the seeds are unique
      // but reproducible when rerunning the same set of benchmarks.
     // 种子线程在所有基准测试中基于线程创建确定的随机状态。
     // 这确保了seed是唯一的,但在重新运行同一组基准时是可重复的。
      arg[i].thread = new ThreadState(i, /*seed=*/1000 + total_thread_count_);
      arg[i].thread->shared = &shared;
      g_env->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    // 要等到所有的线程都初始化了才能继续
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }

    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    shared.mu.Unlock();

    for (int i = 1; i < n; i++) {
      arg[0].thread->stats.Merge(arg[i].thread->stats);
    }
    arg[0].thread->stats.Report(name);
    if (FLAGS_comparisons) {
      fprintf(stdout, "Comparisons: %zu\n", count_comparator_.comparisons());
      count_comparator_.reset();
      fflush(stdout);
    }

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;
  }

  void Crc32c(ThreadState* thread) {
    // Checksum about 500MB of data total
    // 500MB数据的CRC检验和
    const int size = 4096;
    const char* label = "(4K per op)";
    std::string data(size, 'x');
    int64_t bytes = 0;
    uint32_t crc = 0;
    while (bytes < 500 * 1048576) {
      crc = crc32c::Value(data.data(), size);
      thread->stats.FinishedSingleOp();
      bytes += size;
    }
    // Print so result is not dead
    std::fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(label);
  }
  // Snappy模式的compress
  void SnappyCompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    int64_t bytes = 0;
    int64_t produced = 0;
    bool ok = true;
    std::string compressed;
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
      produced += compressed.size();
      bytes += input.size();
      thread->stats.FinishedSingleOp();
    }

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "(output: %.1f%%)",
                    (produced * 100.0) / bytes);
      thread->stats.AddMessage(buf);
      thread->stats.AddBytes(bytes);
    }
  }

  void SnappyUncompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    std::string compressed;
    // 这个OK是为了什么？？？为什么要用SnappyCompress
    bool ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
    int64_t bytes = 0;
    char* uncompressed = new char[input.size()];
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok = port::Snappy_Uncompress(compressed.data(), compressed.size(),
                                   uncompressed);
      bytes += input.size();
      thread->stats.FinishedSingleOp();
    }
    delete[] uncompressed;

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      thread->stats.AddBytes(bytes);
    }
  }

  void Open() {
    assert(db_ == nullptr);
    Options options;
    options.env = g_env;
    options.create_if_missing = !FLAGS_use_existing_db;
    //options.block_cache = cache_;
    options.block_cache = nullptr;
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_file_size = FLAGS_max_file_size;
    options.block_size = FLAGS_block_size;
    if (FLAGS_comparisons) {
      options.comparator = &count_comparator_;
    }
    options.max_open_files = FLAGS_open_files;
    options.filter_policy = filter_policy_;
    options.reuse_logs = FLAGS_reuse_logs;
    options.compression =
        FLAGS_compression ? kSnappyCompression : kNoCompression;
    Status s = DB::Open(options, FLAGS_db, &db_);
    if (!s.ok()) {
      std::fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      std::exit(1);
    }
  }

  void OpenBench(ThreadState* thread) {
    for (int i = 0; i < num_; i++) {
      delete db_;
      Open();
      thread->stats.FinishedSingleOp();
    }
  }

  void WriteSeq(ThreadState* thread) { DoWrite(thread, true); }

  void WriteRandom(ThreadState* thread) { DoWrite(thread, false); }

  void DoWrite(ThreadState* thread, bool seq) {
    if (num_ != FLAGS_num) {
      char msg[100];
      std::snprintf(msg, sizeof(msg), "(%d ops)", num_);
      thread->stats.AddMessage(msg);
    }

    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    int64_t bytes = 0;
    KeyBuffer key;
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        const int k = seq ? i + j : thread->rand.Uniform(FLAGS_num);
        key.Set(k);
        batch.Put(key.slice(), gen.Generate(value_size_));
        bytes += value_size_ + key.slice().size();
        thread->stats.FinishedSingleOp();
      }
      s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        std::fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        std::exit(1);
      }
    }
    thread->stats.AddBytes(bytes);
  }
  // 顺序读，这个用迭代器实现顺序读 7777
  void ReadSequential(ThreadState* thread) {
    Iterator* iter = sorted_view_->NewIterator();
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp();
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }
  void ReadIter(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp();
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }
    void CreateView(ThreadState* thread) {
    if(sorted_view_ != NULL) delete sorted_view_;
    sorted_view_ = new Remix(db_,FLAGS_key_num_perseg);
    thread->stats.FinishedSingleOp();
  }
  void _50sn_Leveldb(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    KeyBuffer key;
    //key.Set(thread->rand.Uniform(FLAGS_num/4) + FLAGS_num-FLAGS_num/4);
    key.Set(thread->rand.Uniform(FLAGS_num));
    //cout << key.slice().ToString() << endl;
    for(iter->Seek(key.slice()); iter->Valid()&&i < 50; i++,iter->Next()){
      if(i == 0) cout << iter->key().ToString() << endl;
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp();
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }
  void _50sn_Remix(ThreadState* thread) {
    Iterator* iter = sorted_view_->NewIterator();
    int i = 0;
    int64_t bytes = 0;
    KeyBuffer key;
    //const int range = (FLAGS_num + FLAGS_num/2) / 100;
    key.Set(thread->rand.Uniform(FLAGS_num));
    //key.Set(thread->rand.Uniform(FLAGS_num/4) + FLAGS_num-FLAGS_num/4);
    for (iter->Seek(key.slice()); iter->Valid()&&i < 50; i++, iter->Next()) {
      if(i == 0) cout << iter->key().ToString() << endl;
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp();
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }
    void Single_sn_Leveldb(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    iter->SeekToFirst();
    //cout << iter->key().ToString() << endl;
    //int64_t bytes = 0;
    //bytes += iter->key().size() + iter->value().size();
    iter->Next();
    //bytes += iter->key().size() + iter->value().size();
    thread->stats.FinishedSingleOp();
    delete iter;
   // thread->stats.AddBytes(bytes);
  }
  void Single_sn_Remix(ThreadState* thread) {
    Iterator* iter = sorted_view_->NewIterator();
    iter->SeekToFirst();
    //int64_t bytes = 0;
    //bytes += iter->key().size() + iter->value().size();
    iter->Next();
    //bytes += iter->key().size() + iter->value().size();
    thread->stats.FinishedSingleOp();
    delete iter;
    //thread->stats.AddBytes(bytes);
  }
  // 这个也是用迭代器实现的 7777
  void ReadReverse(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToLast(); i < reads_ && iter->Valid(); iter->Prev()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp();
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void ReadRandom(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    int found = 0;
    KeyBuffer key;
    for (int i = 0; i < reads_; i++) {
      const int k = thread->rand.Uniform(FLAGS_num);
      key.Set(k);
      if (db_->Get(options, key.slice(), &value).ok()) {
        found++;
      }
      thread->stats.FinishedSingleOp();
    }
    char msg[100];
    std::snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }
  // 以随机顺序读取 N 个丢失的键
  void ReadMissing(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    KeyBuffer key;
    for (int i = 0; i < reads_; i++) {
      const int k = thread->rand.Uniform(FLAGS_num);
      key.Set(k);
      Slice s = Slice(key.slice().data(), key.slice().size() - 1);
      db_->Get(options, s, &value);
      thread->stats.FinishedSingleOp();
    }
  }
  // 从 DB 的 1% 部分以随机顺序读取 N 次
  void ReadHot(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    const int range = (FLAGS_num + 99) / 100;
    KeyBuffer key;
    for (int i = 0; i < reads_; i++) {
      const int k = thread->rand.Uniform(range);
      key.Set(k);
      db_->Get(options, key.slice(), &value);
      thread->stats.FinishedSingleOp();
    }
  }

  void SeekRandom(ThreadState* thread) {
    ReadOptions options;
    int found = 0;
    KeyBuffer key;
    for (int i = 0; i < reads_; i++) {
      Iterator* iter = db_->NewIterator(options);
      const int k = thread->rand.Uniform(FLAGS_num);
      key.Set(k);
      iter->Seek(key.slice());
      if (iter->Valid() && iter->key() == key.slice()) found++;
      delete iter;
      thread->stats.FinishedSingleOp();
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void SeekOrdered(ThreadState* thread) {
    ReadOptions options;
    Iterator* iter = db_->NewIterator(options);
    int found = 0;
    int k = 0;
    KeyBuffer key;
    for (int i = 0; i < reads_; i++) {
      k = (k + (thread->rand.Uniform(100))) % FLAGS_num;
      key.Set(k);
      iter->Seek(key.slice());
      if (iter->Valid() && iter->key() == key.slice()) found++;
      thread->stats.FinishedSingleOp();
    }
    delete iter;
    char msg[100];
    std::snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void DoDelete(ThreadState* thread, bool seq) {
    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    KeyBuffer key;
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        const int k = seq ? i + j : (thread->rand.Uniform(FLAGS_num));
        key.Set(k);
        batch.Delete(key.slice());
        thread->stats.FinishedSingleOp();
      }
      s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        std::fprintf(stderr, "del error: %s\n", s.ToString().c_str());
        std::exit(1);
      }
    }
  }

  void DeleteSeq(ThreadState* thread) { DoDelete(thread, true); }

  void DeleteRandom(ThreadState* thread) { DoDelete(thread, false); }

  void ReadWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
      ReadRandom(thread);
    } else {
      // Special thread that keeps writing until other threads are done.
      // 某线程一直写到其他线程完成为止。

      RandomGenerator gen;
      KeyBuffer key;
      while (true) {
        {
          MutexLock l(&thread->shared->mu);
          if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
            // Other threads have finished
            break;
          }
        }

        const int k = thread->rand.Uniform(FLAGS_num);
        key.Set(k);
        Status s =
            db_->Put(write_options_, key.slice(), gen.Generate(value_size_));
        if (!s.ok()) {
          std::fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          std::exit(1);
        }
      }

      // Do not count any of the preceding work/delay in stats.
      // 不要在统计中计算前面的任何工作/延迟。
      thread->stats.Start();
    }
  }

  void Compact(ThreadState* thread) { db_->CompactRange(nullptr, nullptr); }

  void PrintStats(const char* key) {
    std::string stats;
    if (!db_->GetProperty(key, &stats)) {
      stats = "(failed)";
    }
    std::fprintf(stdout, "\n%s\n", stats.c_str());
  }

  static void WriteToFile(void* arg, const char* buf, int n) {
    reinterpret_cast<WritableFile*>(arg)->Append(Slice(buf, n));
  }
  // 这个什么意思？？？
  // 堆配置文件
  void HeapProfile() {
    char fname[100];
    std::snprintf(fname, sizeof(fname), "%s/heap-%04d", FLAGS_db,
                  ++heap_counter_);
    WritableFile* file;
    Status s = g_env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      std::fprintf(stderr, "%s\n", s.ToString().c_str());
      return;
    }
    // 一定返回false为啥？？？
    bool ok = port::GetHeapProfile(WriteToFile, file);
    delete file;
    if (!ok) {
      std::fprintf(stderr, "heap profiling not supported\n");
      g_env->RemoveFile(fname);
    }
  }
};

}  // namespace leveldb

int main(int argc, char** argv) {
  FLAGS_write_buffer_size = leveldb::Options().write_buffer_size;
  FLAGS_max_file_size = leveldb::Options().max_file_size;
  FLAGS_block_size = leveldb::Options().block_size;
  FLAGS_open_files = leveldb::Options().max_open_files;
  std::string default_db_path;

  for (int i = 1; i < argc; i++) {
    double d;
    int n;
    char junk;
    bool flag = false;
    if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
    } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
      FLAGS_compression_ratio = d;
    } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_histogram = n;
    } else if (sscanf(argv[i], "--comparisons=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_comparisons = n;
    } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_use_existing_db = n;
    } else if (sscanf(argv[i], "--reuse_logs=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_reuse_logs = n;
    } else if (sscanf(argv[i], "--compression=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_compression = n;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
      FLAGS_reads = n;
    } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
      FLAGS_threads = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    } else if (sscanf(argv[i], "--max_file_size=%d%c", &n, &junk) == 1) {
      FLAGS_max_file_size = n;
    } else if (sscanf(argv[i], "--block_size=%d%c", &n, &junk) == 1) {
      FLAGS_block_size = n;
    } else if (sscanf(argv[i], "--key_prefix=%d%c", &n, &junk) == 1) {
      FLAGS_key_prefix = n;
    } else if (sscanf(argv[i], "--cache_size=%d%c", &n, &junk) == 1) {
      FLAGS_cache_size = n;
    } else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
      FLAGS_bloom_bits = n;
    } else if (sscanf(argv[i], "--open_files=%d%c", &n, &junk) == 1) {
      FLAGS_open_files = n;
    } else if (strncmp(argv[i], "--db=", 5) == 0) {
      FLAGS_db = argv[i] + 5;
    } else if (sscanf(argv[i], "--num_seg=%d%c", &n, &junk) == 1) {
      FLAGS_key_num_perseg = n;
      flag = true;
    } else {
      std::fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      std::exit(1);
    }
    if(!flag){
      FLAGS_key_num_perseg = FLAGS_key_num_perseg > 100000? FLAGS_key_num_perseg/2000: FLAGS_key_num_perseg;
    }
  }

  leveldb::g_env = leveldb::Env::Default();

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db == nullptr) {
    leveldb::g_env->GetTestDirectory(&default_db_path);
    default_db_path += "/dbbench";
    FLAGS_db = default_db_path.c_str();
  }


  leveldb::Benchmark benchmark;
  benchmark.Run();
  return 0;
}
