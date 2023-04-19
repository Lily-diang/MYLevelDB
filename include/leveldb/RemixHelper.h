/*
 * @Author: Li_diang 787695954@qq.com
 * @Date: 2023-04-17 15:07:58
 * @LastEditors: Li_diang 787695954@qq.com
 * @LastEditTime: 2023-04-17 15:35:49
 * @FilePath: \leveldb\db\RemixHelpter.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */

#ifndef STORAGE_LEVELDB_INCLUDE_REMIX_H_
#define STORAGE_LEVELDB_INCLUDE_REMIX_H_

#include "leveldb/status.h"
#include "leveldb/export.h"
namespace leveldb{
    class LEVELDB_EXPORT Remix_Helper{
        public:
        Remix_Helper() : index_of_runs(-1) {}

        void set_index_of_runs (int index){
            assert(index >= 0);
            index_of_runs = index;
        }

        int get_index_of_runs (){
            return index_of_runs;
        }

        private:
        int index_of_runs;
    };
}  // namespace leveldb
#endif  // STORAGE_LEVELDB_INCLUDE_REMIX_H_