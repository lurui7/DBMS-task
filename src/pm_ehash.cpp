#include"pm_ehash.h"

bool if_has_free_slot(pm_bucket *bucket)
{
    for(int i = 0; i < 7; ++i)
    {
        if(!((bucket->bitmap[0] >> i) & 1))
            return true;
    }
    for(int i = 0; i < 6; ++i)      //bitmap[1]最后一位不对应槽    {
    {
        if(!((bucket->bitmap[1] >> i) & 1))
            return true;
    }
    return false;
}

/**
 * @description: construct a new instance of PmEHash in a default directory
 * @param NULL
 * @return: new instance of PmEHash
 */
PmEHash::PmEHash() {
    if()
    {
        
    }
    else
    {
        recover();
        mapAllPage();
    }
}
/**
 * @description: persist and munmap all data in NVM
 * @param NULL 
 * @return: NULL
 */
PmEHash::~PmEHash() {

}

/**
 * @description: 插入新的键值对，并将相应位置上的位图置1
 * @param kv: 插入的键值对
 * @return: 0 = insert successfully, -1 = fail to insert(target data with same key exist)
 */
int PmEHash::insert(kv new_kv_pair) {
    uint64_t val;
    if(!search(new_kv_pair.key, val))   //先检查此键值对是否已存在
        return -1;
    else
    {
        pm_bucket * bucket = getFreeBucket(new_kv_pair.key);
        int i, j;             //i选择bitmap[0]或bitmap[1]，j选择bitmap[i]中的第几个bit
        int stop_for = 0;
        for(i = 0; i < 1; ++i)
        {
            for(j = 0; j < 7; ++j)
            {
                if(!((bucket->bitmap[i] >> j) & 1))
                {
                    stop_for = 1;
                    break;
                }
            }
            if(stop_for)
                break;
        }
        int slot_num = i * 8 + j;  //根据i和j算出第几个槽是空的
        bucket->slot[slot_num].key = new_kv_pair.key;
        bucket->slot[slot_num].value = new_kv_pair.value;
        bucket->bitmap[i] |= (1 << j);        //位图中对应位置1
        return 0;
    }
}

/**
 * @description: 删除具有目标键的键值对数据，不直接将数据置0，而是将相应位图置0即可
 * @param uint64_t: 要删除的目标键值对的键
 * @return: 0 = removing successfully, -1 = fail to remove(target data doesn't exist)
 */
int PmEHash::remove(uint64_t key) {
    uint64_t val;
    if(search(key, val) == -1)
        return -1;
    else
    {
        uint64_t bucket_num = hashFunc(key);
        pm_bucket * bucket = catalog.buckets_virtual_address[bucket_num];
        for(int  i = 0; i < BUCKET_SLOT_NUM; ++i)
        {
            if(bucket->slot[i].key == key)
            {
                int offset = i % 8;
                bucket->bitmap[i / 8] &= (~(1 << offset));  //位图中对应位置0
                break;
            }
        }
        if(bucket->bitmap[0] == 0 && bucket->bitmap[1] == 0)   //如果删除后使得桶变空，则需要回收空桶
            mergeBucket(bucket_num);
        return 0;
    }
}
/**
 * @description: 更新现存的键值对的值
 * @param kv: 更新的键值对，有原键和新值
 * @return: 0 = update successfully, -1 = fail to update(target data doesn't exist)
 */
int PmEHash::update(kv kv_pair) {
    uint64_t val;
    if(search(kv_pair.key, val) == -1)
        return -1;
    else
    {
        uint64_t bucket_num = hashFunc(kv_pair.key);
        pm_bucket * bucket = catalog.buckets_virtual_address[bucket_num];
        for(int  i = 0; i < BUCKET_SLOT_NUM; ++i)
        {
            if(bucket->slot[i].key == kv_pair.key)
            {
                bucket->slot[i].value = kv_pair.value;
                break;
            }
        }
        return 0;
    }
}
/**
 * @description: 查找目标键值对数据，将返回值放在参数里的引用类型进行返回
 * @param uint64_t: 查询的目标键
 * @param uint64_t&: 查询成功后返回的目标值
 * @return: 0 = search successfully, -1 = fail to search(target data doesn't exist) 
 */
int PmEHash::search(uint64_t key, uint64_t& return_val) {
    int64_t bucket_num = hashFunc(key);
    pm_bucket * bucket = catalog.buckets_virtual_address[bucket_num];
    for(int  i = 0; i < BUCKET_SLOT_NUM; ++i)
    {
        if(bucket->slot[i].key == key)
        {
            return_val = bucket->slot[i].value;
            return 0;
        }
    }
    return -1;
}

/**
 * @description: 用于对输入的键产生哈希值，然后取模求桶号(自己挑选合适的哈希函数处理)
 * @param uint64_t: 输入的键
 * @return: 返回键所属的桶号
 */
uint64_t PmEHash::hashFunc(uint64_t key) {
    uint64_t hash_value = key % (1 << metadata->global_depth);  //直接取键的后global_depth位作为哈希值
    return hash_value;
}

/**
 * @description: 获得供插入的空闲的桶，无空闲桶则先分裂桶然后再返回空闲的桶
 * @param uint64_t: 带插入的键
 * @return: 空闲桶的虚拟地址
 */
pm_bucket* PmEHash::getFreeBucket(uint64_t key) {
    uint64_t bucket_num = hashFunc(key);
    pm_bucket * bucket = catalog.buckets_virtual_address[bucket_num];
    if(!if_has_free_slot(bucket))
    {
        splitBucket(bucket_num);
        bucket_num = hashFunc(key);  //重新哈希找桶
        bucket = catalog.buckets_virtual_address[bucket_num];
    }
    return bucket;
}

/**
 * @description: 获得空闲桶内第一个空闲的位置供键值对插入
 * @param pm_bucket* bucket
 * @return: 空闲键值对位置的虚拟地址
 */
kv* PmEHash::getFreeKvSlot(pm_bucket* bucket) {
    kv * freeslot;
    int i, j;     //i选择bitmap[0]或bitmap[1]，j选择bitmap[i]中的第几个bit
    int stop_for = 0;
    for(i = 0; i < 1; ++i)
    {
        for(j = 0; j < 7; ++j)
        {
            if(!((bucket->bitmap[i] >> j) & 1))
            {
                stop_for = 1;
                break;
            }
        }
        if(stop_for)
            break;
    }
    int slot_num = i * 8 + j;  //根据i和j算出第几个槽是空的
    freeslot = & bucket->slot[slot_num];
    return freeslot;
}

/**
 * @description: 桶满后进行分裂操作，可能触发目录的倍增
 * @param uint64_t: 目标桶在目录中的序号
 * @return: NULL
 */
void PmEHash::splitBucket(uint64_t bucket_id) {
    pm_bucket * full_bucket = catalog.buckets_virtual_address[bucket_id];
    ++ catalog.buckets_virtual_address[bucket_id]->local_depth;
    if(catalog.buckets_virtual_address[bucket_id]->local_depth > metadata->global_depth)
        extendCatalog();          //桶的局部深度大于全局深度时要倍增目录
    pm_bucket * new_bucket;
    if(free_list.empty())
    {
        pm_address address;
        getFreeSlot(address);
        new_bucket = pmAddr2vAddr[address];
    }
    else
    {
        new_bucket = free_list.front(); //先取出队列头部元素再将其删除
        free_list.pop();
    }
    new_bucket->bitmap[0] = 0;
    new_bucket->bitmap[1] = 0;
    new_bucket->local_depth = full_bucket->local_depth;
    kv old_data[15];
    for(int i = 0; i < 15; ++i)    //将已满桶的记录先复制下来
    {
        old_data[i].key = full_bucket->slot[i].key;
        old_data[i].value = full_bucket->slot[i].value;
    }
    full_bucket->bitmap[0] = 0;
    full_bucket->bitmap[1] = 0;    //将位图全部置0
    for(int i = 0; i < 15; ++i)
        insert(old_data[i]);       //重新分布已满桶中的记录
}

/**
 * @description: 桶空后，回收桶的空间，并设置相应目录项指针
 * @param uint64_t: 桶号
 * @return: NULL
 */
void PmEHash::mergeBucket(uint64_t bucket_id) {
    
}

/**
 * @description: 对目录进行倍增，需要重新生成新的目录文件并复制旧值，然后删除旧的目录文件
 * @param NULL
 * @return: NULL
 */
void PmEHash::extendCatalog() {

}

/**
 * @description: 获得一个可用的数据页的新槽位供哈希桶使用，如果没有则先申请新的数据页
 * @param pm_address&: 新槽位的持久化文件地址，作为引用参数返回
 * @return: 新槽位的虚拟地址
 */
void* PmEHash::getFreeSlot(pm_address& new_address) {

}

/**
 * @description: 申请新的数据页文件，并把所有新产生的空闲槽的地址放入free_list等数据结构中
 * @param NULL
 * @return: NULL
 */
void PmEHash::allocNewPage() {
    ++ metadata->max_file_id;
    
}

/**
 * @description: 读取旧数据文件重新载入哈希，恢复哈希关闭前的状态
 * @param NULL
 * @return: NULL
 */
void PmEHash::recover() {

}

/**
 * @description: 重启时，将所有数据页进行内存映射，设置地址间的映射关系，空闲的和使用的槽位都需要设置 
 * @param NULL
 * @return: NULL
 */
void PmEHash::mapAllPage() {

}

/**
 * @description: 删除PmEHash对象所有数据页，目录和元数据文件，主要供gtest使用。即清空所有可扩展哈希的文件数据，不止是内存上的
 * @param NULL
 * @return: NULL
 */
void PmEHash::selfDestory() {

}