#ifndef _XT_MAP_H
#define _XT_MAP_H

#include <stdint.h> 

/* map entry descriptor */
typedef struct _me {
    uint32_t kl; // len of key                                                                         
    uint32_t vl; // len of value                                                                    
} __attribute__((packed)) xt_me;

/* map                                                                                              
 *                                                                                                     
 * Layout of this map:                                                                                 
 *                                                                                                  
 * +---------------+                                                                                   
 * | num           | 2B                                                                                
 * +---------------+                                                                                
 * | version       | 2B                                                                                
 * +---------------+                                                                                   
 * | total_size    | 4B                                                                             
 * +===============+                                                                                   
 * | kl 1          | 4B                                                                                
 * +------         |                                                                                
 * | vl 1          | 4B                                                                                
 * +---------------+                                                                                   
 * | key 1 ...     |                                                                                
 * ...           ...                                                                                   
 * +---------------+                                                                                   
 * | val 1 ...     |                                                                                
 * ...           ...                                                                                   
 * +===============+                                                                                   
 * ...           ...                                                                                
 * +===============+                                                                                   
 * | magic num     |                                                                                   
 * +---------------+                                                                                
 * */
typedef struct _map {
    uint16_t num; // number of k/v entries in this map                                              
    uint16_t version; // map version                                                                   
    uint32_t total_size; // whole map's size include size of user data and management data             
    xt_me entries[0]; // k/v entry descriptor array                                                 
} __attribute__((packed)) xt_map_t;

/* map k/v */
typedef struct _mi {
    void* start; // start address of this item                                                         
    uint32_t len; // len of this item                                                               
} xt_mi;
#endif
