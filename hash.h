//
// Created by WenPan on 4/27/18.
//

#define NUM_BUCKET 99737
#define HT_PRIME_1 31
#define HT_PRIME_2 37
#define HASH_KEY_LEN 2
/*
 * a node of the hash table
 * */
typedef struct {
    char* key;  // partial key
    void* radix_pointer;    // change this to a pointer to a sub-tree (RADIX)
}ht_node;


/*ß
 * hash table header
 * */
typedef struct {
    int size;
    int count;
    ht_node** items;
    void * leaf_head;
} ht_header;


void * ht_insert(ht_header* ht, const char* key, void * sub_radix);
void * ht_search(ht_header* ht, const char* key, int8_t is_insert);
void ht_delete(ht_header* h, const char* key);
static ht_node * ht_new_item(const char* k, void * sub_radix);



