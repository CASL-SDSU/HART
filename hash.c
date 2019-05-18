//
// Created by WenPan on 4/27/18.
//

/*
 * a node of the hash table
 * */

#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "hash.h"
#include "art.h"

/*
 * Create a new node for hash table
 * The strdup() function returns a pointer to a new string which is a
 * duplicate of the string s.  Memory for the new string is obtained
 * with malloc(3), and can be freed with free(3).
 * */
static ht_node* ht_new_item(const char* k, void * sub_radix) {
    ht_node * i = malloc(sizeof(ht_node));
    if (k!=NULL)
        i->key = strdup(k);
    else
        i->key = NULL;
    i->radix_pointer = sub_radix;
    return i;
}


/*
 * Create a new hash table
 * */
ht_header* ht_new(ht_header * ht, int node_num) {
    ht->size = node_num;
    ht->count = 0;
    /** items are pointers, space is created for pointers,
     * Values are set to NULL*/
    ht->items = calloc((size_t)ht->size, sizeof(ht_node *));
    for (int i = 0; i < ht->size; i++)
        ht->items[i] = ht_new_item(NULL, NULL);
    return ht;
}


/*
 * Deletion  function*/
static void ht_del_node(ht_node* i) {
    free(i->key);
    free(i->radix_pointer);
    free(i);
}


void ht_del_hash_table(ht_header* ht) {
    for (int i = 0; i < ht->size; i++) {
        ht_node* item = ht->items[i];
        if (item != NULL) {
            ht_node(item);
        }
    }
    free(ht->items);
    free(ht);
}


/* A hash funtion from
 * https://github.com/jamesroutley/write-a-hash-table/tree/master/03-hashing
 * a:
 * m： our desired bucket array length
 * */
//static int ht_hash(const char* s, const int a, const int m) {
//    long hash = 0;
//    const int len_s = strlen(s);
//    for (int i = 0; i < len_s; i++) {
//        hash += (long)pow(a, len_s - (i+1)) * s[i];
//        hash = hash % m;
//    }
//    return (int)hash;
//}


static int ht_hash(unsigned char *str, const int prime)
{

    int hash = prime;
    for (int i = 0; i < strlen(str); i++) {
        hash = hash*31 + str[i];
    }

    return hash;
}


/* Hash collisions handling
 * */
static int ht_get_hash(const char* s, const int num_buckets, const int attempt) {
    const int hash_a = ht_hash(s, HT_PRIME_1);
    if (attempt) {
        const int hash_b = ht_hash(s, HT_PRIME_2);
        const int hash = hash_a + attempt * hash_b;
        return hash % num_buckets;
    } else
        return  hash_a % num_buckets;
}


/**
 *
 *  Methods: insert, search, delete
 * */


/* Insertion
 * Get index by hash function
 * Hash collision handling is included */
void * ht_insert(ht_header* ht, const char* key, void * sub_radix) {
    ht_node* item = ht_new_item(key, sub_radix);
    int index = ht_get_hash(item->key, ht->size, 0);
    ht_node* cur_item = ht->items[index];
    int i = 1;
    // Search until find an empty slot
    while (cur_item-->key != NULL) {
        index = ht_get_hash(item->key, ht->size, i);
        cur_item = ht->items[index];
        i++;
    }
    ht->items[index] = item;
    ht->count++;
}


/*Items are always valid because it has been assigned valid pointers to a node
 * however, keys can still be NULL, means the node has never been used
 * is_insert = 1, always return a node for new insertion
 * is_insert -= 0, return NULL if no match found*/
void * ht_search_bak(ht_header* ht, const char* key, int8_t is_insert) {


    int index = ht_get_hash(key, ht->size, 0);
    ht_node* item = ht->items[index];

    art_tree * a = (art_tree *)(ht->items[99710]->radix_pointer);
    if (a->root != NULL)
        a = NULL;

    int i = 1;
    while (item != NULL) {
        /* if a key is NULL, mean it's a free node, return if insertion
         * If is search, means no record is found*/
        if (item->key == NULL && is_insert){
            item->key = strdup(key);
            return  item->radix_pointer;
        }

        if (item->key == NULL && !is_insert) {
            index = ht_get_hash(key, ht->size, i);
            item = ht->items[index];
            i++;
            continue;
        }

        if (strcmp(item->key, key) == 0) {
            return item->radix_pointer;
        }
        index = ht_get_hash(key, ht->size, i);
        item = ht->items[index];
        i++;
    }
    return NULL;
}

/* Serves for both insertiuon and search
 * 1: insertion: return a new/existing item
 * 2: search: return a existing/NULL */
void * ht_search(ht_header* ht, const char* key, int8_t is_insert) {
    int index = ht_get_hash(key, ht->size, 0);
    ht_node* item = ht->items[index];

    int i = 1;
    while (item->key != NULL) {
        if (strcmp(item->key, key) == 0) {
            return item->radix_pointer;
        }
        index = ht_get_hash(key, ht->size, i);
        item = ht->items[index];
        i++;
    }
    if (is_insert){
        art_tree * t = (art_tree *)malloc(sizeof(art_tree));
        art_tree_init2(t, ht->leaf_head);
        item->radix_pointer = t;
        item->key = strdup(key);
        // ht_insert(ht, key, (void *) t);
        return item->radix_pointer;

    }

    else
        return NULL;
}


// delete function
static ht_node HT_DELETED_ITEM = {NULL, NULL};


void ht_delete(ht_header* ht, const char* key) {
    int index = ht_get_hash(key, ht->size, 0);
    ht_node* item = ht->items[index];
    int i = 1;
    while (item != NULL) {
        if (item != &HT_DELETED_ITEM) {
            if (strcmp(item->key, key) == 0) {
                ht_del_node(item);
                ht->items[index] = &HT_DELETED_ITEM;
            }
        }
        index = ht_get_hash(key, ht->size, i);
        item = ht->items[index];
        i++;
    }
    ht->count--;
}
