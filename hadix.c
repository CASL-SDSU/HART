//
// Created by WenPan on 5/9/18.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "art.h"
#include "hash.h"
#include "flush_delay.h"

int extra_latency = 0;
int num_leafs = 0;
int num_node4 = 0;
int num_node16 = 0;
int num_node48 = 0;
int num_node256 = 0;

/*
 * Insert into hash-radix hybrid architecture
 * Currently we set hash key as the first 3 characters of a key
 * The rest of the keys goes to a radix tree
 * */

void* hadix_insert(ht_header* ht, const unsigned char *key, int key_len,
            uint64_t *value) {
    // variable length array is supported in c99f
    // First copy first 3 chars to a hash key;
    unsigned char hash_key[HASH_KEY_LEN + 1];
    unsigned char radix_key[key_len - HASH_KEY_LEN + 1];
    strncpy(hash_key, key, HASH_KEY_LEN);
    strncpy(radix_key, key + HASH_KEY_LEN, key_len - HASH_KEY_LEN);
    //Check if a sub-radix tree exists; if not, init a new one and insert into
    // the hash table
    art_tree * t = (art_tree *) ht_search(ht, hash_key, 1);
    if (t==NULL){
        t = (art_tree *)malloc(sizeof(art_tree));
        art_tree_init2(t, ht->leaf_head);
        ht_insert(ht, hash_key, (void *) t);
    }
    key_len = strlen(radix_key);

    return  art_insert(t, (unsigned char*)radix_key, key_len, value);
}


/* search fundtion of hadix tree*/
uint64_t * hadix_search(ht_header* ht, unsigned char* key, int key_len)
{
    unsigned char hash_key[HASH_KEY_LEN + 1];
    unsigned char radix_key[key_len - HASH_KEY_LEN + 1];
    strncpy(hash_key, key, HASH_KEY_LEN);
    strncpy(radix_key, key + HASH_KEY_LEN, key_len - HASH_KEY_LEN);

    art_tree * t = (art_tree *) ht_search(ht, hash_key, 0);
    if (t==NULL){
        return NULL;
    }
    uint64_t *val = (uint64_t*)art_search(t, (unsigned char*)radix_key,
                     key_len - HASH_KEY_LEN);
    return  val;
}


int  * hadix_delete(ht_header* ht, unsigned char* key, int key_len)
{
    unsigned char hash_key[HASH_KEY_LEN + 1];
    unsigned char radix_key[key_len - HASH_KEY_LEN + 1];
    strncpy(hash_key, key, HASH_KEY_LEN);
    strncpy(radix_key, key + HASH_KEY_LEN, key_len - HASH_KEY_LEN);

    art_tree * t = (art_tree *) ht_search(ht, hash_key, 0);
    return art_delete(t, radix_key,strlen(radix_key));
}

int alloc_trees(ht_header * new_ht, int num_bucket){

    /* A shared log in a siglly linked list */
    art_log * log_head = (art_log *)malloc(sizeof(art_log));
    if (log_head==NULL)
        return 0;
    log_head->next = NULL;
    log_head->leaf = NULL;
//    art_tree t[NUM_BUCKET];
//    for (int i = 0; i < num_bucket; i++)
//    {
//        // art_tree_init2(&(t[i]), log_head);
//        //new_ht->items[i]->radix_pointer = &t[i];
//        // ht_node* item = ht_new_item(NULL, &t[i]);
//    }
}

int test_art_insert_search(char * filename)
{
    art_tree t;
    ht_header new_ht;
    clock_t start, finish;
    double duration;
    int res = art_tree_init2(&t, NULL);
    ht_new(&new_ht, NUM_BUCKET);
    art_leaf *log_head = NULL;
    fail_unless(res == 0);

    alloc_trees(&new_ht, NUM_BUCKET);

    int len;
    char buf[512];
    FILE *f = fopen(filename, "r");
	int tmp_latency = 0;
    uint64_t line = 1;
    start = clock();
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';
        /*
            fail_unless(NULL ==
                        art_insert(&t, (unsigned char*)buf, len, &line));*/
        //art_insert(&t, (unsigned char*)buf, len, &line);
        hadix_insert(&new_ht, (unsigned char*)buf, len, &line);
        line++;
        //if (line % 1000 == 0)
            //printf("%d\n", line);
    }
    uint64_t nlines = line - 1;
    finish = clock();
    duration = (double)(finish - start) / CLOCKS_PER_SEC;
    printf( "RART Insert spends %f seconds\n", duration );
    //stats_report();
    printf( "%d \n", extra_latency - tmp_latency);
	tmp_latency = extra_latency;
    printf("leaf\tnode4\tnode16\tnode48\tnode256\n");
	printf("%d\t%d\t%d\t%d\t%d\n", num_leafs, num_node4, num_node16, num_node48, num_node256);
    //test_range_query(&t, 10000);

    //test_art_recovery(&t);

    // rebuild
    //art_tree t2;
    //int res2 = art_tree_init(&t2);
    //fail_unless(res2 == 0);
    //start = clock();
    //art_recover(&t2, &t);
    //finish = clock();
    //duration = (double)(finish - start) / CLOCKS_PER_SEC;
    //printf( "WOART rebuild spends %f seconds\n", duration );






    // Search for each line
    fseek(f, 0, SEEK_SET);
    line = 1;
    start = clock();
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        if (len <= 3)
            continue;
        buf[len-1] = '\0';

        uint64_t *val = (uint64_t*)hadix_search(&new_ht, (unsigned char*)buf, len-1);

        //if (fail_unless(line == *val))
        //{
        //   printf("Line: %d Val: %" PRIuPTR " Str: %s\n", line,
        //           val, buf);
        //}

        line++;
    }
    finish = clock();
    duration = (double)(finish - start) / CLOCKS_PER_SEC;
    printf( "Hadix search spends %f seconds\n", duration );
    printf( "%d \n", extra_latency - tmp_latency);
	tmp_latency = extra_latency;
    //stats_report();
    //printf( "Flush count is %d \n", extra_latency);

    // Updatess
    fseek(f, 0, SEEK_SET);
    line = 1;
    start = clock();
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';
        hadix_insert(&new_ht, (unsigned char*)buf, len, &line);
        line++;
    }
    nlines = line - 1;
    finish = clock();
    duration = (double)(finish - start) / CLOCKS_PER_SEC;
    printf( "Hadix update spends %f seconds\n", duration );
    printf( "%d \n", extra_latency - tmp_latency);
	tmp_latency = extra_latency;
    //stats_report();
    //printf( "Flush count is %d \n", extra_latency);
    //test_art_update(&t, f);


    start = clock();
    // Check the minimum
    art_leaf *l = art_minimum(&t);
    fail_unless(l && strcmp((char*)l->key, "A") == 0);
    // Check the maximum
    l = art_maximum(&t);
    fail_unless(l && strcmp((char*)l->key, "zythum") == 0);
    finish = clock();
    duration = (double)(finish - start) / CLOCKS_PER_SEC;
    printf( "Hadix find/max_min  spends %f seconds\n", duration );
    printf( "%d \n", extra_latency - tmp_latency);
	tmp_latency = extra_latency;

    // delete
    line = 1;
    fseek(f, 0, SEEK_SET);
    start = clock();
    uintptr_t val;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';


        // Delete, should get lineno back
        hadix_delete(&new_ht, (unsigned char*)buf, len);
        //val = (uintptr_t)art_delete(&t, (unsigned char*)buf, len);
        //fail_unless(line == val);

        // Check the size
        //fail_unless(art_size(&t) == nlines - line);
        line++;
    }
    finish = clock();
    duration = (double)(finish - start) / CLOCKS_PER_SEC;
    printf( "Hadix delete spends %f seconds\n", duration );
    stats_report();
    printf( "%d \n", extra_latency - tmp_latency);
	tmp_latency = extra_latency;
    //printf( "Flush count is %d \n", extra_latency);


    res = art_tree_destroy(&t);
    fail_unless(res == 0);

}


int main(int argc, char **argv) {
    clock_t start, finish;
    double duration;
    start = clock();
    char * p;
    char * filename = argv[1];
    int conv = (int)strtol(argv[2], &p, 10);
    //extra_latency = conv;
    //printf("conv is %d", conv);
    //test_flush_latency();
    //test_art_insert();
    //test_art_insert_verylong();
    test_art_insert_search(filename);
    //test_art_rebuild(filenames)
    //test_art_insert_delete();
    //test_art_insert_iter();

    finish = clock();
    duration = (double)(finish - start) / CLOCKS_PER_SEC;
    printf( "%f seconds\n", duration );
}

