#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include "art.h"
//#include "../src/lib/pmalloc.h"
#include "flush_delay.h"

#ifdef __i386__
#include <emmintrin.h>
#else
#ifdef __amd64__
#include <emmintrin.h>
#endif
#endif

#define CACHELINE_SIZE 64
#define LEAF_NODE 0
#define LOG_NODE 1
#define MAX_KEY_LEN 25
#define MAX_VALUE_LEN 16
#define PM_NODE_TYPES 2
#define SLAB_ALLOCATE 1

#define LOG_MODE 0
/**
 * Macros to manipulate pointer tags
 * Set last bit to 1 to identify a pointer to a LEAF
 * *********************************************************************
 * uintptr_t： an unsigned integer type with the property that any valid
 *  pointer to void can be converted to this type, then converted back
 *  to pointer to void, and the result will compare equal to the
 * original pointer
 */
#define IS_LEAF(x) (((uintptr_t)x & 1))
#define SET_LEAF(x) ((void*)((uintptr_t)x | 1))
#define LEAF_RAW(x) ((art_leaf*)((void*)((uintptr_t)x & ~1)))

extern int extra_latency;
extern int num_leafs;
extern int num_node4;
extern int num_node16;
extern int num_node48;
extern int num_node256;

/**********************************************************************
 * ********   pmalloc() and pfree() wrap psedo code  ******************
 */

void* pmalloc (size_t size){
    return malloc(size);
}

void pfree (void *ptr, size_t size){
    return free(ptr);
}


void set_status(art_leaf *l, uint8_t status)
{
    uint32_t key_len = 0xfffffffc;
    key_len = (l->key_len & 0xfffffffc) | (status & 0x3);
    l->key_len = key_len;
    //persistent(&l->key_len, sizeof(uint32_t), 1);
}

inline uint8_t read_status(art_leaf *l)
{
    return (uint8_t)(0x3&l->key_len);
}

void set_keylen(art_leaf *l, uint32_t key_len)
{
    l->key_len = (l->key_len &0x3) | (key_len << 2);
    //persistent(&(l->key_len), sizeof(uint32_t), 1);
}

uint32_t  read_keylen(art_leaf *l)
{
    return (uint32_t)(l->key_len >> 2);
}



/* Slab allocation implementation
 * input: allocation type
 * return: a single data structure space*/
alloc_meta * tree_alloc_meta = NULL;


uint8_t add_idx(meta_node* curr_meta, uint8_t type)
{
    uint16_t bit_map_array_index, shift_index;
    uint16_t mask = 0xff>>2;
    uint16_t idx = (uint8_t)(curr_meta->bitmap[7])& mask;
    idx++;

    bit_map_array_index = (idx - 1) / 8;
    shift_index = (idx - 1)%8;
    curr_meta->bitmap[bit_map_array_index] |= (1<<shift_index);
    curr_meta->bitmap[7] =  mask & idx;
    //printf("%d", curr_meta[type].bitmap[7]);
    return idx;
}


meta_node* init_alloc_meta(alloc_meta* tree_alloc_meta, uint8_t type)
{
    uint16_t  default_size = 56;
    if (type == LEAF_NODE)
    {
        tree_alloc_meta->leaf_chunk_head = (meta_node *) malloc(sizeof(meta_node));
        tree_alloc_meta->leaf_chunk_head->next = NULL;
        for (int i = 0; i < 8 ; ++i) {
            tree_alloc_meta->leaf_chunk_head->bitmap[i] = 0;
        }
        tree_alloc_meta->curr_leaf_chunk = tree_alloc_meta->leaf_chunk_head;
        persistent(&(tree_alloc_meta->curr_leaf_chunk), sizeof(meta_node *), 1);
    }
}


meta_node * alloc_new_mem_trunk(uint8_t type)
{
    uint16_t  default_size = 56;
    meta_node * prev_node;
    meta_node * new_alloc_meta = (meta_node *) pmalloc(sizeof(meta_node));
    if(type == LEAF_NODE)
    {
        prev_node = tree_alloc_meta->curr_leaf_chunk;
        prev_node->next = new_alloc_meta;
        persistent(&(prev_node->next), sizeof(meta_node *), 1);
        for (int i = 0; i < 8 ; ++i) {
            new_alloc_meta->bitmap[i] = 0;
        }
        tree_alloc_meta->curr_leaf_chunk = new_alloc_meta;
        new_alloc_meta->next = NULL;
        persistent(new_alloc_meta, sizeof(meta_node), 1);
    }
    return new_alloc_meta;

}
/*type: 0, leaf node
 *      1, log
 *      For leaf nodes, since key_len is different, here we use max key_len for sake of simplity*/
void * slab_allocate(uint8_t type, size_t size)
{
    //return pmalloc(size);
    meta_node *curr_chunk;

    /*tree_alloc_meta only init one time for a progress*/
    if(tree_alloc_meta == NULL)
    {
        tree_alloc_meta = (alloc_meta *)pmalloc(sizeof(alloc_meta));
        tree_alloc_meta->curr_leaf_chunk = NULL;
        tree_alloc_meta->curr_log_chunk = NULL;
        tree_alloc_meta->leaf_chunk_head = NULL;
        tree_alloc_meta->log_chunk_head = NULL;
        tree_alloc_meta->leaf_recycle_list = NULL;
        tree_alloc_meta->log_recycle_list = NULL;
        init_alloc_meta(tree_alloc_meta, LEAF_NODE);
        init_alloc_meta(tree_alloc_meta, LOG_NODE);
        persistent(tree_alloc_meta, sizeof(alloc_meta), 1);
    }

    // Init allocate space for header_maps. Only do this one time
    uint16_t default_size = 56;
    uint16_t mask = 0xff>>2;
    uint16_t idx = 0;

    if(type == LEAF_NODE)
    {
        curr_chunk = tree_alloc_meta->curr_leaf_chunk;
    }
    else
        curr_chunk = tree_alloc_meta->curr_log_chunk;
    //Allocate for a new node
    idx = (uint8_t)(curr_chunk[type].bitmap[7])& mask;
    if (idx == default_size )
    {
        // FIXME: lazy persistent strategy: only call persistent() call for bitmap when switch to a new memory chunk
        // FIXME: do we do the same for leaf node? if so we can save tons of times!
        persistent(curr_chunk->bitmap, sizeof(char)*8, 1);
        curr_chunk = alloc_new_mem_trunk(type);
        idx = add_idx(curr_chunk, type);
    }
    else
    {
        idx = add_idx(curr_chunk, type);
        // check if all the slot has been used in this memory block
    }
    if (type == LEAF_NODE)
        return &(curr_chunk->mem_chunk[idx - 1]);
}



/**********************************************************************
********       art data structure code start here    *****************
**********************************************************************/
/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
static art_node* alloc_node(uint8_t type) {
    art_node* n;
    switch (type) {
        case NODE4:
            n = (art_node*)calloc(1, sizeof(art_node4));
            break;
        case NODE16:
            n = (art_node*)calloc(1, sizeof(art_node16));
            break;
        case NODE48:
            n = (art_node*)calloc(1, sizeof(art_node48));
            break;
        case NODE256:
            n = (art_node*)calloc(1, sizeof(art_node256));
            break;
        default:
            abort();
    }
    n->type = type;
    return n;
}

/**
 * Initializes an ART tree
 * @return 0 on success.
 * fixme: log_head point to a log type with no data
 */
int art_tree_init(art_tree *t) {
    t->root = NULL;
    t->leaf_head = NULL;

    art_log * head;
    head = (art_log *)malloc(sizeof(art_log));
    if (head==NULL)
        return 0;
    head->next = NULL;
    head->leaf = NULL;

    t->log_head = head;

    t->size = 0;
    t->flush_count = 0;
    return 0;
}


/*
 * This is a art_tree_init function for hadix tree only
 * to keep the code compatable with old code, we build a new function instead 
 * of modifing the old one
 */
int art_tree_init2(art_tree *t, art_log * head) {
    t->root = NULL;
    t->leaf_head = NULL;


    if (head == NULL) {
        head = (art_log *) malloc(sizeof(art_log));
        if (head == NULL)
            return 0;
        head->next = NULL;
        head->leaf = NULL;
    }

    t->log_head = (art_log *) head;

    t->size = 0;
    t->flush_count = 0;
    //printf("init return 0");
    return 0;
}

// Recursively destroys the tree
static void destroy_node(art_node *n) {
    // Break if null
    if (!n) return;

    // Special case leafs
    if (IS_LEAF(n)) {
        if(!SLAB_ALLOCATE)
        {
            pfree(LEAF_RAW(n), sizeof(art_node));
        }
        return;
    }

    // Handle each node type
    int i;
    union {
        art_node4 *p1;
        art_node16 *p2;
        art_node48 *p3;
        art_node256 *p4;
    } p;
    switch (n->type) {
        case NODE4:
            p.p1 = (art_node4*)n;
            for (i=0;i<n->num_children;i++) {
                destroy_node(p.p1->children[i]);
            }
            break;

        case NODE16:
            p.p2 = (art_node16*)n;
            for (i=0;i<n->num_children;i++)  {
                destroy_node(p.p2->children[i]);
            }
            break;

        case NODE48:
            p.p3 = (art_node48*)n;
            for (i=0;i<n->num_children;i++) {
                destroy_node(p.p3->children[i]);
            }
            break;

        case NODE256:
            p.p4 = (art_node256*)n;
            for (i=0;i<256;i++) {
                if (p.p4->children[i])
                    destroy_node(p.p4->children[i]);
            }
            break;

        default:
            abort();
    }

    // Free ourself on the way up
    free(n);
}

int destory_slab_leaf(alloc_meta * tree_alloc_meta)
{
    meta_node * current_node, *prev_node;
    current_node = tree_alloc_meta->leaf_chunk_head;
    while(current_node != NULL)
    {
        prev_node = current_node;
        current_node = current_node->next;
        //bitmap change is unnecessary, however, we use it to emulate the random deletion cost
        for (int i = 0; i < 8; i++)
        {
            prev_node->bitmap[i] = 0;
            persistent(&(prev_node->bitmap[i]), sizeof(char), 1);
        }
        pfree(prev_node, sizeof(meta_node));
    }
    return 0;
}

int destory_slab_log(alloc_meta * tree_alloc_meta)
{
    meta_node * current_node, *prev_node;
    current_node = tree_alloc_meta->log_chunk_head;
    while(current_node != NULL)
    {
        prev_node = current_node;
        current_node = current_node->next;
        free(prev_node->mem_chunk);
        free(prev_node->bitmap);
    }
}

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int art_tree_destroy(art_tree *t) {
    destroy_node(t->root);
    t->leaf_head = NULL;
    if(SLAB_ALLOCATE)
    {
        destory_slab_leaf(tree_alloc_meta);
        if(LOG_MODE)
            destory_slab_log(tree_alloc_meta);
    }

    return 0;
}

/**
 * Returns the size of the ART tree.
 */

#ifndef BROKEN_GCC_C99_INLINE
extern inline uint64_t art_size(art_tree *t);
#endif

static art_node** find_child(art_node *n, unsigned char c) {
    int i, mask, bitfield;
    union {
        art_node4 *p1;
        art_node16 *p2;
        art_node48 *p3;
        art_node256 *p4;
    } p;
    switch (n->type) {
        case NODE4:
            p.p1 = (art_node4*)n;
            for (i=0 ; i < n->num_children; i++) {
                /* this cast works around a bug in gcc 5.1 when unrolling loops
                 * https://gcc.gnu.org/bugzilla/show_bug.cgi?id=59124
                 */
                if (((unsigned char*)p.p1->keys)[i] == c)
                    return &p.p1->children[i];
            }
            break;

            {
                case NODE16:
                    p.p2 = (art_node16*)n;

                // support non-86 architectures
#ifdef __i386__
                // Compare the key to all 16 stored keys
                __m128i cmp;
                cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c),
                        _mm_loadu_si128((__m128i*)p.p2->keys));

                // Use a mask to ignore children that don't exist
                mask = (1 << n->num_children) - 1;
                bitfield = _mm_movemask_epi8(cmp) & mask;
#else
#ifdef __amd64__
                // Compare the key to all 16 stored keys
                __m128i cmp;
                cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c),
                                     _mm_loadu_si128((__m128i*)p.p2->keys));

                // Use a mask to ignore children that don't exist
                mask = (1 << n->num_children) - 1;
                bitfield = _mm_movemask_epi8(cmp) & mask;
#else
                // Compare the key to all 16 stored keys
                bitfield = 0;
                for (i = 0; i < 16; ++i) {
                    if (p.p2->keys[i] == c)
                        bitfield |= (1 << i);
                }

                // Use a mask to ignore children that don't exist
                mask = (1 << n->num_children) - 1;
                bitfield &= mask;
#endif
#endif

                /*
                 * If we have a match (any bit set) then we can
                 * return the pointer match using ctz to get
                 * the index.
                 */
                if (bitfield)
                    return &p.p2->children[__builtin_ctz(bitfield)];
                break;
            }

        case NODE48:
            p.p3 = (art_node48*)n;
            i = p.p3->keys[c];
            if (i)
                return &p.p3->children[i-1];
            break;

        case NODE256:
            p.p4 = (art_node256*)n;
            if (p.p4->children[c])
                return &p.p4->children[c];
            break;

        default:
            abort();
    }
    return NULL;
}

// Simple inlined if
static inline int min(int a, int b) {
    return (a < b) ? a : b;
}

/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */
static int check_prefix(const art_node *n, const unsigned char *key, int key_len, int depth) {
    int max_cmp = min(min(n->partial_len, MAX_PREFIX_LEN), key_len - depth);
    int idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (n->partial[idx] != key[depth+idx])
            return idx;
    }
    return idx;
}

/**
 * Checks if a leaf matches
 * @return 0 on success.
 */
static int leaf_matches(const art_leaf *n, const unsigned char *key, int key_len, int depth) {
    (void)depth;
    // Fail if the key lengths are different
    if (read_keylen(n) != (uint32_t)key_len) return 1;

    // Compare the keys starting at the depth
    return memcmp(n->key, key, key_len);
}


art_leaf* art_search_leaf(const art_tree *t, const unsigned char *key, int key_len) {
    art_node **child;
    art_node *n = t->root;
    int prefix_len, depth = 0;
    while (n) {
        // Might be a leaf
        if (IS_LEAF(n)) {
            n = (art_node*)LEAF_RAW(n);
            // Check if the expanded path matches
            if (!leaf_matches((art_leaf*)n, key, key_len, depth)) {
                return ((art_leaf*)n);
            }
            return NULL;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = check_prefix(n, key, key_len, depth);
            if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len))
                return NULL;
            depth = depth + n->partial_len;
        }

        // Recursively search
        child = find_child(n, key[depth]);
        n = (child) ? *child : NULL;
        depth++;
    }
    return NULL;
}


/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_search(const art_tree *t, const unsigned char *key, int key_len) {
    art_node **child;
    art_node *n = t->root;
    int prefix_len, depth = 0;
    while (n) {
        // Might be a leaf
        if (IS_LEAF(n)) {
            n = (art_node*)LEAF_RAW(n);
            // Check if the expanded path matches
            if (!leaf_matches((art_leaf*)n, key, key_len, depth)) {
                return ((art_leaf*)n)->value;
            }
            return NULL;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = check_prefix(n, key, key_len, depth);
            if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len))
                return NULL;
            depth = depth + n->partial_len;
        }
        // Recursively search
        child = find_child(n, key[depth]);
        n = (child) ? *child : NULL;
        depth++;
    }
    return NULL;
}


// Find the minimum leaf under a node
static art_leaf* minimum(const art_node *n) {
    // Handle base cases
    if (!n) return NULL;
    if (IS_LEAF(n)) return LEAF_RAW(n);

    int idx;
    switch (n->type) {
        case NODE4:
            return minimum(((const art_node4*)n)->children[0]);
        case NODE16:
            return minimum(((const art_node16*)n)->children[0]);
        case NODE48:
            idx=0;
            while (!((const art_node48*)n)->keys[idx]) idx++;
            idx = ((const art_node48*)n)->keys[idx] - 1;
            return minimum(((const art_node48*)n)->children[idx]);
        case NODE256:
            idx=0;
            while (!((const art_node256*)n)->children[idx]) idx++;
            return minimum(((const art_node256*)n)->children[idx]);
        default:
            abort();
    }
}


// Find the maximum leaf under a node
static art_leaf* maximum(const art_node *n) {
    // Handle base cases
    if (!n) return NULL;
    if (IS_LEAF(n)) return LEAF_RAW(n);

    int idx;
    switch (n->type) {
        case NODE4:
            return maximum(((const art_node4*)n)->children[n->num_children-1]);
        case NODE16:
            return maximum(((const art_node16*)n)->children[n->num_children-1]);
        case NODE48:
            idx=255;
            while (!((const art_node48*)n)->keys[idx]) idx--;
            idx = ((const art_node48*)n)->keys[idx] - 1;
            return maximum(((const art_node48*)n)->children[idx]);
        case NODE256:
            idx=255;
            while (!((const art_node256*)n)->children[idx]) idx--;
            return maximum(((const art_node256*)n)->children[idx]);
        default:
            abort();
    }
}


/**
 * Returns the minimum valued leaf
 */
art_leaf* art_minimum(art_tree *t) {
    return minimum((art_node*)t->root);
}

/**
 * Returns the maximum valued leaf
 */
art_leaf* art_maximum(art_tree *t) {
    return maximum((art_node*)t->root);
}

// typedef struct {
// 	art_leaf *leaf;
// 	art_log *next;
// 	art_log *prev;
// 	uint8_t type;
// 	uint8_t status;
// }art_log;


/**
 * 1. Allocate space
 * 2. Set status to ALLOCATED, Persistent it
 * 3. Add to log
 * 4. Initilize the field of leaf node
 * 5. persistent it
 * 6. Update staus as an atomic flag
 * */
static art_leaf* make_leaf(art_log **log_head, const unsigned char *key, int key_len, uint64_t *value) {
    art_leaf *l = (art_leaf*)slab_allocate(LEAF_NODE, sizeof(art_leaf));
    //FIXME: Do we need to set status here?
    //set_status(l, ALLOCATED);
    //l->status = ALLOCATED;
    //persistent(&(l->status), sizeof(uint32_t),0);
    l->value = (void *)pmalloc(sizeof(uint64_t));
    memcpy(l->value, value, sizeof(uint64_t));
    persistent(l->value, sizeof(uint64_t), 1);
    set_keylen(l, key_len);
    //l->key_len = key_len;
    memcpy(l->key, key, key_len);
	num_leafs++;
//    l->prev = NULL;
//    l->next = NULL;
//    persistent(&(l->next), sizeof(art_leaf *),1);
//    persistent(&(l->prev), sizeof(art_leaf *),1);
    //set_status(l, INITILIZED);
    //l->status = INITILIZED;
    //persistent(&(l->status), sizeof(uint32_t),1);
    return l;
}

/**
 * l1:abcdefg hkkfvk
 * l2:ssddefg hkklva
 * 			 ^
 * 			 |
 * 			depth
 *
 * compare start at depth, find common prefix
 * return the length of common prefix
 * */
static int longest_common_prefix(art_leaf *l1, art_leaf *l2, int depth) {
    int max_cmp = min(read_keylen(l1), read_keylen(l2)) - depth;
    int idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (l1->key[depth+idx] != l2->key[depth+idx])
            return idx;
    }
    return idx;
}

static void copy_header(art_node *dest, art_node *src) {
    dest->num_children = src->num_children;
    dest->partial_len = src->partial_len;
    memcpy(dest->partial, src->partial, min(MAX_PREFIX_LEN, src->partial_len));
    persistent(&(dest->num_children), sizeof(uint8_t), 1);
    persistent(&(dest->partial_len), sizeof(uint32_t), 1);
    persistent(dest->partial, sizeof(unsigned char)*min(MAX_PREFIX_LEN, src->partial_len), 1);
}

static void add_child256(art_node256 *n, art_node **ref, unsigned char c, void *child) {
    (void)ref;
    n->n.num_children++;
    n->children[c] = (art_node*)child;
}

static void add_child48(art_node48 *n, art_node **ref, unsigned char c, void *child) {
    if (n->n.num_children < 48) {
        int pos = 0;
        while (n->children[pos]) pos++;
        n->children[pos] = (art_node*)child;
        n->keys[c] = pos + 1;
        n->n.num_children++;
    } else {
        art_node256 *new_node = (art_node256*)alloc_node(NODE256);
		num_node256++;
		num_node48--;
        for (int i=0;i<256;i++) {
            if (n->keys[i]) {
                new_node->children[i] = n->children[n->keys[i] - 1];
            }
        }
        copy_header((art_node*)new_node, (art_node*)n);
        *ref = (art_node*)new_node;
        free(n);
        add_child256(new_node, ref, c, child);
    }
}

static void add_child16(art_node16 *n, art_node **ref, unsigned char c, void *child) {
    if (n->n.num_children < 16) {
        unsigned mask = (1 << n->n.num_children) - 1;

        // support non-x86 architectures
#ifdef __i386__
        __m128i cmp;

            // Compare the key to all 16 stored keys
            cmp = _mm_cmplt_epi8(_mm_set1_epi8(c),
                    _mm_loadu_si128((__m128i*)n->keys));

            // Use a mask to ignore children that don't exist
            unsigned bitfield = _mm_movemask_epi8(cmp) & mask;
#else
#ifdef __amd64__
        __m128i cmp;

        // Compare the key to all 16 stored keys
        cmp = _mm_cmplt_epi8(_mm_set1_epi8(c),
                             _mm_loadu_si128((__m128i*)n->keys));

        // Use a mask to ignore children that don't exist
        unsigned bitfield = _mm_movemask_epi8(cmp) & mask;
#else
        // Compare the key to all 16 stored keys
        unsigned bitfield = 0;
        for (short i = 0; i < 16; ++i) {
            if (c < n->keys[i])
                bitfield |= (1 << i);
        }

        // Use a mask to ignore children that don't exist
        bitfield &= mask;
#endif
#endif

        // Check if less than any
        unsigned idx;
        if (bitfield) {
            // determines the count of trailing zero in the binary representation of a number.
            idx = __builtin_ctz(bitfield);
            memmove(n->keys+idx+1,n->keys+idx,n->n.num_children-idx);
            memmove(n->children+idx+1,n->children+idx,
                    (n->n.num_children-idx)*sizeof(void*));
        } else
            idx = n->n.num_children;

        // Set the child
        n->keys[idx] = c;
        n->children[idx] = (art_node*)child;
        n->n.num_children++;

    } else {
        art_node48 *new_node = (art_node48*)alloc_node(NODE48);
		num_node48++;
		num_node16--;
        // Copy the child pointers and populate the key map
        memcpy(new_node->children, n->children,
               sizeof(void*)*n->n.num_children);
        for (int i=0;i<n->n.num_children;i++) {
            new_node->keys[n->keys[i]] = i + 1;
        }
        copy_header((art_node*)new_node, (art_node*)n);
        *ref = (art_node*)new_node;
        free(n);
        add_child48(new_node, ref, c, child);
    }
}

/*keys are sorted*/
static void add_child4(art_node4 *n, art_node **ref, unsigned char c, void *child) {
    if (n->n.num_children < 4) {
        int idx;
        for (idx=0; idx < n->n.num_children; idx++) {
            if (c < n->keys[idx]) break;
        }

        // Shift to make room
        //memmove()) is a safe version of memcpy()
        //shifting both keys and pointers to the right
        memmove(n->keys+idx+1, n->keys+idx, n->n.num_children - idx);
        memmove(n->children+idx+1, n->children+idx,
                (n->n.num_children - idx)*sizeof(void*));

        // Insert element
        n->keys[idx] = c;
        n->children[idx] = (art_node*)child;
        n->n.num_children++;

    } else {
        art_node16 *new_node = (art_node16*)alloc_node(NODE16);
		num_node16++;
		num_node4--;
        // Copy the child pointers and the key map
        memcpy(new_node->children, n->children,
               sizeof(void*)*n->n.num_children);
        memcpy(new_node->keys, n->keys,
               sizeof(unsigned char)*n->n.num_children);
        copy_header((art_node*)new_node, (art_node*)n);
        *ref = (art_node*)new_node;
        free(n);
        add_child16(new_node, ref, c, child);
    }
}

static void add_child(art_node *n, art_node **ref, unsigned char c, void *child) {
    switch (n->type) {
        case NODE4:
            return add_child4((art_node4*)n, ref, c, child);
        case NODE16:
            return add_child16((art_node16*)n, ref, c, child);
        case NODE48:
            return add_child48((art_node48*)n, ref, c, child);
        case NODE256:
            return add_child256((art_node256*)n, ref, c, child);
        default:
            abort();
    }
}

/**
 * Calculates the index at which the prefixes mismatch
 * Specificilly. When no match found, return 0;
 * Which means when partial_len == 0, this is not a compressed node
 */
static int prefix_mismatch(const art_node *n, const unsigned char *key, int key_len, int depth) {
    int max_cmp = min(min(MAX_PREFIX_LEN, n->partial_len), key_len - depth);
    int idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (n->partial[idx] != key[depth+idx])
            return idx;
    }

    // If the prefix is short we can avoid finding a leaf
    if (n->partial_len > MAX_PREFIX_LEN) {
        // Prefix is longer than what we've checked, find a min leaf l
        //and get  the prefix of key and l
        art_leaf *l = minimum(n);
        max_cmp = min(read_keylen(l), key_len)- depth;
        for (; idx < max_cmp; idx++) {
            if (l->key[idx+depth] != key[depth+idx])
                return idx;
        }
    }
    return idx;
}


/**
 * Insert a leaf node into the double-linked-list
 * The operation is atomic safe
 * fixme: has to add mechanism to prevent persistent memory leak*/
//static void linklist_insert(art_log *log_head, art_leaf **leaf_header, art_leaf *node)
//{
//    node->next = *leaf_header;
//    node->prev = NULL;
//    //persistent(&(node->next), sizeof(art_leaf *), 0);
//    //persistent(&(node->prev), sizeof(art_leaf *), 0);
//
//    if (*leaf_header != NULL)
//    {
//        (*leaf_header)->prev = node;
//        //persistent((*leaf_header)->prev, sizeof(void*),0);
//    }
//    *leaf_header = node;
//    //persistent(*leaf_header, sizeof(art_leaf),0);
//    set_status(node, INLIST);
//    //node->status = INLIST;
//    //persistent(&(node->status),sizeof(uint32_t),1);
//}



/* This function is different from recursive_insert
 *  This function doesnot need to create a leaf node as leaf already exists*/
static void* recursive_insert_leaf(art_tree *t, art_node *n, art_node **ref,
                                   art_leaf **leaf_header, art_log **log_header,
                                   int depth, int *old, art_leaf *leaf) {
    // If we are at a NULL node, inject a leaf
    art_node *tmp;
    const unsigned char *key = leaf->key;
    int key_len = read_keylen(leaf);
    uint64_t *value = leaf->value;

    if (!n) {
        tmp = (art_node*)(SET_LEAF(leaf));
        // leaf_header = LEAF_RAW (*ref);
        *ref = tmp;
        //set the log invalid; we don't delete it immedially for future use
        return NULL;
    }

    // If we are at a leaf, we need to replace it with a node
    if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);

        // Check if we are updating an existing value, depth not used
        // leaf_matches() returns 0 when match
        if (!leaf_matches(l, key, key_len, depth)) {
            printf("error, find replicate keys during recovery");
            return NULL;
        }

        // New key, we must split the leaf into a node4
        art_node4 *new_node = (art_node4*)alloc_node(NODE4);
        art_leaf *l2 = leaf;

        // Determine longest prefix
        int longest_prefix = longest_common_prefix(l, l2, depth);
        //In a compressed node, partial_len is set to be prefix length
        new_node->n.partial_len = longest_prefix;
        memcpy(new_node->n.partial, key+depth, min(MAX_PREFIX_LEN, longest_prefix));
        // Add the leafs to the new node4
        *ref = (art_node*)new_node;
        add_child4(new_node, ref, l->key[depth+longest_prefix], SET_LEAF(l));
        add_child4(new_node, ref, l2->key[depth+longest_prefix], SET_LEAF(l2));
        return NULL;
    }

    // Check if given node has a prefix
    if (n->partial_len) {
        // Determine if the prefixes differ, since we need to split
        // Calculates the index at which the prefixes mismatch
        int prefix_diff = prefix_mismatch(n, key, key_len, depth);
        //which means there are child nodes, still need to compare
        if ((uint32_t)prefix_diff >= n->partial_len) {
            depth += n->partial_len;
            goto RECURSE_SEARCH;
        }

        // Create a new inner node, which contains the common parts (prefix)
        art_node4 *new_node = (art_node4*)alloc_node(NODE4);
        *ref = (art_node*)new_node;
        new_node->n.partial_len = prefix_diff;
        memcpy(new_node->n.partial, n->partial, min(MAX_PREFIX_LEN, prefix_diff));

        // Adjust the prefix of the old node
        if (n->partial_len <= MAX_PREFIX_LEN) {
            add_child4(new_node, ref, n->partial[prefix_diff], n);
            n->partial_len -= (prefix_diff+1);
            memmove(n->partial, n->partial+prefix_diff+1,
                    min(MAX_PREFIX_LEN, n->partial_len));
        } else {
            n->partial_len -= (prefix_diff+1);
            art_leaf *l = minimum(n);
            add_child4(new_node, ref, l->key[depth+prefix_diff], n);
            memcpy(n->partial, l->key+depth+prefix_diff+1,
                   min(MAX_PREFIX_LEN, n->partial_len));
        }

        // Insert the new leaf
        art_leaf *l = leaf;
        //Wen Pan: insert into linked list
        add_child4(new_node, ref, key[depth+prefix_diff], SET_LEAF(l));
        return NULL;
    }

    RECURSE_SEARCH:;

    // Find a child to recurse to
    art_node **child = find_child(n, key[depth]);
    if (child) {
        return recursive_insert_leaf(t,*child, child,leaf_header, log_header,
                                     depth+1, old, leaf);
    }

    // No child, node goes within us
    art_leaf *l = leaf;
    add_child(n, ref, key[depth], SET_LEAF(l));
    return NULL;
}


art_leaf * get_next_leaf(meta_node ** current_node, uint8_t * idx)
{
    uint16_t bit_map_array_index, shift_index;

    while(*current_node != NULL)
    {
        if (*idx == 55)
        {
            *current_node = (*current_node)->next;
        }
        if (*current_node == NULL)
            return NULL;
        *idx = ((*idx) + 1) % 56;
        bit_map_array_index = *idx / 8;
        shift_index = (*idx) % 8;
        if (((*current_node)->bitmap[bit_map_array_index]) & (1<<shift_index) != 0 )
            break;
    }


    return (art_leaf *)(&((*current_node)->mem_chunk[*idx]));
}


/* RARTree recovery function
 * FIXME: Need to recover from bulk memory instead of linked-leaf*/
void* art_recover(art_tree *t, art_tree *old_T) {
    int old_val = 0;
    uint8_t idx ;
    idx = 0;
    meta_node * current_node = (tree_alloc_meta->leaf_chunk_head);
    art_leaf * current_leaf = get_next_leaf(&current_node,&idx);
    while (current_leaf ) {
        void *old = recursive_insert_leaf(t, t->root, &t->root, &t->leaf_head,
                                          &t->log_head, 0, &old_val, current_leaf);
        if (!old_val) t->size++;
        current_leaf = get_next_leaf(&current_node, &idx);
    }
}




/** n: the tree the are insert to
 * ref: the tree node that has index to the inserted node n
 * key, key_len, value: you know it
 * depth: current depth
 * old: a flag incicates we find an existing leaf and update it
 * return value: pointer to old_val when update is performaned (
 * find same key already exists in the radix tree)
 * return 0 when create a new node
 * */
static void* recursive_insert(art_tree *t, art_node *n, art_node **ref,
                              art_leaf **leaf_header, art_log **log_header,
                              const unsigned char *key, int key_len, uint64_t *value, int depth, int *old) {
    // If we are at a NULL node, inject a leaf
    art_node *tmp;
    if (!n) {
        tmp = (art_node*)SET_LEAF(make_leaf(log_header, key, key_len, value));
        // leaf_header = LEAF_RAW (*ref);
        //linklist_insert(*log_header, leaf_header, LEAF_RAW (tmp));
        *ref = tmp;

        return NULL;
    }

    // If we are at a leaf, we need to replace it with a node
    if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);

        // Check if we are updating an existing value, depth not used
        // fixme: use delete + insert to update a existing leaf
        // leaf_matches() returns 0 when match
        if (!leaf_matches(l, key, key_len, depth)) {
            void * new_value = (void *) pmalloc(sizeof(uint64_t));
            pfree(l->value, sizeof(uint64_t));
            l->value = new_value;
            memcpy(l->value, value, sizeof(uint64_t));
            persistent(l->value, sizeof(uint64_t), 1);
            *old = 1;
            return NULL;
        }

        // New key, we must split the leaf into a node4
        art_node4 *new_node = (art_node4*)alloc_node(NODE4);
		num_node4++;

        // Create a new leaf
        art_leaf *l2 = make_leaf(log_header, key, key_len, value);

        // Determine longest prefix
        int longest_prefix = longest_common_prefix(l, l2, depth);
        //In a compressed node, partial_len is set to be prefix length
        new_node->n.partial_len = longest_prefix;
        memcpy(new_node->n.partial, key+depth, min(MAX_PREFIX_LEN, longest_prefix));
        // Add the leafs to the new node4
        *ref = (art_node*)new_node;
        add_child4(new_node, ref, l->key[depth+longest_prefix], SET_LEAF(l));
        //Wen Pan: insert into linked list
        //linklist_insert(*log_header, leaf_header, l2);
        add_child4(new_node, ref, l2->key[depth+longest_prefix], SET_LEAF(l2));
        return NULL;
    }

    // Check if given node has a prefix
    if (n->partial_len) {
        // Determine if the prefixes differ, since we need to split
        // Calculates the index at which the prefixes mismatch
        int prefix_diff = prefix_mismatch(n, key, key_len, depth);
        //which means there are child nodes, still need to compare
        if ((uint32_t)prefix_diff >= n->partial_len) {
            depth += n->partial_len;
            goto RECURSE_SEARCH;
        }

        // Create a new inner node, which contains the common parts (prefix)
        art_node4 *new_node = (art_node4*)alloc_node(NODE4);
		num_node4++;
        *ref = (art_node*)new_node;
        new_node->n.partial_len = prefix_diff;
        memcpy(new_node->n.partial, n->partial, min(MAX_PREFIX_LEN, prefix_diff));

        // Adjust the prefix of the old node
        if (n->partial_len <= MAX_PREFIX_LEN) {
            add_child4(new_node, ref, n->partial[prefix_diff], n);
            n->partial_len -= (prefix_diff+1);
            memmove(n->partial, n->partial+prefix_diff+1,
                    min(MAX_PREFIX_LEN, n->partial_len));
        } else {
            n->partial_len -= (prefix_diff+1);
            art_leaf *l = minimum(n);
            add_child4(new_node, ref, l->key[depth+prefix_diff], n);
            memcpy(n->partial, l->key+depth+prefix_diff+1,
                   min(MAX_PREFIX_LEN, n->partial_len));
        }

        // Insert the new leaf
        art_leaf *l = make_leaf(log_header, key, key_len, value);
        //Wen Pan: insert into linked list
        //linklist_insert(*log_header, leaf_header, LEAF_RAW (l));
        add_child4(new_node, ref, key[depth+prefix_diff], SET_LEAF(l));
        return NULL;
    }

    RECURSE_SEARCH:;

    // Find a child to recurse to
    art_node **child = find_child(n, key[depth]);
    if (child) {
        return recursive_insert(t,*child, child,leaf_header, log_header,
                                key, key_len, value, depth+1, old);
    }

    // No child, node goes within us
    art_leaf *l = make_leaf(log_header, key, key_len, value);
    //Wen Pan: insert into linked list
    //linklist_insert(*log_header, leaf_header, LEAF_RAW (l));
    add_child(n, ref, key[depth], SET_LEAF(l));
    return NULL;
}

/**
 * Inserts a new value into the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg value Opaque value.
 * @return NULL if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* art_insert(art_tree *t, const unsigned char *key, int key_len, uint64_t *value) {
    int old_val = 0;
    void *old = recursive_insert(t, t->root, &t->root, &t->leaf_head,
                                 &t->log_head, key, key_len, value, 0, &old_val);
    if (!old_val) t->size++;
    return old;
}

static void remove_child256(art_node256 *n, art_node **ref, unsigned char c) {
    n->children[c] = NULL;
    n->n.num_children--;

    // Resize to a node48 on underflow, not immediately to prevent
    // trashing if we sit on the 48/49 boundary
    if (n->n.num_children == 37) {
        art_node48 *new_node = (art_node48*)alloc_node(NODE48);
        *ref = (art_node*)new_node;
        copy_header((art_node*)new_node, (art_node*)n);

        int pos = 0;
        for (int i=0;i<256;i++) {
            if (n->children[i]) {
                new_node->children[pos] = n->children[i];
                new_node->keys[i] = pos + 1;
                pos++;
            }
        }
        free(n);
    }
}

static void remove_child48(art_node48 *n, art_node **ref, unsigned char c) {
    int pos = n->keys[c];
    n->keys[c] = 0;
    n->children[pos-1] = NULL;
    n->n.num_children--;

    if (n->n.num_children == 12) {
        art_node16 *new_node = (art_node16*)alloc_node(NODE16);
        *ref = (art_node*)new_node;
        copy_header((art_node*)new_node, (art_node*)n);

        int child = 0;
        for (int i=0;i<256;i++) {
            pos = n->keys[i];
            if (pos) {
                new_node->keys[child] = i;
                new_node->children[child] = n->children[pos - 1];
                child++;
            }
        }
        free(n);
    }
}

static void remove_child16(art_node16 *n, art_node **ref, art_node **l) {
    int pos = l - n->children;
    memmove(n->keys+pos, n->keys+pos+1, n->n.num_children - 1 - pos);
    memmove(n->children+pos, n->children+pos+1, (n->n.num_children - 1 - pos)*sizeof(void*));
    n->n.num_children--;

    if (n->n.num_children == 3) {
        art_node4 *new_node = (art_node4*)alloc_node(NODE4);
        *ref = (art_node*)new_node;
        copy_header((art_node*)new_node, (art_node*)n);
        memcpy(new_node->keys, n->keys, 4);
        memcpy(new_node->children, n->children, 4*sizeof(void*));
        free(n);
    }
}

static void remove_child4(art_node4 *n, art_node **ref, art_node **l) {
    int pos = l - n->children;
    memmove(n->keys+pos, n->keys+pos+1, n->n.num_children - 1 - pos);
    memmove(n->children+pos, n->children+pos+1, (n->n.num_children - 1 - pos)*sizeof(void*));
    n->n.num_children--;

    // Remove nodes with only a single child
    if (n->n.num_children == 1) {
        art_node *child = n->children[0];
        if (!IS_LEAF(child)) {
            // Concatenate the prefixes
            int prefix = n->n.partial_len;
            if (prefix < MAX_PREFIX_LEN) {
                n->n.partial[prefix] = n->keys[0];
                prefix++;
            }
            if (prefix < MAX_PREFIX_LEN) {
                int sub_prefix = min(child->partial_len, MAX_PREFIX_LEN - prefix);
                memcpy(n->n.partial+prefix, child->partial, sub_prefix);
                prefix += sub_prefix;
            }

            // Store the prefix in the child
            memcpy(child->partial, n->n.partial, min(prefix, MAX_PREFIX_LEN));
            child->partial_len += n->n.partial_len + 1;
        }
        *ref = child;
        free(n);
    }
}

/**
 * Remove a pointer/key pair from the inner node
 * If the size of the node is shrinked into certain boundary,
 * Change the type of the node (e.g., node16->node4)
 * */
static void remove_child(art_node *n, art_node **ref, unsigned char c, art_node **l) {
    switch (n->type) {
        case NODE4:
            return remove_child4((art_node4*)n, ref, l);
        case NODE16:
            return remove_child16((art_node16*)n, ref, l);
        case NODE48:
            return remove_child48((art_node48*)n, ref, c);
        case NODE256:
            return remove_child256((art_node256*)n, ref, c);
        default:
            abort();
    }
}


/**
 * return l if successfully find the leaf node*/
static art_leaf* recursive_delete(art_node *n, art_node **ref, const unsigned char *key, int key_len, int depth) {
    // Search terminated
    if (!n) return NULL;

    // Handle hitting a leaf node
    if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);
        if (!leaf_matches(l, key, key_len, depth)) {
            *ref = NULL;
            return l;
        }
        return NULL;
    }

    // Bail if the prefix does not match
    if (n->partial_len) {
        int prefix_len = check_prefix(n, key, key_len, depth);
        if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len)) {
            return NULL;
        }
        depth = depth + n->partial_len;
    }

    // Find child node
    art_node **child = find_child(n, key[depth]);
    if (!child) return NULL;

    // If the child is leaf, delete from this node
    if (IS_LEAF(*child)) {
        art_leaf *l = LEAF_RAW(*child);
        if (!leaf_matches(l, key, key_len, depth)) {
            remove_child(n, ref, key[depth], child);
            return l;
        }
        return NULL;

        // Recurse
    } else {
        return recursive_delete(*child, child, key, key_len, depth+1);
    }
}

/**Wen Pan: Remove a leaf from double-linkerd_list
 * Considerations:
 * 	1. Consistency
 * 	2. Recover
 *
 * A special case： leaf is the header
 * */
//int remove_leaf_from_list(art_tree *t, art_leaf *l){
//
//
//    if(!l)
//        return 1;
//    art_leaf *prev, *next;
//
//    prev = l->prev;
//    next = l->next;
//
//    if (prev != NULL) {
//        prev->next = next;
//        persistent(&(prev->next), sizeof(void *), 0);
//    }
//    if (next != NULL)
//        next->prev = prev;
//
//    //only one update is needed
//    //persistent(&(next->prev), sizeof(void *),0);
//    set_status(l, REMOVED);
//    //l->status = REMOVED;
//    //persistent(&(l->status), sizeof(uint32_t),1);
//    l->prev = NULL;
//    l->next = NULL;
//    //persistent(l, 2 * sizeof(void *),0);
//
//    if(t->leaf_head == l)
//        t->leaf_head = l->next;
//    return 0;
//}

void *delete_log(art_tree *t,const unsigned char *key){
    return NULL;

}


//FIXME: a possible solution is to check the address of l and find the memory chunk address range contains l
// pseudo code for deletetion
// mark_delete only remove corresponding bit in the bitmap
// Assume a background GC process check on each memory chunk for final free() process
void mark_delete(art_tree *t, art_leaf *l)
{
    return;
}

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 *
 * fixme: after deletion, the node should be gone
 * and return an old value is no possible;
 * Thus, user program shoul dexpect a different return value of this call
 */
int art_delete(art_tree *t, const unsigned char *key, int key_len) {
    //delete_log(art_tree *t,const unsigned char *key);
    art_leaf *l = recursive_delete(t->root, &t->root, key, key_len, 0);
    if (l) {	//if the leaf node does exist
        t->size--;
        void *old = l->value;
        pfree(l->value, sizeof(uint64_t));
        //void *old = l->value;
        // pseudo code for deletetion
        // mark_delete only remove corresponding bit in the bitmap
        // Assume a background GC process check on each memory chunk for final free() process
        mark_delete(t, l);
        //pfree(l, sizeof(art_leaf) + read_keylen(l));
        return 0;
        //return old;
    }
    //return NULL;
    return 1;
}

// Recursively iterates over the tree
static int recursive_iter(art_node *n, art_callback cb, void *data) {
    // Handle base cases
    if (!n) return 0;
    if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);
        return cb(data, (const unsigned char*)l->key, read_keylen(l), l->value);
    }

    int idx, res;
    switch (n->type) {
        case NODE4:
            for (int i=0; i < n->num_children; i++) {
                res = recursive_iter(((art_node4*)n)->children[i], cb, data);
                if (res) return res;
            }
            break;

        case NODE16:
            for (int i=0; i < n->num_children; i++) {
                res = recursive_iter(((art_node16*)n)->children[i], cb, data);
                if (res) return res;
            }
            break;

        case NODE48:
            for (int i=0; i < 256; i++) {
                idx = ((art_node48*)n)->keys[i];
                if (!idx) continue;

                res = recursive_iter(((art_node48*)n)->children[idx-1], cb, data);
                if (res) return res;
            }
            break;

        case NODE256:
            for (int i=0; i < 256; i++) {
                if (!((art_node256*)n)->children[i]) continue;
                res = recursive_iter(((art_node256*)n)->children[i], cb, data);
                if (res) return res;
            }
            break;

        default:
            abort();
    }
    return 0;
}

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each. The call back gets a
 * key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int art_iter(art_tree *t, art_callback cb, void *data) {
    return recursive_iter(t->root, cb, data);
}

/**
 * Checks if a leaf prefix matches
 * @return 0 on success.
 */
static int leaf_prefix_matches(const art_leaf *n, const unsigned char *prefix, int prefix_len) {
    // Fail if the key length is too short
    if (read_keylen(n) < (uint32_t)prefix_len) return 1;

    // Compare the keys
    return memcmp(n->key, prefix, prefix_len);
}

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each that matches a given prefix.
 * The call back gets a key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg prefix The prefix of keys to read
 * @arg prefix_len The length of the prefix
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int art_iter_prefix(art_tree *t, const unsigned char *key, int key_len, art_callback cb, void *data) {
    art_node **child;
    art_node *n = t->root;
    int prefix_len, depth = 0;
    while (n) {
        // Might be a leaf
        if (IS_LEAF(n)) {
            n = (art_node*)LEAF_RAW(n);
            // Check if the expanded path matches
            if (!leaf_prefix_matches((art_leaf*)n, key, key_len)) {
                art_leaf *l = (art_leaf*)n;
                return cb(data, (const unsigned char*)l->key, read_keylen(l), l->value);
            }
            return 0;
        }

        // If the depth matches the prefix, we need to handle this node
        if (depth == key_len) {
            art_leaf *l = minimum(n);
            if (!leaf_prefix_matches(l, key, key_len))
                return recursive_iter(n, cb, data);
            return 0;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = prefix_mismatch(n, key, key_len, depth);

            // Guard if the mis-match is longer than the MAX_PREFIX_LEN
            if ((uint32_t)prefix_len > n->partial_len) {
                prefix_len = n->partial_len;
            }

            // If there is no match, search is terminated
            if (!prefix_len) {
                return 0;

                // If we've matched the prefix, iterate on this node
            } else if (depth + prefix_len == key_len) {
                return recursive_iter(n, cb, data);
            }

            // if there is a full match, go deeper
            depth = depth + n->partial_len;
        }

        // Recursively search
        child = find_child(n, key[depth]);
        n = (child) ? *child : NULL;
        depth++;
    }
    return 0;
}


