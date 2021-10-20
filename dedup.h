#ifndef __NOVA_DEDUP_H
#define __NOVA_DEDUP_H

#include <linux/types.h>
#include "entry.h"

struct nova_hentry{
    struct hlist_node node;
    entrynr_t entrynr;
};

extern int nova_dedup_new_write(struct super_block *sb,const char* data_buffer, unsigned long *blocknr);

struct nova_hentry *nova_find_in_weak_hlist(struct super_block *sb, struct hlist_head *hlist, struct nova_fp_weak *fp_weak);

struct nova_hentry *nova_find_in_strong_hlist(struct super_block *sb, struct hlist_head *hlist, struct nova_fp_strong *fp_strong);

struct nova_hentry *nova_alloc_hentry(void);

#endif