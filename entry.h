#ifndef __NOVA_ENTRY_H
#define __NOVA_ENTRY_H

#include "fingerprint.h"

typedef uint64_t entrynr_t;

#define NON_FIN_FLAG 0xFF
#define FP_WEAK_FLAG 0xFE
#define FP_STRONG_FLAG 0xEF

struct nova_pmm_entry {
    uint64_t tag_TXID;
    uint64_t refcount;
    uint64_t blocknr;
    struct nova_fp_strong fp_strong;
    struct nova_fp_weak fp_weak;
    uint8_t flag; // 
    uint8_t padding[3];
};


_Static_assert(sizeof(struct nova_pmm_entry) == 64, "Metadata Entry not 64B!");

struct nova_entry_node
{
    struct list_head link;
    entrynr_t entrynr;
};

extern entrynr_t nova_alloc_entry(struct super_block *sb);
extern int nova_init_entry_list(struct super_block *sb);
extern int nova_free_entry(struct super_block *sb,entrynr_t entry);
extern void nova_free_entry_list(struct super_block *sb) ;
// entrynr_t nova_alloc_free_entry(struct super_block *sb);

extern int nova_calc_non_fin_thread_init(struct super_block *sb);
extern int nova_calc_non_fin_stop(struct super_block *sb);
extern void wakeup_calc_non_fin(struct super_block *sb);
#endif // __NOVA_ENTRY_H