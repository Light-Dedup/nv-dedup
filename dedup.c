#include <linux/fs.h>
#include "dedup.h"
#include "nova.h"
#include "entry.h"

inline bool cmp_fp_strong(struct nova_fp_strong *dst, struct nova_fp_strong *src) {
    return (dst->u64s[0] == src->u64s[0] && dst->u64s[1] == src->u64s[1] 
            && dst->u64s[2] == src->u64s[2] && dst->u64s[3] == src->u64s[3] );
}

unsigned long nova_alloc_block_write(struct super_block *sb,const char *data_buffer)
{
    int allocated;
    unsigned long blocknr;
    void *kmem;
	INIT_TIMING(memcpy_time);

    allocated = nova_new_data_block(sb, &blocknr ,ALLOC_INIT_ZERO);

	nova_dbg_verbose("%s: alloc %d blocks @ %lu\n", __func__,
					allocated, blocknr);

	if (allocated <= 0) {
        nova_dbg("%s alloc blocks failed %d\n", __func__,
					allocated);
	}

    kmem = nova_get_block(sb,nova_get_block_off(sb, blocknr, NOVA_BLOCK_TYPE_4K));
    
    NOVA_START_TIMING(memcpy_w_nvmm_t, memcpy_time);
	nova_memunlock_range(sb, kmem , PAGE_SIZE);
	memcpy_to_pmem_nocache(kmem , data_buffer, PAGE_SIZE);
	nova_memlock_range(sb, kmem , PAGE_SIZE);
	NOVA_END_TIMING(memcpy_w_nvmm_t, memcpy_time);

    return blocknr;
}

unsigned long nova_dedup_new_write(struct super_block *sb,const char* data_buffer)
{
    struct nova_sb_info *sbi = NOVA_SB(sb);
    struct nova_fp_weak fp_weak;
    struct nova_fp_strong fp_strong = {0},entry_fp_strong = {0};
    u32 weak_idx, strong_idx, entry_strong_idx;
    u64 weak_find_entry, strong_find_entry;
    void *kmem;
    struct nova_pmm_entry *weak_entry, *strong_entry, *pentries, *pentry;
    unsigned long blocknr;
    entrynr_t alloc_entry;
    bool flush_entry = false;// whether flush weak entry

    nova_fp_weak_calc(&sbi->nova_fp_weak_ctx, data_buffer,&fp_weak);
    weak_idx = hash_32(fp_weak.u32, sbi->num_entries_bits);
    weak_find_entry = sbi->weak_hash_table[weak_idx];
    if(weak_find_entry != 0) {
        /**
         *  Only when a match is found will a strong fingerprint like MD5
         *  be calculated to check if two chunks are truly the same or not.
         * from NV-Dedup
        */ 

       weak_entry = nova_get_block(sb, weak_find_entry);
       if(weak_entry->flag) {
           /**
            *  The sixth field is a 1 B flag to indicate 
            *  whether the strong fingerprint is valid or not.
            *  from NV-Dedup
            */
           // if the strong fingerprint is valid
           // assign it to entry_fp_strong

           entry_fp_strong = weak_entry->fp_strong;
       }else {
           /**
            * If a newlyarrived chunk has the same weak fingerprint as a stored chunk,
            *  NV-Dedup calculates the strong fingerprint of both chunks for further comparison.
            *  Then, NV-Dedup updates the entry of the stored chunk by adding the strong fingerprint.
            */
           kmem = nova_get_block(sb, weak_entry->blocknr);
           nova_fp_strong_calc(&sbi->nova_fp_strong_ctx, kmem, &fp_strong);

           weak_entry->flag = 1;
           weak_entry->fp_strong = fp_strong;
           flush_entry = true;

            strong_idx = hash_64(fp_strong.u64s[0],sbi->num_entries_bits);
            sbi->strong_hash_table[strong_idx] = weak_find_entry;
       }

       nova_fp_strong_calc(&sbi->nova_fp_strong_ctx, data_buffer, &entry_fp_strong);

       if(cmp_fp_strong(&fp_strong, &entry_fp_strong)) {
           // if both fingerprint are euqal
           blocknr = weak_entry->blocknr;
           ++weak_entry->refcount;
           flush_entry = true;
       } else {
           // if both fingerprint are not equal
           entry_strong_idx = hash_64(entry_fp_strong.u64s[0],sbi->num_entries_bits);
           strong_find_entry = sbi->strong_hash_table[entry_strong_idx];
           if(strong_find_entry != 0) {
               // if the corresponding strong fingerprint is found
               // add the refcount and return
               strong_entry = nova_get_block(sb, strong_find_entry);
               ++strong_entry->refcount;
               nova_flush_buffer(strong_entry,sizeof(*strong_entry),true);
               blocknr = strong_entry->blocknr;

           } else {
               // if the corresponding strong fingerprint is not found
               // alloc a new entry and write
               // add the strong fingerprint to strong fingerprint hash table
               alloc_entry = nova_alloc_entry(sb);
               pentries = nova_get_block(sb, nova_get_block_off(sb, sbi->metadata_start, NOVA_BLOCK_TYPE_4K));
               blocknr = nova_alloc_block_write(sb,data_buffer);
               pentry = pentries + alloc_entry;
               pentry->flag = 1;
               pentry->fp_weak = fp_weak;
               pentry->fp_strong = entry_fp_strong;
               pentry->blocknr = blocknr;
               pentry->refcount = 1;
               nova_flush_buffer(pentry, sizeof(*pentry), true);
               sbi->strong_hash_table[entry_strong_idx] = nova_get_addr_off(sbi, pentry);
               sbi->blocknr_to_entry[blocknr] = nova_get_addr_off(sbi, pentry);
           }

       }

       if(flush_entry) nova_flush_buffer(weak_entry,sizeof(*weak_entry),true);

    }else {
        // alloc an entry to record this nonexistent block
        alloc_entry = nova_alloc_entry(sb);

        blocknr = nova_alloc_block_write(sb,data_buffer);
        
        pentries = nova_get_block(sb, nova_get_block_off(sb, sbi->metadata_start, NOVA_BLOCK_TYPE_4K));
        pentry = pentries + alloc_entry;
        pentry->flag = false;
        pentry->fp_weak = fp_weak;
        pentry->blocknr = blocknr;
        pentry->refcount = 1;
        nova_flush_buffer(pentry, sizeof(*pentry),true);

        sbi->weak_hash_table[weak_idx] = nova_get_addr_off(sbi, pentry);
        sbi->blocknr_to_entry[blocknr] = nova_get_addr_off(sbi,pentry);
    }

    return blocknr;
}