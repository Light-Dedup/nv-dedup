#include <linux/fs.h>
#include "dedup.h"
#include "nova.h"
#include "entry.h"
#include <linux/random.h>

inline bool cmp_fp_strong(struct nova_fp_strong *dst, struct nova_fp_strong *src) {
    return (dst->u64s[0] == src->u64s[0] && dst->u64s[1] == src->u64s[1] 
            && dst->u64s[2] == src->u64s[2] && dst->u64s[3] == src->u64s[3] );
}

int nova_alloc_block_write(struct super_block *sb,const char *data_buffer, unsigned long *blocknr)
{
    int allocated = 0;
    void *kmem;
	INIT_TIMING(memcpy_time);

    allocated = nova_new_data_block(sb, blocknr ,ALLOC_INIT_ZERO);

	nova_dbg("%s: alloc %d blocks @ %lu\n", __func__,
					allocated, *blocknr);

	if (allocated < 0) {
        nova_dbg("%s alloc blocks failed %d\n", __func__,
					allocated);
        goto out;
	}

    kmem = nova_get_block(sb,nova_get_block_off(sb, *blocknr, NOVA_BLOCK_TYPE_4K));
    
    NOVA_START_TIMING(memcpy_w_nvmm_t, memcpy_time);
	nova_memunlock_range(sb, kmem , PAGE_SIZE);
	memcpy_to_pmem_nocache(kmem , data_buffer, PAGE_SIZE);
	nova_memlock_range(sb, kmem , PAGE_SIZE);
	NOVA_END_TIMING(memcpy_w_nvmm_t, memcpy_time);

out:
    return allocated;
}


int nova_dedup_str_fin(struct super_block *sb, const char* data_buffer,unsigned long *blocknr) 
{
    /**
     *  Str_Fin method calculates a single strong fingerprint for data 
     */

    /**
     * we alter the Str_Fin method to calculate both weak and strong fingerprints for a chunk
     *  at a high duplication ratio so that no weak fingerprint is missed with Str_Fin.
     */

    struct nova_sb_info *sbi = NOVA_SB(sb);
    struct nova_fp_weak fp_weak;
    struct nova_fp_strong fp_strong = {0}, entry_fp_strong = {0};
    struct nova_pmm_entry *pentries, *pentry;
    u32 weak_idx;
    u64 strong_idx;
    entrynr_t weak_find_entry, strong_find_entry, alloc_entry;
    int allocated = 0;
    // void *kmem;
    INIT_TIMING(weak_fp_calc_time);
    INIT_TIMING(strong_fp_calc_time);
    INIT_TIMING(hash_table_time);

    pentries = nova_get_block(sb, nova_get_block_off(sb, sbi->metadata_start, NOVA_BLOCK_TYPE_4K));

    NOVA_START_TIMING(weak_fp_calc_t, weak_fp_calc_time);
    nova_fp_weak_calc(&sbi->nova_fp_weak_ctx, data_buffer,&fp_weak);
    NOVA_END_TIMING(weak_fp_calc_t, weak_fp_calc_time);

    NOVA_START_TIMING(strong_fp_calc_t, strong_fp_calc_time);
    nova_fp_strong_calc(&sbi->nova_fp_strong_ctx, data_buffer, &fp_strong);
    NOVA_END_TIMING(strong_fp_calc_t, strong_fp_calc_time);

    NOVA_START_TIMING(hash_table_t, hash_table_time);
    weak_idx = hash_32(fp_weak.u32, sbi->num_entries_bits);
    weak_find_entry = sbi->weak_hash_table[weak_idx];
    NOVA_END_TIMING(hash_table_t, hash_table_time);
    
    NOVA_START_TIMING(hash_table_t, hash_table_time);
    strong_idx = hash_64(entry_fp_strong.u64s[0],sbi->num_entries_bits);
    strong_find_entry = sbi->strong_hash_table[strong_idx];
    NOVA_END_TIMING(hash_table_t, hash_table_time);

    if( strong_find_entry != 0 ) {
        pentry = pentries + strong_find_entry;
        ++pentry->refcount;
        pentry->fp_weak = fp_weak;
        pentry->flag = FP_STRONG_FLAG;
        ++sbi->dup_block;
        *blocknr = pentry->blocknr;
        allocated = 1;
    }else {
        alloc_entry = nova_alloc_entry(sb);
        allocated = nova_alloc_block_write(sb,data_buffer,blocknr);
        if(allocated < 0) 
            goto out;

        pentry = pentries + alloc_entry;
        pentry->flag = FP_STRONG_FLAG;
        pentry->blocknr = *blocknr;
        pentry->fp_strong = fp_strong;
        pentry->fp_weak = fp_weak;
        pentry->refcount = 1;
        sbi->strong_hash_table[strong_idx] = alloc_entry;
        sbi->blocknr_to_entry[*blocknr] = alloc_entry;
        strong_find_entry = alloc_entry;
    }

    nova_flush_buffer(pentry, sizeof(*pentry), true);

    if(weak_find_entry == 0) {
        sbi->weak_hash_table[weak_idx] = strong_find_entry;
    }

out:
    return allocated;
}

int nova_dedup_weak_str_fin(struct super_block *sb, const char* data_buffer, unsigned long *blocknr) 
{
    /**
     * w_s_Fin method calculates a weak fingerprint for a data chunk
     *  and uses a strong fingerprint to 
     * check data that are not surely identified by comparing weak fingerprinting. 
     */
    struct nova_sb_info *sbi = NOVA_SB(sb);
    struct nova_fp_weak fp_weak;
    struct nova_fp_strong fp_strong = {0}, entry_fp_strong = {0};
    struct nova_pmm_entry *pentries, *weak_entry, *strong_entry, *pentry;
    u32 weak_idx;
    u64 strong_idx, entry_strong_idx;
    entrynr_t weak_find_entry, strong_find_entry, alloc_entry;
    int allocated = 0;
    void *kmem;
    bool flush_entry = false;
    INIT_TIMING(weak_fp_calc_time);
    INIT_TIMING(strong_fp_calc_time);
    INIT_TIMING(hash_table_time);

    pentries = nova_get_block(sb, nova_get_block_off(sb, sbi->metadata_start, NOVA_BLOCK_TYPE_4K));

    NOVA_START_TIMING(weak_fp_calc_t, weak_fp_calc_time);
    nova_fp_weak_calc(&sbi->nova_fp_weak_ctx, data_buffer,&fp_weak);
    NOVA_END_TIMING(weak_fp_calc_t, weak_fp_calc_time);

    NOVA_START_TIMING(hash_table_t, hash_table_time);
    weak_idx = hash_32(fp_weak.u32, sbi->num_entries_bits);
    weak_find_entry = sbi->weak_hash_table[weak_idx];
    NOVA_END_TIMING(hash_table_t, hash_table_time);

    if(weak_find_entry != 0) {
        /**
         * If a newlyarrived chunk has the same weak fingerprint as a stored chunk
         *  NV-Dedup calculates the strong fingerprint of both chunks for further comparison. 
         * Then, NV-Dedup updates the entry of the stored chunk by adding the strong fingerprint.
         */

        weak_entry = pentries + weak_find_entry;
        
        if(weak_entry->flag == FP_STRONG_FLAG) {
             /**
            *  The sixth field is a 1 B flag to indicate 
            *  whether the strong fingerprint is valid or not.
            *  from NV-Dedup
            */
           // if the strong fingerprint is valid
           // assign it to entry_fp_strong

           entry_fp_strong = weak_entry->fp_strong;
        }else if(weak_entry->flag == FP_WEAK_FLAG){
            kmem = nova_get_block(sb, weak_entry->blocknr);
            NOVA_START_TIMING(strong_fp_calc_t, strong_fp_calc_time);
            nova_fp_strong_calc(&sbi->nova_fp_strong_ctx, kmem, &fp_strong);
            NOVA_END_TIMING(strong_fp_calc_t, strong_fp_calc_time);
            
            weak_entry->flag = FP_STRONG_FLAG;
            weak_entry->fp_strong = fp_strong;
            flush_entry = true;
            
            strong_idx = hash_64(fp_strong.u64s[0],sbi->num_entries_bits);
            sbi->strong_hash_table[strong_idx] = weak_find_entry;
        }

        NOVA_START_TIMING(strong_fp_calc_t, strong_fp_calc_time);
        nova_fp_strong_calc(&sbi->nova_fp_strong_ctx, data_buffer, &entry_fp_strong);
        NOVA_END_TIMING(strong_fp_calc_t, strong_fp_calc_time);

        if(cmp_fp_strong(&fp_strong, &entry_fp_strong)) {
            *blocknr = weak_entry->blocknr;
            ++weak_entry->refcount;
            flush_entry = true;
            ++sbi->dup_block;
            allocated = 1;
        } else {
            NOVA_START_TIMING(hash_table_t, hash_table_time);
            entry_strong_idx = hash_64(entry_fp_strong.u64s[0],sbi->num_entries_bits);
            strong_find_entry = sbi->strong_hash_table[entry_strong_idx];
            NOVA_END_TIMING(hash_table_t, hash_table_time);
            
            if(strong_find_entry != 0) {
                // if the corresponding strong fingerprint is found
                // add the refcount and return
                strong_entry = pentries + strong_find_entry;
                ++strong_entry->refcount;
                nova_flush_buffer(strong_entry,sizeof(*strong_entry),true);
                *blocknr = strong_entry->blocknr;
                allocated = 1;
                ++sbi->dup_block;
            } else {
                // if the corresponding strong fingerprint is not found
                // alloc a new entry and write
                // add the strong fingerprint to strong fingerprint hash table
                alloc_entry = nova_alloc_entry(sb);
                allocated = nova_alloc_block_write(sb,data_buffer,blocknr);
                if(allocated < 0)
                    goto out;
                
                pentry = pentries + alloc_entry;
                pentry->flag = FP_STRONG_FLAG;
                pentry->fp_weak = fp_weak;
                pentry->fp_strong = entry_fp_strong;
                pentry->blocknr = *blocknr;
                pentry->refcount = 1;
                nova_flush_buffer(pentry, sizeof(*pentry), true);
                sbi->strong_hash_table[entry_strong_idx] = alloc_entry;
                sbi->blocknr_to_entry[*blocknr] = alloc_entry;
            }
        }

        if(flush_entry) nova_flush_buffer(weak_entry, sizeof(*weak_entry), true);

    }else {
        /**
         * If the weak fingerprint is not found in the metadata table, 
         * NV-Dedup will deem the chunk to be non-existent 
         * and the calculation of strong fingerprint needs not be done for the chunk
         */

        alloc_entry = nova_alloc_entry(sb);

        allocated = nova_alloc_block_write(sb,data_buffer,blocknr);
        if(allocated < 0 )
            goto out;
        
        pentry = pentries + alloc_entry;
        pentry->flag = FP_WEAK_FLAG;
        pentry->fp_weak = fp_weak;
        pentry->blocknr = *blocknr;
        pentry->refcount = 1;
        nova_flush_buffer(pentry, sizeof(*pentry),true);

        sbi->weak_hash_table[weak_idx] = alloc_entry;
        sbi->blocknr_to_entry[*blocknr] = alloc_entry;
    }

out:
    return allocated;
}

int nova_dedup_non_fin(struct super_block *sb, const char* data_buffer, unsigned long* blocknr)
{
    struct nova_sb_info *sbi = NOVA_SB(sb);
    struct nova_pmm_entry *pentries, *pentry;
    entrynr_t alloc_entry;
    int allocated = 0;
    
    alloc_entry = nova_alloc_entry(sb);
    allocated = nova_alloc_block_write(sb, data_buffer,blocknr);
    if(allocated < 0)
        goto out;
    pentries = nova_get_block(sb, nova_get_block_off(sb, sbi->metadata_start,NOVA_BLOCK_TYPE_4K));
    pentry = pentries + alloc_entry;
    pentry->blocknr = *blocknr;
    pentry->flag = NON_FIN_FLAG;
    pentry->refcount = 1;
    nova_flush_buffer(pentry, sizeof(*pentry), true);
    sbi->blocknr_to_entry[*blocknr] = alloc_entry;

out:
    return allocated;
}

int nova_dedup_new_write(struct super_block *sb,const char* data_buffer, unsigned long *blocknr)
{
    struct nova_sb_info *sbi = NOVA_SB(sb);
    u32 dup_block = 0;
    u32 dup_mode = 0;
    unsigned long randomNum;

    ++sbi->cur_block;
    if(sbi->cur_block >= SAMPLE_BLOCK) {
        // if(sbi->dedup_mode == NON_FIN) {
        //     wakeup_calc_non_fin(sb);
        // }
        dup_block = sbi->dup_block;
        if(dup_block > STR_FIN_THRESH) {
            sbi->dedup_mode = STR_FIN;
        }else if(dup_block > NON_FIN_THRESH) {
            sbi->dedup_mode = WEAK_STR_FIN;
        }else {
            get_random_bytes(&randomNum, sizeof(unsigned long));
            if(randomNum & 1) {
                sbi->dedup_mode = NON_FIN;
            }
            else {
                sbi->dedup_mode = WEAK_STR_FIN;
            }
        }
        sbi->cur_block = 0;
        sbi->dup_block = 0;
    }

    dup_mode = sbi->dedup_mode;
    if(dup_mode & NON_FIN) {
        return nova_dedup_non_fin(sb, data_buffer, blocknr);
    }else if(dup_mode & WEAK_STR_FIN) {
        return nova_dedup_weak_str_fin(sb, data_buffer, blocknr);
    }else if(dup_mode & STR_FIN) {
        return nova_dedup_str_fin(sb, data_buffer, blocknr);
    }else {
        return -ESRCH;
    }
}