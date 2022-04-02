#include <linux/fs.h>
#include "entry.h"
#include "super.h"
#include "nova.h"
#include "dedup.h"

/* 
* Author:Hsiao
* Assume the lock is acquired before calling
*/
entrynr_t nova_alloc_entry(struct super_block *sb)
{
    entrynr_t entrynr;
    struct nova_sb_info *sbi = NOVA_SB(sb);
    struct nova_entry_node *alloc_entry;

    mutex_lock(&sbi->free_list_mutex);
    alloc_entry = list_first_entry(&sbi->meta_free_list,struct nova_entry_node, link);
    list_del(&alloc_entry->link);
    entrynr = alloc_entry->entrynr;
    mutex_unlock(&sbi->free_list_mutex);

    return entrynr;
}

/*
* Author:Hsiao
* Assume the lock is acquired before calling
*/
int nova_free_entry(struct super_block *sb, entrynr_t entrynr)
{
    struct nova_sb_info *sbi = NOVA_SB(sb);
    struct nova_entry_node *free_entry;
    
    mutex_lock(&sbi->free_list_mutex);
    free_entry = &sbi->free_list_buf[entrynr];
    list_add_tail(&free_entry->link,&sbi->meta_free_list);
    mutex_unlock(&sbi->free_list_mutex);

    return 0;
}

/*
* Author:Hsiao
* init entry free list
*/
int nova_init_entry_list(struct super_block *sb){
    struct nova_sb_info *sbi = NOVA_SB(sb);
    size_t buf_sz;
    unsigned long i;
    struct nova_entry_node *i_node;

    buf_sz = sbi->num_blocks * sizeof(struct nova_entry_node);
    sbi->free_list_buf = vzalloc(buf_sz);
    if( sbi->free_list_buf == NULL) {
        vfree(sbi->free_list_buf);
        return -ENOMEM;
    }

    INIT_LIST_HEAD(&sbi->meta_free_list);
    mutex_init(&sbi->free_list_mutex);


    for(i = 0; i < sbi->num_blocks; ++i) {
        i_node = &sbi->free_list_buf[i];
        i_node->entrynr = i;
        list_add_tail(&i_node->link,&sbi->meta_free_list);
    }
    return 0;
}

/*
* Author:Hsiao
* free entry free list
*/
void nova_free_entry_list(struct super_block *sb) 
{
    struct nova_sb_info *sbi = NOVA_SB(sb);

    vfree(sbi->free_list_buf);
}
/**
 * @author
 * 
 */
int nova_calc_non_fin_stop(struct super_block *sb)
{
    struct nova_sb_info *sbi = NOVA_SB(sb);
    
    nova_info("%s is called", __func__);
    if(sbi->calc_non_fin_thread) {
        sbi->should_non_fin_thread_done = 1;
        kthread_stop(sbi->calc_non_fin_thread);
    }
    nova_info("%s goes end", __func__);
    return 0;
}

static void calc_non_fin_try_sleeping(struct nova_sb_info *sbi)
{
    DEFINE_WAIT(wait);
    nova_info("%s is called", __func__);
    prepare_to_wait(&sbi->calc_non_fin_wait, &wait, TASK_INTERRUPTIBLE);
    schedule();
    finish_wait(&sbi->calc_non_fin_wait, &wait);
    nova_info("%s is end", __func__);
}

static int nova_calc_non_fin(struct super_block *sb)
{
    struct nova_sb_info *sbi = NOVA_SB(sb);
    struct nova_pmm_entry *pentries, *pentry;
    struct nova_fp_weak fp_weak;
    u32 weak_idx;
    void *kmem;
    unsigned long idx;
    struct nova_hentry *hentry;

    pentries = nova_get_block(sb, nova_get_block_off(sb, sbi->metadata_start, NOVA_BLOCK_TYPE_4K));

    for(idx = 0; idx < sbi->num_entries; ++idx) {
        pentry = pentries + idx;
        if(pentry->flag == NON_FIN_FLAG) {
            if(pentry->blocknr != 0 && pentry->blocknr < sbi->num_blocks && sbi->blocknr_to_entry[pentry->blocknr] == idx) {
                kmem = nova_get_block(sb, pentry->blocknr);
                nova_fp_weak_calc(&sbi->nova_non_fin_calc_ctx, kmem, &fp_weak);
                pentry->fp_weak = fp_weak;
                nova_flush_buffer(pentry, sizeof(*pentry), true);
                weak_idx = (fp_weak.u32 & ((1 << sbi->num_entries_bits) - 1));
                hentry = nova_alloc_hentry(sb);
                hentry->entrynr = idx;
                hlist_add_head(&hentry->node, &sbi->weak_hash_table[weak_idx]);
                // sbi->weak_hash_table[weak_idx] = idx;
            }
        }
    }
    return 0;
}
/**
 * @author: Hsiao
 * 
 */
static int calc_non_fin(void *arg)
{
    struct super_block *sb = arg;
    struct nova_sb_info *sbi = NOVA_SB(sb);

    nova_dbg("Running calc non fin thread\n");

    for( ; ; ) {
        calc_non_fin_try_sleeping(sbi);

        if(kthread_should_stop())
            break;
        
        nova_calc_non_fin(sb);
        
        if (sbi->should_non_fin_thread_done) {
            break;
        }
    }
    nova_dbg("Exiting calc non fin thread\n");

    return 0;
}
/**
 * @author: Hsiao
 * 
 */
int nova_calc_non_fin_thread_init(struct super_block *sb)
{
    struct nova_sb_info *sbi = NOVA_SB(sb);
    int ret = 0;

    init_waitqueue_head(&sbi->calc_non_fin_wait);

    sbi->calc_non_fin_thread = kthread_run(calc_non_fin, sb, "nova_calc_non_fin");
    if( IS_ERR(sbi->calc_non_fin_thread)) {
        nova_info("Failed to start NOVA non_fin calculator thread.\n");
		ret = -1;
    }
    sbi->should_non_fin_thread_done = 0;
    nova_info("Start NOVA non_fin calculator thread.\n");
    return ret;
}

void wakeup_calc_non_fin(struct super_block *sb)
{
    struct nova_sb_info *sbi = NOVA_SB(sb);
    
    if(!waitqueue_active(&sbi->calc_non_fin_wait))
        return;
    nova_dbg("waking up the calc non fin thread");
    wake_up_interruptible(&sbi->calc_non_fin_wait);
}