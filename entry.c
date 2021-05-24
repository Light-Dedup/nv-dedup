#include <linux/fs.h>
#include "entry.h"
#include "super.h"
#include "nova.h"

/* 
* Author:Hsiao
* Assume the lock is acquired before calling
*/
entrynr_t nova_alloc_entry(struct super_block *sb)
{
    entrynr_t entrynr;
    struct nova_sb_info *sbi = NOVA_SB(sb);

    BUG_ON( kfifo_out(&sbi->meta_free_list, &entrynr, sizeof(entrynr_t)) != sizeof(entrynr_t) );

    return entrynr;
}

/*
* Author:Hsiao
* Assume the lock is acquired before calling
*/
int nova_free_entry(struct super_block *sb, entrynr_t entrynr)
{
    struct nova_sb_info *sbi = NOVA_SB(sb);
    
    BUG_ON( kfifo_in(&sbi->meta_free_list, &entrynr, sizeof(entrynr)) != sizeof(entrynr) ); 

    return 0;
}

/*
* Author:Hsiao
* init entry free list
*/
int nova_init_entry_list(struct super_block *sb){
    struct nova_sb_info *sbi = NOVA_SB(sb);
    size_t buf_sz;
    char *buf;
    int ret;
    unsigned long i;

    buf_sz = sbi->num_blocks * sizeof(entrynr_t);
    buf = vzalloc(buf_sz);
    if( buf == NULL) {
        vfree(buf);
        return -ENOMEM;
    }

    ret = kfifo_init(&sbi->meta_free_list, buf, buf_sz);

    if(ret) {
        vfree(buf);
        return ret;
    }

    for(i = 0; i < sbi->num_blocks; ++i) {
        BUG_ON( kfifo_in(&sbi->meta_free_list, &i, sizeof(i)) == 0 );
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

    vfree(sbi->meta_free_list.kfifo.data);
}
