#ifndef __NOVA_DEDUP_H
#define __NOVA_DEDUP_H

#include <linux/types.h>

extern unsigned long nova_dedup_new_write(struct super_block *sb,const char* data_buffer);

#endif