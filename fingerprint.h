#ifndef FINGERPRINT_H_
#define FINGERPRINT_H_

#include <linux/types.h>
#include <crypto/hash.h>
#include <crypto/skcipher.h>
#include "stats.h"

#define NOVA_FP_STRONG_CTX_BUF_SIZE 256

struct nova_fp_hash_ctx {
	struct crypto_shash *alg;
};


struct nova_fp_strong {
	union {
		uint64_t u64s[4];
	};
};

struct nova_fp_weak {
	union {
		uint32_t u32;
	};
};

_Static_assert(sizeof(struct nova_fp_strong) == 32, "Strong Fingerprint not 32B!");
_Static_assert(sizeof(struct nova_fp_weak) == 4, "Weak Fingerprint not 32B!");

static inline int nova_fp_strong_ctx_init(struct nova_fp_hash_ctx *ctx) {
	struct crypto_shash *alg = crypto_alloc_shash("md5", 0, 0);
	if (IS_ERR(alg))
		return PTR_ERR(alg);
	if (crypto_shash_descsize(alg) > NOVA_FP_STRONG_CTX_BUF_SIZE) {
		crypto_free_shash(alg);
		return -EINVAL;
	}
	ctx->alg = alg;
	return 0;
}

static inline int nova_fp_weak_ctx_init(struct nova_fp_hash_ctx *ctx) {
	struct crypto_shash *alg = crypto_alloc_shash("crc32", 0, 0);
	if (IS_ERR(alg))
		return PTR_ERR(alg);
	if (crypto_shash_descsize(alg) > NOVA_FP_STRONG_CTX_BUF_SIZE) {
		crypto_free_shash(alg);
		return -EINVAL;
	}
	ctx->alg = alg;
	return 0;
}

static inline void nova_fp_hash_ctx_free(struct nova_fp_hash_ctx *ctx) {
	crypto_free_shash(ctx->alg);
}

static inline int nova_fp_strong_calc(struct nova_fp_hash_ctx *fp_ctx, const void *addr, struct nova_fp_strong *fp)
{
	struct shash_desc *shash_desc;
	int ret;

	shash_desc = kmalloc(sizeof(struct shash_desc) +
		crypto_shash_descsize(fp_ctx->alg), GFP_KERNEL);
	if (shash_desc == NULL)
		return -ENOMEM;
	shash_desc->tfm = fp_ctx->alg;
	ret = crypto_shash_digest(shash_desc, (const void*)addr, 4096, (void*)fp->u64s);
	kfree(shash_desc);

	return ret;
}

static inline int nova_fp_weak_calc(struct nova_fp_hash_ctx *fp_ctx, const void *addr, struct nova_fp_weak *fp)
{
	struct shash_desc *shash_desc;
	int ret;

	shash_desc = kmalloc(sizeof(struct shash_desc) +
		crypto_shash_descsize(fp_ctx->alg), GFP_KERNEL);
	if (shash_desc == NULL)
		return -ENOMEM;
	shash_desc->tfm = fp_ctx->alg;
	ret = crypto_shash_digest(shash_desc, (const void*)addr, 4096, (void*)&fp->u32);
	kfree(shash_desc);

	return ret;
}

#endif // FINGERPRINT_H_