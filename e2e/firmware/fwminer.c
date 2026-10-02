/*
 * fwminer: find a share for one Stratum job using the Bitaxe firmware's own
 * job-construction code.
 *
 * Usage:
 *   fwminer <coinbase1> <coinbase2> <extranonce1> <extranonce2_len>
 *           <prevhash> <version> <nbits> <ntime> <version_mask>
 *           <min_difficulty> <extranonce2_counter> [merkle_branch ...]
 *
 * All values are the strings from mining.subscribe / mining.notify. Output:
 *   <extranonce2> <ntime> <nonce> <version_bits> <header_hex> <difficulty>
 * with ntime, nonce and version_bits formatted exactly as the firmware puts
 * them into mining.submit.
 *
 * Everything that decides what gets hashed is done by firmware functions:
 * extranonce_2_generate, calculate_coinbase_tx_hash,
 * calculate_merkle_root_hash, construct_bm_job, increment_bitmask and
 * test_nonce_value.
 */
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mining.h"
#include "stratum_api.h"
#include "utils.h"

int main(int argc, char **argv)
{
    if (argc < 12) {
        fprintf(stderr, "fwminer: not enough arguments\n");
        return 2;
    }
    const char *coinbase1 = argv[1];
    const char *coinbase2 = argv[2];
    const char *extranonce1 = argv[3];
    uint32_t extranonce2_len = (uint32_t)strtoul(argv[4], NULL, 10);
    uint32_t version_mask = (uint32_t)strtoul(argv[9], NULL, 16);
    double min_difficulty = strtod(argv[10], NULL);
    uint64_t counter = strtoull(argv[11], NULL, 10);
    int n_branches = argc - 12;

    if (extranonce2_len == 0 || extranonce2_len > MAX_EXTRANONCE_2_LEN || n_branches > MAX_MERKLE_BRANCHES) {
        fprintf(stderr, "fwminer: unsupported extranonce2 length or branch count\n");
        return 2;
    }

    /* The same parsing as the firmware's mining.notify handler. */
    mining_notify notify;
    memset(&notify, 0, sizeof(notify));
    notify.prev_block_hash = argv[5];
    notify.version = (uint32_t)strtoul(argv[6], NULL, 16);
    notify.target = (uint32_t)strtoul(argv[7], NULL, 16);
    notify.ntime = (uint32_t)strtoul(argv[8], NULL, 16);

    uint8_t branches[MAX_MERKLE_BRANCHES][32];
    for (int i = 0; i < n_branches; i++) {
        if (hex2bin(argv[12 + i], branches[i], 32) != 32) {
            fprintf(stderr, "fwminer: bad merkle branch\n");
            return 2;
        }
    }

    char extranonce2[MAX_EXTRANONCE_2_LEN * 2 + 1];
    extranonce_2_generate(counter, extranonce2_len, extranonce2);

    uint8_t coinbase_hash[32];
    calculate_coinbase_tx_hash(coinbase1, coinbase2, extranonce1, extranonce2, coinbase_hash);

    uint8_t merkle_root[32];
    calculate_merkle_root_hash(coinbase_hash, (const uint8_t (*)[32])branches, n_branches, merkle_root);

    bm_job job;
    memset(&job, 0, sizeof(job));
    construct_bm_job(&notify, merkle_root, version_mask, min_difficulty, &job);

    /* The ASIC walks nonces and rolls the version; do the same in software. */
    uint32_t rolled_version = job.version;
    for (int roll = 0; roll < 4096; roll++) {
        for (uint32_t nonce = 0; nonce < 100000; nonce++) {
            double diff = test_nonce_value(&job, nonce, rolled_version);
            if (diff < min_difficulty) {
                continue;
            }
            /* Rebuild the header exactly as test_nonce_value hashed it. */
            uint8_t header[80];
            memcpy(header, &rolled_version, 4);
            reverse_32bit_words(job.prev_block_hash, header + 4);
            reverse_32bit_words(job.merkle_root, header + 36);
            memcpy(header + 68, &job.ntime, 4);
            memcpy(header + 72, &job.target, 4);
            memcpy(header + 76, &nonce, 4);
            char header_hex[161];
            bin2hex(header, 80, header_hex, sizeof(header_hex));

            /* Formatting as in STRATUM_V1_submit_share. */
            printf("%s %08" PRIx32 " %08" PRIx32 " %08" PRIx32 " %s %.17g\n",
                   extranonce2, job.ntime, nonce, rolled_version ^ job.version, header_hex, diff);
            return 0;
        }
        if (version_mask == 0) {
            break;
        }
        rolled_version = increment_bitmask(rolled_version, version_mask);
    }
    fprintf(stderr, "fwminer: no share found\n");
    return 1;
}
