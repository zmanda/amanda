#ifndef FIND_H
#define FIND_H

#include "diskfile.h"

#define DEFAULT_SORT_ORDER      "hkdlspbfw"

typedef struct find_result_s {
    struct find_result_s *next;
    char *timestamp;		/* dump timestamp */
    char *write_timestamp;
    char *hostname;
    char *diskname;
    char *storage;
    int   storage_id;
    int level;
    char *label;	/* holding filename for holding files */
    off_t filenum;
    char *status;	/* status of the part */
    char *dump_status;	/* status of the dump; should be identical for all parts in dump */
    char *message;	/* reason for dump_status; should be identical for all parts in dump */
    int partnum;	/* -1 for holding files */
    int totalparts;	/* -1 for holding files */
    double sec;		/* may be 0.0 for older log files or holding files */
    off_t bytes;	/* may be 0 for older log files, can be compressed */
    off_t kb;		/* may be 0 for older log files, can be compressed */
    off_t orig_kb;      /* native size */
    crc_t native_crc;
    crc_t client_crc;
    crc_t server_crc;
    void *user_ptr;
} find_result_t;

/* Finds /all/ dumps still on a volume. If diskqp is not NULL, then dumps
 * not matching any existing disklist entry will be added to diskqp and to
 * the global disklist. If diskqp is NULL, disks not matching existing
 * disklist entries will be skipped. See search_logfile below, which does
 * the dirty work for find_dump. */
find_result_t *find_dump(disklist_t* diskqp);

/* Return a list of unqualified filenames of logfiles for active
 * tapes.  Filenames are relative to the logdir.
 *
 * @returns: dynamically allocated, null-terminated strv
 */
char **find_log(void);

void sort_find_result(char *sort_order, find_result_t **output_find);
void sort_find_result_with_storage(char *sort_order, char **storage_list, find_result_t **output_find);
void print_find_result(find_result_t *output_find);
void free_find_result(find_result_t **output_find);
find_result_t *dump_exist(find_result_t *output_find, char *hostname,
                          char *diskname, char *datestamp, int level);
find_result_t *dumps_match(find_result_t *output_find, char *hostname,
                           char *diskname, char *datestamp, char *level,
                           int ok);
find_result_t *dumps_match_dumpspecs(find_result_t *output_find,
				     GSList *dumpspecs,
				     int ok);

/* This function looks in a particular log.xxx file for dumps. Returns TRUE
 * if something was found. This function also skips dumps whose disklist
 * entries are not marked 'todo'.
 * * output_find      : Put found dumps here.
 * * volume_label     : If not NULL, restrict the search to
 *                      dumps matching the given volume details.
 * * log_datestamp    : If not NULL, checks that this logfile is from this time.
 * * logfile          : Name of logfile in config dir.
 * * dynamic_disklist : If not NULL, adds disks not already in the global
 *                      disklist to the given disklist (and the global one).
 *                      If dynamic_disklist is NULL, skips disks not in the
 *                      global disklist.
 */
gboolean search_logfile(find_result_t **output_find, const char *volume_label,
                        const char *log_datestamp, const char *logfile,
                        disklist_t * dynamic_disklist);

/* return all dumps on holding disk; not really a search at all.
 *
 * * output_find      : Put found dumps here.
 * * dynamic_disklist : If not NULL, adds disks not already in the global
 *                      disklist to the given disklist (and the global one).
 *                      If dynamic_disklist is NULL, skips disks not in the
 *                      global disklist.
 */
void search_holding_disk(
	find_result_t **output_find,
	disklist_t * dynamic_disklist);

#endif	/* !FIND_H */
