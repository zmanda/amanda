#ifndef FIND_H
#define FIND_H

#include "diskfile.h"

#define DEFAULT_SORT_ORDER      "hkdlpb"

typedef struct find_result_s {
    struct find_result_s *next;
    char *timestamp;
    char *hostname;
    char *diskname;
    int level;
    char *label;
    off_t filenum;
    char *status;
    char *partnum;
    void *user_ptr;
} find_result_t;

find_result_t *find_dump(int dyna_disklist, disklist_t* diskqp);
char **find_log(void);
void sort_find_result(char *sort_order, find_result_t **output_find);
void print_find_result(find_result_t *output_find);
void free_find_result(find_result_t **output_find);
find_result_t *dump_exist(find_result_t *output_find, char *hostname, char *diskname, char *datestamp, int level);
find_result_t *dumps_match(find_result_t *output_find, char *hostname, char *diskname, char *datestamp, char *level, int ok);
#endif	/* !FIND_H */
