#include "taper-source.h"

typedef struct {
    guint64 tape_size;
    guint64 tape_used;
    ConsumerFunctor consumer;
    gpointer consumer_data;
} tape_info_t;

/* A ConsumerFunctor. */
static ssize_t size_limited_consumer(gpointer user_data, queue_buffer_t * buffer) {
    tape_info_t * info = user_data;
    ssize_t result;
    
    result = info->consumer(info->consumer_data, buffer);

    info->tape_used += result;
    if (info->tape_size < info->tape_used)
        return -1;
    else
        return result;
}

int main(int argc, char ** argv) {
    TaperSource * source;
    tape_info_t info;
    
    glib_init();

    switch (argc) {
    default:
        g_fprintf(stderr, "USAGE: %16s volume-size ( holding-disk-file splitsize | \n"
               "                                          "
               "split-disk-buffer splitsize\n"
               "                                          "
               "fallback-splitsize )\n",
               basename(argv[0]));
        return EXIT_FAILURE;
    case 4: {
        /* FILE-WRITE */
        guint64 splitsize = strtod(argv[3], NULL);
        
        source = taper_source_new("", FILE_WRITE, argv[2], -1, NULL, 
                                  splitsize, 0);
    }
        break;
    case 5: {
        guint64 splitsize, fallback_splitsize;
        splitsize = strtod(argv[3], NULL);
        fallback_splitsize = strtod(argv[4], NULL);
        
        source = taper_source_new("", PORT_WRITE, NULL, STDIN_FILENO,
                                  argv[2][0] == '\0' ? NULL : argv[2],
                                  splitsize, fallback_splitsize);
    }
        break;
    }

    if (source == NULL)
        return EXIT_FAILURE;

    info.tape_used = 0;
    info.tape_size = strtod(argv[1], NULL);
    info.consumer = fd_write_consumer;
    info.consumer_data = GINT_TO_POINTER(STDOUT_FILENO);

    for (;;) {
        gboolean success = do_consumer_producer_queue(taper_source_producer,
                                                      source,
                                                      size_limited_consumer,
                                                      &info);

        if (success) {
            if (taper_source_get_end_of_data(source)) {
                g_fprintf(stderr, "Got end of data.\n");
                return EXIT_SUCCESS;
            } else if (taper_source_get_end_of_part(source)) {
                taper_source_start_new_part(source);
                g_fprintf(stderr, "Finished part. Starting new one.\n");
                continue;
            } else {
                g_fprintf(stderr, "Read error.\n");
                return EXIT_FAILURE;
            }
        } else {
            /* Write or read error. (we can't tell) */
            info.tape_used = 0;
            if (taper_source_seek_to_part_start(source)) {
                g_fprintf(stderr, "Retrying a part.\n");
                continue;
            } else {
                g_fprintf(stderr, "Couldn't seek. Dying.\n");
                return EXIT_FAILURE;
            }
        }
    }

    g_assert_not_reached();
}
