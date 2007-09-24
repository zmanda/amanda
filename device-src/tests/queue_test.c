#include <queueing.h>
#include <device.h>
#include <amanda.h>

int main(void) {
    /* ignore SIGPIPE */
    signal(SIGPIPE, SIG_IGN);

    /* Comment out this line to disable threads. */
    device_api_init();

    /* The integer here is the block size to use. Set it to something
     * bigger for better performance. */
    return !do_consumer_producer_queue_full(fd_read_producer,
                                            GINT_TO_POINTER(0),
                                            fd_write_consumer,
                                            GINT_TO_POINTER(1),
                                            1,  /* Block size */
                                            10, /* Buffer size. */
                                            STREAMING_REQUIREMENT_DESIRED);
}
