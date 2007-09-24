#include <queueing.h>
#include <device.h>
#include <amanda.h>

static void ignore_signal(int signum) {
    static struct sigaction sigact, oldact;
    sigact.sa_sigaction = NULL;
    sigact.sa_handler = SIG_IGN;
    sigemptyset(&(sigact.sa_mask));
    sigact.sa_flags = SA_RESTART;
    sigact.sa_restorer = NULL;

    sigaction(signum, &sigact, &oldact);
    sigaction(signum, NULL, &sigact);
}

int main(void) {
    ignore_signal(SIGPIPE);

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
