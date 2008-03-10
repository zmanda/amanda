/*
 * Copyright (c) 2005 Zmanda, Inc.  All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1 as
 * published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 *
 * Contact information: Zmanda Inc., 505 N Mathlida Ave, Suite 120
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#include "semaphore.h"
#include "amanda.h"

/*
 * test that decrement waits properly
 */

struct test_decr_wait_data {
    semaphore_t *sem;
    gboolean increment_called;
};

static gpointer
test_decr_wait_thread(gpointer datap)
{
    struct test_decr_wait_data *data = datap;

    /* should block */
    semaphore_decrement(data->sem, 20);

    /* if increment hasn't been called yet, that's an error. */
    if (!data->increment_called)
	return GINT_TO_POINTER(0);

    return GINT_TO_POINTER(1);
}

static gboolean
test_decr_wait(void)
{
    GThread *th;
    struct test_decr_wait_data data = {
	semaphore_new_with_value(10),
	FALSE
    };
    int rv;

    /* die after 10 seconds (default signal disposition is to fail) */
    alarm(10);

    th = g_thread_create(test_decr_wait_thread, (gpointer)&data, TRUE, NULL);

    /* sleep to give semaphore_decrement() a chance to block (or not). */
    g_usleep(G_USEC_PER_SEC / 4);

    /* and then increment the semaphore enough that the decrement can succeed */
    data.increment_called = TRUE;
    semaphore_increment(data.sem, 10);

    /* join the thread and see how it fared. */
    rv = GPOINTER_TO_INT(g_thread_join(th));

    semaphore_free(data.sem);

    if (rv == 1) {
	printf(" PASS: semaphore-test.test_decr_wait\n");
	return TRUE;
    } else {
	printf(" FAIL: semaphore-test.test_decr_wait\n");
	return FALSE;
    }
}


/*
 * test that semaphore_wait_empty waits properly
 */

static gpointer
test_wait_empty_thread(gpointer datap)
{
    semaphore_t *sem = datap;

    /* should block */
    semaphore_decrement(sem, 20);

    /* value should be 10 now (decremented from 30) */
    if (sem->value != 10)
	return GINT_TO_POINTER(1);

    /* sleep for a bit */
    g_usleep(G_USEC_PER_SEC / 4);

    /* decrement those last 10, which should trigger the zero */
    semaphore_decrement(sem, 10);

    return GINT_TO_POINTER(0);
}

static gboolean
test_wait_empty(void)
{
    GThread *th;
    semaphore_t *sem = semaphore_new_with_value(10);
    int rv;

    /* die after 10 seconds (default signal disposition is to fail) */
    alarm(10);

    th = g_thread_create(test_wait_empty_thread, (gpointer)sem, TRUE, NULL);

    /* sleep to give semaphore_decrement() a chance to block (or not). */
    g_usleep(G_USEC_PER_SEC / 4);

    /* add another 10, so decrement can hit zero next time it's called */
    semaphore_increment(sem, 10);

    /* and wait on the semaphore emptying */
    semaphore_wait_empty(sem);

    /* join the thread and see how it fared. */
    rv = GPOINTER_TO_INT(g_thread_join(th));

    semaphore_free(sem);

    if (rv == 1) {
	printf(" PASS: semaphore-test.test_wait_empty\n");
	return TRUE;
    } else {
	printf(" FAIL: semaphore-test.test_wait_empty\n");
	return FALSE;
    }
}

/*
 * test that semaphore_force_adjust correctly wakes both
 * semaphore_decrement and semaphore_wait_empty.
 */

static gpointer
test_force_adjust_thread(gpointer datap)
{
    semaphore_t *sem = datap;

    /* this should block */
    semaphore_decrement(sem, 20);

    /* and this should block, too - it's fun */
    semaphore_wait_empty(sem);

    return NULL;
}

static gboolean
test_force_adjust(void)
{
    GThread *th;
    semaphore_t *sem = semaphore_new_with_value(10);

    /* die after 10 seconds (default signal disposition is to fail) */
    alarm(10);

    th = g_thread_create(test_force_adjust_thread, (gpointer)sem, TRUE, NULL);

    /* sleep to give semaphore_decrement() a chance to block (or not). */
    g_usleep(G_USEC_PER_SEC / 4);

    /* add another 20, so decrement can proceed, but leave the value at 10 */
    semaphore_force_adjust(sem, 20);

    /* sleep to give semaphore_wait_empty() a chance to block (or not). */
    g_usleep(G_USEC_PER_SEC / 4);

    /* and empty out the semaphore */
    semaphore_force_adjust(sem, -10);

    g_thread_join(th);

    semaphore_free(sem);

    /* it we didn't hang yet, it's all good */
    printf(" PASS: semaphore-test.test_force_adjust\n");
    return TRUE;
}

/*
 * test that semaphore_force_set correctly wakes both
 * semaphore_decrement and semaphore_wait_empty.
 */

static gpointer
test_force_set_thread(gpointer datap)
{
    semaphore_t *sem = datap;

    /* this should block */
    semaphore_decrement(sem, 20);

    /* and this should block, too - it's fun */
    semaphore_wait_empty(sem);

    return NULL;
}

static gboolean
test_force_set(void)
{
    GThread *th;
    semaphore_t *sem = semaphore_new_with_value(10);

    /* die after 10 seconds (default signal disposition is to fail) */
    alarm(10);

    th = g_thread_create(test_force_set_thread, (gpointer)sem, TRUE, NULL);

    /* sleep to give semaphore_decrement() a chance to block (or not). */
    g_usleep(G_USEC_PER_SEC / 4);

    /* set it to 30, so decrement can proceed, but leave the value at 10 */
    semaphore_force_set(sem, 30);

    /* sleep to give semaphore_wait_empty() a chance to block (or not). */
    g_usleep(G_USEC_PER_SEC / 4);

    /* and empty out the semaphore */
    semaphore_force_set(sem, 0);

    g_thread_join(th);

    semaphore_free(sem);

    /* it we didn't hang yet, it's all good */
    printf(" PASS: semaphore-test.test_force_set\n");
    return TRUE;
}

/*
 * Main loop
 */

int
main(void)
{
    gboolean pass = TRUE;

#if defined(G_THREADS_ENABLED) && !defined(G_THREADS_IMPL_NONE)
    amanda_thread_init(NULL);

    pass = test_decr_wait() && pass;
    pass = test_wait_empty() && pass;
    pass = test_force_adjust() && pass;
    pass = test_force_set() && pass;

    return pass?0:1;
#else
    printf("No thread support on this platform -- nothing to test\n");
    return 0;
#endif
}
