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

#include <device.h>
#include <amanda.h>
#include "util.h"

/* global so the 'atexit' handler can access it */

static void
cleanup_vtape_dir(char *device_path)
{
    char *quoted = g_shell_quote(device_path);
    char *cmd = vstralloc("rm -rf ", quoted, NULL);

    /* would you rather write 'rm -rf' here? */
    if (system(cmd) == -1) {
	exit(1);
    }

    amfree(cmd);
    amfree(quoted);
}

static char *
setup_vtape_dir(void)
{
    char *cwd = g_get_current_dir();
    char *device_path = NULL;
    char *data_dir = NULL;

    device_path = vstralloc(cwd, "/vfs-test-XXXXXX", NULL);
    amfree(cwd);

    if (mkdtemp(device_path) == NULL) {
	fprintf(stderr, "Could not create temporary directory in %s\n", cwd);
	return NULL;
    }

    /* append "/data/" to that for the VFS device*/
    data_dir = vstralloc(device_path, "/data/", NULL);
    if (mkdir(data_dir, 0777) == -1) {
	fprintf(stderr, "Could not create %s: %s\n", cwd, strerror(errno));
	amfree(data_dir);
	return NULL;
    }

    amfree(data_dir);
    return device_path;
}

static Device *
setup_device(char *device_path)
{
    Device *device;
    char *device_name = NULL;

    device_name = vstralloc("file:", device_path, NULL);
    device = device_open(device_name);
    if (!device) {
	fprintf(stderr, "Could not open device %s\n", device_name);
    }

    amfree(device_name);
    return device;
}

static gboolean
check_free_space(Device *device)
{
    GValue value;
    QualifiedSize qsize;

    bzero(&value, sizeof(value));
    if (!device_property_get(device, PROPERTY_FREE_SPACE, &value)) {
	fprintf(stderr, "Could not get property_free_space\n");
	return FALSE;
    }

    qsize = *(QualifiedSize*)g_value_get_boxed(&value);
    g_value_unset(&value);

    if (qsize.accuracy != SIZE_ACCURACY_REAL) {
	fprintf(stderr, "property_free_space accuracy is not SIZE_ACCURACY_REAL\n");
	return FALSE;
    }

    if (qsize.bytes == 0) {
	fprintf(stderr, "property_free_space returned bytes=0\n");
	return FALSE;
    }

    return TRUE;
}

int
main(int argc G_GNUC_UNUSED, char **argv G_GNUC_UNUSED)
{
    Device *device = NULL;
    gboolean ok = TRUE;
    char *device_path = NULL;
    pid_t pid;
    amwait_t status;

    amanda_thread_init();

    device_path = setup_vtape_dir();

    /* run the tests in a subprocess so we can clean up even if they fail */
    switch (pid = fork()) {
	case -1: /* error */
	    perror("fork");
	    g_assert_not_reached();

	case 0: /* child */
	    device_api_init();

	    device = setup_device(device_path);
	    if (!device)
		return 1;

	    ok = ok && check_free_space(device);

	    g_object_unref(device);

	    if (!ok) exit(1);
	    exit(0);
	    g_assert_not_reached();

	default: /* parent */
	    if (waitpid(pid, &status, 0) == -1)
		perror("waitpid");

	    /* cleanup */
	    cleanup_vtape_dir(device_path);
	    amfree(device_path);

	    /* figure our own return status */
	    if (WIFEXITED(status))
		return WEXITSTATUS(status);
	    else if (WIFSIGNALED(status)) {
		fprintf(stderr, "Test failed with signal %d\n", (int)WTERMSIG(status));
		return 1;
	    } else {
		/* weird.. */
		return 1;
	    }
	    g_assert_not_reached();
    }
}
