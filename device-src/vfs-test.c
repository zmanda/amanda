/*
 * Copyright (c) 2008,2009 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *
 * Contact information: Zmanda Inc., 465 N Mathlida Ave, Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#include "glib-util.h"
#include "amanda.h"
#include "device.h"
#include "conffile.h"
#include "testutils.h"

/* Global state set up for the tests */
static char *device_path = NULL;

/*
 * Utilities
 */

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
setup_device(void)
{
    Device *device;
    char *device_name = NULL;

    device_name = vstralloc("file:", device_path, NULL);
    device = device_open(device_name);
    if (device->status != DEVICE_STATUS_SUCCESS) {
	g_critical("Could not open device %s: %s\n", device_name, device_error(device));
    }

    amfree(device_name);
    return device;
}

/*
 * Tests
 */

static int
test_vfs_free_space(void)
{
    Device *device = NULL;
    GValue value;
    QualifiedSize qsize;

    device = setup_device();
    if (!device)
	return FALSE;

    bzero(&value, sizeof(value));
    if (!device_property_get(device, PROPERTY_FREE_SPACE, &value)) {
	g_debug("Could not get property_free_space\n");
	return FALSE;
    }

    qsize = *(QualifiedSize*)g_value_get_boxed(&value);
    g_value_unset(&value);

    if (qsize.accuracy != SIZE_ACCURACY_REAL) {
	g_debug("property_free_space accuracy is not SIZE_ACCURACY_REAL\n");
	return FALSE;
    }

    if (qsize.bytes == 0) {
	g_debug("property_free_space returned bytes=0\n");
	return FALSE;
    }

    g_object_unref(device);

    return TRUE;
}

/*
 * Main driver
 */

int
main(int argc, char **argv)
{
    int result;
    static TestUtilsTest tests[] = {
        TU_TEST(test_vfs_free_space, 90),
	TU_END()
    };

    glib_init();
    config_init(0, NULL);
    device_api_init();

    /* TODO: if more tests are added, we'll need a setup/cleanup hook
     * for testutils */
    device_path = setup_vtape_dir();

    result = testutils_run_tests(argc, argv, tests);

    cleanup_vtape_dir(device_path);
    amfree(device_path);

    return result;
}
