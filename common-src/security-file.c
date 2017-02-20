/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
 * All Rights Reserved.
 *
 * Permission to use, copy, modify, distribute, and sell this software and its
 * documentation for any purpose is hereby granted without fee, provided that
 * the above copyright notice appear in all copies and that both that
 * copyright notice and this permission notice appear in supporting
 * documentation, and that the name of U.M. not be used in advertising or
 * publicity pertaining to distribution of the software without specific,
 * written prior permission.  U.M. makes no representations about the
 * suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 *
 */
/*
 */
#include "amanda.h"
#include "amutil.h"
#include "ammessage.h"
#include "security-file.h"

#define LINE_SIZE 1024

static
message_t *
open_security_file(FILE **file)
{
    message_t *message;
    if ((message = check_security_file_permission_message())) {
	return message;
    }

    *file = fopen(DEFAULT_SECURITY_FILE, "r");
    if (!*file) {
	return build_message(
		AMANDA_FILE, __LINE__, 3600095, MSG_ERROR, 2,
		"security_file", DEFAULT_SECURITY_FILE,
		"errno"        , errno);
    }

    return NULL;
}

static
message_t *
security_file_check_path(
    char *prefix,
    char *path)
{
    FILE *sec_file;
    char *iprefix;
    char *p, *l;
    char line[LINE_SIZE];
    gboolean found = FALSE;
    message_t *message;

    if (!prefix) {
	return build_message(
		AMANDA_FILE, __LINE__, 3600093, MSG_ERROR, 0);
    }
    if (!path) {
	return build_message(
		AMANDA_FILE, __LINE__, 3600094, MSG_ERROR, 0);
    }

    message = open_security_file(&sec_file);
    if (message) {
	return message;
    }


    iprefix = g_strdup(prefix);
    for (p = iprefix; *p; ++p) *p = tolower(*p);

    while (fgets(line, LINE_SIZE, sec_file)) {
	char *p = strchr(line, '=');
	int len = strlen(line);
	if (len == 0) continue;
	if (*line == '#') continue;
	if (line[len-1] == '\n')
	    line[len-1] = '\0';
	if (p) {
	    *p = '\0';
	    p++;
	    for (l = line; *l; ++l) *l = tolower(*l);
	    if (g_str_equal(iprefix, line)) {
		found = TRUE;
		if (g_str_equal(path, p)) {
		    g_free(iprefix);
		    fclose(sec_file);
		    return NULL;
		}
	    }
	}
    }

    if (!found) { /* accept the configured path */
	if ((strcmp(iprefix,"amgtar:gnutar_path") == 0 &&
	     strcmp(path,GNUTAR) == 0) ||
	    (strcmp(iprefix,"ambsdtar:bsdtar_path") == 0 &&
	     strcmp(path,BSDTAR) == 0) ||
	    (strcmp(iprefix,"amstar:star_path") == 0 &&
	     strcmp(path,STAR) == 0) ||
	    (strcmp(iprefix,"runtar:gnutar_path") == 0 &&
	     strcmp(path,GNUTAR) == 0)) {
	    g_free(iprefix);
	    fclose(sec_file);
	    return NULL;
	}
    }

    message = build_message(
		AMANDA_FILE, __LINE__, 3600096, MSG_ERROR, 2,
		"prefix", iprefix,
		"path"  , path);
    g_free(iprefix);
    fclose(sec_file);
    return message;
}

static gboolean
security_file_get_boolean(
    char *name)
{
    FILE *sec_file;
    char *iname;
    char *n, *l;
    char line[LINE_SIZE];
    char oline[LINE_SIZE];
    message_t *message;

    message = open_security_file(&sec_file);
    if (message) {
	return FALSE;
    }

    if (!sec_file) {
	return FALSE;
    }

    iname = g_strdup(name);
    for (n = iname; *n; ++n) *n = tolower(*n);

    while (fgets(line, LINE_SIZE, sec_file)) {
	char *p;
	int len = strlen(line);
	if (len == 0) continue;
	if (*line == '#') continue;
	if (line[len-1] == '\n')
	    line[len-1] = '\0';
	strcpy(oline, line);
	p = strchr(line, '=');
	if (p) {
	    *p = '\0';
	    p++;
	    for (l = line; *l; ++l) *l = tolower(*l);
	    if (g_str_equal(iname, line)) {
		if (g_str_equal(p, "YES") ||
		    g_str_equal(p, "yes")) {
		    g_free(iname);
		    fclose(sec_file);
		    return TRUE;
		}
		if (g_str_equal(p, "NO") ||
		    g_str_equal(p, "no")) {
		    g_free(iname);
		    fclose(sec_file);
		    return FALSE;
		}
		error("BOGUS line '%s' in " DEFAULT_SECURITY_FILE " file", oline);
	    }
	}
    }

    g_free(iname);
    fclose(sec_file);
    return FALSE;
}

static gboolean
security_file_get_portrange(
    char *name, int *plow, int *phigh)
{
    FILE *sec_file;
    char *iname;
    char *n, *l;
    char line[LINE_SIZE];
    char oline[LINE_SIZE];
    message_t *message;

    *plow = -1;
    *phigh = -1;
    message = open_security_file(&sec_file);
    if (message) {
	return FALSE;
    }

    if (!sec_file) {
	return FALSE;
    }

    iname = g_strdup(name);
    for (n = iname; *n; ++n) *n = tolower(*n);

    while (fgets(line, LINE_SIZE, sec_file)) {
	char *p;
	int len = strlen(line);
	if (len == 0) continue;
	if (*line == '#') continue;
	if (line[len-1] == '\n')
	    line[len-1] = '\0';
	strcpy(oline, line);
	p = strchr(line, '=');
	if (p) {
	    *p = '\0';
	    p++;
	    for (l = line; *l; ++l) *l = tolower(*l);
	    if (g_str_equal(iname, line)) {
		char *shigh = strchr(p, ',');
		if (shigh) {
		    shigh++;
		    *plow = atoi(p);
		    *phigh = atoi(shigh);
		    return TRUE;
		}
		error("BOGUS line '%s' in " DEFAULT_SECURITY_FILE " file", oline);
	    }
	}
    }

    g_free(iname);
    fclose(sec_file);
    return FALSE;
}

static message_t * check_security_file_permission_message_recursive(
     char *security_real_path, char *security_orig);

message_t *
check_security_file_permission_message(void)
{
    char  security_real_path[PATH_MAX];
    char *sec_real_path;

#ifdef SINGLE_USERID
    uid_t ruid = getuid();
    uid_t euid = geteuid();

    if (ruid != 0 && euid != 0 && ruid == euid) {
	return NULL;
    }
#endif

    sec_real_path = realpath(DEFAULT_SECURITY_FILE, security_real_path);
    if (!sec_real_path) {
	return build_message(
		AMANDA_FILE, __LINE__, 3600097, MSG_ERROR, 2,
		"errno", errno,
		"security_file", DEFAULT_SECURITY_FILE);
    }

    if (EUIDACCESS(security_real_path, R_OK) == -1) {
	char  ruid_str[NUM_STR_SIZE];
	char  euid_str[NUM_STR_SIZE];

	g_snprintf(ruid_str, sizeof(ruid_str), "%d", (int)getuid());
	g_snprintf(euid_str, sizeof(euid_str), "%d", (int)geteuid());

	return build_message(
		AMANDA_FILE, __LINE__, 3600063, MSG_ERROR, 5,
		"errno", errno,
		"noun", "access",
		"filename", security_real_path,
		"ruid", ruid_str,
		"euid", euid_str);
    }
    return check_security_file_permission_message_recursive(security_real_path, DEFAULT_SECURITY_FILE);
}

static
message_t *
check_security_file_permission_message_recursive(
    char *security_real_path,
    char *security_orig)
{
    struct stat stat_buf;
    char *s;

    if (!stat(security_real_path, &stat_buf)) {
        if (stat_buf.st_uid != 0 ) {
	    return build_message(
		AMANDA_FILE, __LINE__, 3600088, MSG_ERROR, 2,
		"filename", security_real_path,
		"security_orig", security_orig);
        }
        if (stat_buf.st_mode & S_IWOTH) {
	    return build_message(
		AMANDA_FILE, __LINE__, 3600089, MSG_ERROR, 2,
		"filename", security_real_path,
		"security_orig", security_orig);
        }
        if (stat_buf.st_mode & S_IWGRP) {
	    return build_message(
		AMANDA_FILE, __LINE__, 3600090, MSG_ERROR, 2,
		"filename", security_real_path,
		"security_orig", security_orig);
        }
    }
    else {
	return build_message(
		AMANDA_FILE, __LINE__, 3600098, MSG_ERROR, 3,
		"errno", errno,
		"filename", security_real_path,
		"security_orig", security_orig);
    }

    if ((s = strrchr(security_real_path, '/'))) {
	*s = '\0';
	if (*security_real_path) {
	    return check_security_file_permission_message_recursive(security_real_path, security_orig);
	}
    }
    return NULL;
}

message_t *
security_allow_program_as_root(
    char *name,
    char *path)
{
    message_t *message;
    char *prefix;

    prefix = g_strdup_printf("%s:%s", get_pname(), name);

    message = security_file_check_path(prefix, path);
    g_free(prefix);
    return message;
}

gboolean
security_allow_to_restore(void)
{
    uid_t ruid = getuid();
    uid_t euid = geteuid();
    struct passwd *pw;

    /* non-root can do restore as non-root */
    if (ruid != 0 && euid != 0 && ruid == euid) {
	return TRUE;
    }

    /* root can do a restore */
    if (ruid == 0 && euid == 0)
	return TRUE;

    if ((pw = getpwnam(CLIENT_LOGIN)) == NULL) {
	return FALSE;
    }
    if (euid == pw->pw_uid) {
	return security_file_get_boolean("restore_by_amanda_user");
    } else {
	return FALSE;
    }
}

gboolean
security_allow_bind(
    int s,
    sockaddr_union *addr)
{

    int port;
    int type;
    socklen_t_equiv length = sizeof(type);
    port = SU_GET_PORT(addr);
    if (getsockopt(s, SOL_SOCKET, SO_TYPE, &type, &length) == -1) {
	fprintf(stderr, "getsockopt failed: %s", strerror(errno));
	return FALSE;
    }

    if (type == SOCK_STREAM) {
	int low, high;
	if (security_file_get_portrange("tcp_port_range", &low, &high)) {
	    if (low <= port && port <= high) {
		return TRUE;
	    } else {
		fprintf(stderr, "tcp port out of range (%d <= %d <= %d)\n", low, port, high);
		return FALSE;
	    }
	} else {
	    fprintf(stderr, "No defined tcp_port_range in '%s'\n", DEFAULT_SECURITY_FILE);
	    return FALSE;
	}
    } else if (type == SOCK_DGRAM) {
	int low, high;
	if (security_file_get_portrange("udp_port_range", &low, &high)) {
	    if (low <= port && port <= high) {
		return TRUE;
	    } else {
		fprintf(stderr, "udp port out of range (%d <= %d <= %d)\n", low, port, high);
		return FALSE;
	    }
	} else {
	    fprintf(stderr, "No defined udp_port_range in '%s'\n", DEFAULT_SECURITY_FILE);
	    return FALSE;
	}
    } else {
	fprintf(stderr, "Wrong socket type: %d\n", type);
	return FALSE;
    }
}

