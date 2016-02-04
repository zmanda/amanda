/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2013 Zmanda, Inc.  All Rights Reserved.
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
#include "util.h"
#include "security-file.h"

#define LINE_SIZE 1024

static
FILE *open_security_file(FILE *verbose)
{
    FILE *sec_file;

    if (!check_security_file_permission(verbose)) {
	return NULL;
    }

    sec_file = fopen(DEFAULT_SECURITY_FILE, "r");
    if (!sec_file) {
	if (verbose) {
	    g_fprintf(verbose,"ERROR [Can't open '%s': %s\n", DEFAULT_SECURITY_FILE, strerror(errno));
	}
	g_debug("ERROR [Can't open '%s': %s", DEFAULT_SECURITY_FILE, strerror(errno));
	return NULL;
    }

    return sec_file;
}

static gboolean
security_file_check_path(
    char *prefix,
    char *path,
    FILE *verbose)
{
    FILE *sec_file;
    char *iprefix;
    char *p, *l;
    char line[LINE_SIZE];
    gboolean found = FALSE;

    if (!prefix) {
	return FALSE;
    }
    if (!path) {
	return FALSE;
    }

    sec_file = open_security_file(verbose);
    if (!sec_file) {
	return FALSE;
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
		    return TRUE;
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
	    return TRUE;
	}
    }

    if (verbose) {
	g_fprintf(verbose, "[ERROR: security file do not allow to run '%s' as root for '%s']\n", path, iprefix);
    }
    g_debug("ERROR: security file do not allow to run '%s' as root for '%s'", path, iprefix);
    g_free(iprefix);
    fclose(sec_file);
    return FALSE;
}

static gboolean
security_file_get_boolean(
    char *name,
    FILE *verbose)
{
    FILE *sec_file;
    char *iname;
    char *n, *l;
    char line[LINE_SIZE];
    char oline[LINE_SIZE];

    sec_file = open_security_file(verbose);
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
		error("BOGUS line '%s' in /etc/amanda-security.conf file", oline);
	    }
	}
    }

    g_free(iname);
    fclose(sec_file);
    return FALSE;
}

static gboolean check_security_file_permission_recursive(
		FILE *verbose, char *security_real_path, char *quote_orig);

gboolean
check_security_file_permission(
    FILE *verbose)
{
    char *quoted = quote_string(DEFAULT_SECURITY_FILE);
    char  security_real_path[PATH_MAX];
    char *sec_real_path;

#ifdef SINGLE_USERID
    uid_t ruid = getuid();
    uid_t euid = geteuid();

    if (ruid != 0 && euid != 0 && ruid == euid) {
	amfree(quoted);
	return TRUE;
    }
#endif

    sec_real_path = realpath(DEFAULT_SECURITY_FILE, security_real_path);
    if (!sec_real_path) {
	if (verbose)
	    g_fprintf(verbose, "ERROR [Can't get realpath of the security file '%s': %s]\n", quoted, strerror(errno));
	g_debug("ERROR [Can't get realpath of the security file '%s': %s]", quoted, strerror(errno));
	amfree(quoted);
	return FALSE;
    }

    if (EUIDACCESS(security_real_path, R_OK) == -1) {
	char  ruid_str[NUM_STR_SIZE];
	char  euid_str[NUM_STR_SIZE];

	g_snprintf(ruid_str, sizeof(ruid_str), "%d", (int)getuid());
	g_snprintf(euid_str, sizeof(euid_str), "%d", (int)geteuid());

	if (verbose)
	    g_fprintf(verbose, "ERROR [can not access '%s': %s (ruid:%s euid:%s)]\n", quoted, strerror(errno), ruid_str, euid_str);
	g_debug("ERROR [can not access '%s': %s (ruid:%s euid:%s)]", quoted, strerror(errno), ruid_str, euid_str);
	amfree(quoted);
	return FALSE;
    }
    return check_security_file_permission_recursive(verbose, security_real_path, quoted);
    amfree(quoted);
}

static
gboolean
check_security_file_permission_recursive(
    FILE *verbose,
    char *security_real_path,
    char *quoted_orig)
{
    struct stat stat_buf;
    char *s;
    char *quoted = quote_string(security_real_path);


    if (!stat(security_real_path, &stat_buf)) {
        if (stat_buf.st_uid != 0 ) {
            if (verbose)
		g_fprintf(verbose, "ERROR [%s (%s) is not owned by root]\n", quoted, quoted_orig);
	    g_debug("ERROR [%s (%s) is not owned by root]", quoted, quoted_orig);
            amfree(quoted);
            return FALSE;
        }
        if (stat_buf.st_mode & S_IWOTH) {
            if (verbose)
		g_fprintf(verbose, "ERROR [%s (%s) is writable by everyone]\n", quoted, quoted_orig);
	    g_debug("ERROR [%s (%s) is writable by everyone]", quoted, quoted_orig);
            amfree(quoted);
            return FALSE;
        }
        if (stat_buf.st_mode & S_IWGRP) {
            if (verbose)
		g_fprintf(verbose, "ERROR [%s (%s) is writable by the group]\n", quoted, quoted_orig);
	    g_debug("ERROR [%s (%s) is writable by the group]", quoted, quoted_orig);
            amfree(quoted);
            return FALSE;
        }
    }
    else {
        if (verbose)
	    g_fprintf(verbose, "ERROR [can not stat %s (%s): %s]\n", quoted, quoted_orig, strerror(errno));
	g_debug("ERROR [can not stat %s (%s): %s]", quoted, quoted_orig, strerror(errno));
        amfree(quoted);
        return FALSE;
    }

    amfree(quoted);
    if ((s = strrchr(security_real_path, '/'))) {
	*s = '\0';
	if (*security_real_path) {
	    return check_security_file_permission_recursive(
					verbose, security_real_path, quoted_orig);
	}
    }
    return TRUE;
}

gboolean
security_allow_program_as_root(
    char *name,
    char *path,
    FILE *verbose)
{
    gboolean r;
    char *prefix;

    prefix = g_strdup_printf("%s:%s", get_pname(), name);

    r = security_file_check_path(prefix, path, verbose);
    g_free(prefix);
    return r;
}

gboolean
security_allow_to_restore(
    FILE *verbose)
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
	return security_file_get_boolean("restore_by_amanda_user", verbose);
    } else {
	return FALSE;
    }
}

