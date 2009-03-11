/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
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
 * U.M. DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING ALL
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT SHALL U.M.
 * BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Authors: the Amanda Development Team.  Its members are listed in a
 * file named AUTHORS, in the root directory of this distribution.
 */
/* 
 * $Id: amandad_util.c,v 1.5 2006/07/19 17:46:07 martinea Exp $
 *
 */

#include "amandad.h"
#include "util.h"

#define MAXMAXDUMPS 16

void
init_g_options(
    g_option_t *	g_options)
{
    g_options->str      = NULL;
    g_options->features = NULL;
    g_options->hostname = NULL;
    g_options->auth     = NULL;
    g_options->maxdumps = 0;
    g_options->config   = NULL;
}


g_option_t *
parse_g_options(
    char *	str,
    int		verbose)
{
    g_option_t *g_options;
    char *p, *tok;
    int new_maxdumps;

    g_options = alloc(sizeof(g_option_t));
    init_g_options(g_options);
    g_options->str = stralloc(str);

    p = stralloc(str);
    tok = strtok(p,";");

    while (tok != NULL) {
	if(strncmp(tok,"features=", 9) == 0) {
	    char *t = tok+9;
	    char *u = strchr(t, ';');
	    if (u)
	       *u = '\0';
	    if(g_options->features != NULL) {
		dbprintf(_("multiple features option\n"));
		if(verbose) {
		    g_printf(_("ERROR [multiple features option]\n"));
		}
	    }
	    if((g_options->features = am_string_to_feature(t)) == NULL) {
		dbprintf(_("bad features value \"%s\"\n"), t);
		if(verbose) {
		    g_printf(_("ERROR [bad features value \"%s\"]\n"), t);
		}
	    }
	    if (u)
	       *u = ';';
	}
	else if(strncmp(tok,"hostname=", 9) == 0) {
	    if(g_options->hostname != NULL) {
		dbprintf(_("multiple hostname option\n"));
		if(verbose) {
		    g_printf(_("ERROR [multiple hostname option]\n"));
		}
	    }
	    g_options->hostname = stralloc(tok+9);
	}
	else if(strncmp(tok,"auth=", 5) == 0) {
	    if(g_options->auth != NULL) {
		dbprintf(_("multiple auth option\n"));
		if(verbose) {
		    g_printf(_("ERROR [multiple auth option]\n"));
		}
	    }
	    g_options->auth = stralloc(tok+5);
	}
	else if(strncmp(tok,"maxdumps=", 9) == 0) {
	    if(g_options->maxdumps != 0) {
		dbprintf(_("multiple maxdumps option\n"));
		if(verbose) {
		    g_printf(_("ERROR [multiple maxdumps option]\n"));
		}
	    }
	    if(sscanf(tok+9, "%d;", &new_maxdumps) == 1) {
		if (new_maxdumps > MAXMAXDUMPS) {
		    g_options->maxdumps = MAXMAXDUMPS;
		}
		else if (new_maxdumps > 0) {
		    g_options->maxdumps = new_maxdumps;
		}
		else {
		    dbprintf(_("bad maxdumps value \"%s\"\n"), tok+9);
		    if(verbose) {
			g_printf(_("ERROR [bad maxdumps value \"%s\"]\n"),
			       tok+9);
		    }
		}
	    }
	    else {
		dbprintf(_("bad maxdumps value \"%s\"\n"), tok+9);
		if(verbose) {
		    g_printf(_("ERROR [bad maxdumps value \"%s\"]\n"),
			   tok+9);
		}
	    }
	}
	else if(strncmp(tok,"config=", 7) == 0) {
	    if(g_options->config != NULL) {
		dbprintf(_("multiple config option\n"));
		if(verbose) {
		    g_printf(_("ERROR [multiple config option]\n"));
		}
	    }
	    g_options->config = stralloc(tok+7);
	    if (strchr(g_options->config, '/')) {
		amfree(g_options->config);
		dbprintf(_("invalid character in config option\n"));
		if(verbose) {
		    g_printf(_("ERROR [invalid character in config option]\n"));
		}
	    }
	}
	else {
	    dbprintf(_("unknown option \"%s\"\n"), tok);
	    if(verbose) {
		g_printf(_("ERROR [unknown option \"%s\"]\n"), tok);
	    }
	}
	tok = strtok(NULL, ";");
    }
    if(g_options->features == NULL) {
	g_options->features = am_set_default_feature_set();
    }
    if(g_options->maxdumps == 0) /* default */
	g_options->maxdumps = 1;
    amfree(p);
    return g_options;
}

void
free_g_options(
    g_option_t *	g_options)
{
    if (g_options != NULL) {
	amfree(g_options->str);
	am_release_feature_set(g_options->features);
	amfree(g_options->hostname);
	amfree(g_options->auth);
	amfree(g_options->config);
	amfree(g_options);
    }
}
