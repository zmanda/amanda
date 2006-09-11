/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-2000 University of Maryland at College Park
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
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/*
 * $Id: clientconf.c,v 1.15 2006/07/25 18:43:36 martinea Exp $
 *
 * read configuration file
 */
/*
 *
 * XXX - I'm not happy *at all* with this implementation, but I don't
 * think YACC would be any easier.  A more table based implementation
 * would be better.  Also clean up memory leaks.
 */
#include "amanda.h"
#include "arglist.h"
#include "util.h"
#include "clientconf.h"
#include "clock.h"

/* configuration parameters */
static char *cln_config_dir = NULL;

val_t client_conf[CLN_CLN];

command_option_t *client_options      = NULL;
int               client_options_size = 0;

/* predeclare local functions */

static void init_defaults(void);
static void read_conffile_recursively(char *filename);

static int read_confline(void);

keytab_t client_keytab[] = {
    { "CONF", CONF_CONF },
    { "INDEX_SERVER", CONF_INDEX_SERVER },
    { "TAPE_SERVER", CONF_TAPE_SERVER },
    { "TAPEDEV", CONF_TAPEDEV },
    { "AUTH", CONF_AUTH },
    { "SSH_KEYS", CONF_SSH_KEYS },
    { "AMANDAD_PATH", CONF_AMANDAD_PATH },
    { "CLIENT_USERNAME", CONF_CLIENT_USERNAME },
    { "GNUTAR_LIST_DIR", CONF_GNUTAR_LIST_DIR },
    { "AMANDATES", CONF_AMANDATES },
    { "INCLUDEFILE", CONF_INCLUDEFILE },
    { NULL, CONF_UNKNOWN },
};

t_conf_var client_var [] = {
   { CONF_CONF           , CONFTYPE_STRING, read_string, CLN_CONF           , NULL },
   { CONF_INDEX_SERVER   , CONFTYPE_STRING, read_string, CLN_INDEX_SERVER   , NULL },
   { CONF_TAPE_SERVER    , CONFTYPE_STRING, read_string, CLN_TAPE_SERVER    , NULL },
   { CONF_TAPEDEV        , CONFTYPE_STRING, read_string, CLN_TAPEDEV        , NULL },
   { CONF_AUTH           , CONFTYPE_STRING, read_string, CLN_AUTH           , NULL },
   { CONF_SSH_KEYS       , CONFTYPE_STRING, read_string, CLN_SSH_KEYS       , NULL },
   { CONF_AMANDAD_PATH   , CONFTYPE_STRING, read_string, CLN_AMANDAD_PATH   , NULL },
   { CONF_CLIENT_USERNAME, CONFTYPE_STRING, read_string, CLN_CLIENT_USERNAME, NULL },
   { CONF_GNUTAR_LIST_DIR, CONFTYPE_STRING, read_string, CLN_GNUTAR_LIST_DIR, NULL },
   { CONF_AMANDATES      , CONFTYPE_STRING, read_string, CLN_AMANDATES      , NULL },
   { CONF_UNKNOWN        , CONFTYPE_INT   , NULL       , CLN_CLN            , NULL }
};

static int first_file = 1;

/*
** ------------------------
**  External entry points
** ------------------------
*/

/* return  0 on success        */
/* return  1 on error          */
/* return -1 if file not found */

int read_clientconf(
    char *filename)
{
    if(first_file == 1) {
	init_defaults();
	first_file = 0;
    } else {
	allow_overwrites = 1;
    }

    /* We assume that conf_confname & conf are initialized to NULL above */
    read_conffile_recursively(filename);

    command_overwrite(client_options, client_var, client_keytab, client_conf,
		      "");

    return got_parserror;
}


char *
client_getconf_byname(
    char *	str)
{
    static char *tmpstr;
    char number[NUM_STR_SIZE];
    t_conf_var *np;
    keytab_t *kt;
    char *s;
    char ch;

    tmpstr = stralloc(str);
    s = tmpstr;
    while((ch = *s++) != '\0') {
	if(islower((int)ch))
	    s[-1] = (char)toupper(ch);
    }

    for(kt = client_keytab; kt->token != CONF_UNKNOWN; kt++)
	if(kt->keyword && strcmp(kt->keyword, tmpstr) == 0) break;

    if(kt->token == CONF_UNKNOWN) return NULL;

    for(np = client_var; np->token != CONF_UNKNOWN; np++)
	if(np->token == kt->token) break;

    if(np->type == CONFTYPE_INT) {
	snprintf(number, SIZEOF(number), "%d", client_getconf_int(np->parm));
	tmpstr = newstralloc(tmpstr, number);
    } else if(np->type == CONFTYPE_BOOL) {
	if(client_getconf_int(np->parm) == 0) {
	    tmpstr = newstralloc(tmpstr, "off");
	}
	else {
	    tmpstr = newstralloc(tmpstr, "on");
	}
    } else if(np->type == CONFTYPE_REAL) {
	snprintf(number, SIZEOF(number), "%lf", client_getconf_real(np->parm));
	tmpstr = newstralloc(tmpstr, number);
    } else {
	tmpstr = newstralloc(tmpstr, client_getconf_str(np->parm));
    }

    return tmpstr;
}

int
client_getconf_seen(
    cconfparm_t parm)
{
    t_conf_var *np;
    np = get_np(client_var, parm);
    return(client_conf[np->parm].seen);
}

int
client_getconf_int(
    cconfparm_t	parm)
{
    t_conf_var *np;
    np = get_np(client_var, parm);
    if (np->type != CONFTYPE_INT) {
	error("client_getconf_int: np is not a CONFTYPE_INT");
	/*NOTREACHED*/
    }

    return(client_conf[np->parm].v.i);
}

off_t
client_getconf_am64(
    cconfparm_t	parm)
{
    t_conf_var *np;
    np = get_np(client_var, parm);
    if (np->type != CONFTYPE_AM64) {
	error("client_getconf_am64: np is not a CONFTYPE_AM64");
	/*NOTREACHED*/
    }
    return(client_conf[np->parm].v.am64);
}

double
client_getconf_real(
    cconfparm_t	parm)
{
    t_conf_var *np;
    np = get_np(client_var, parm);
    if (np->type != CONFTYPE_REAL) {
	error("client_getconf_real: np is not a CONFTYPE_REAL");
	/*NOTREACHED*/
    }
    return(client_conf[np->parm].v.r);
}

char *
client_getconf_str(
    cconfparm_t parm)
{
    t_conf_var *np;
    np = get_np(client_var, parm);
    if (np->type != CONFTYPE_STRING) {
	error("client_getconf_string: np is not a CONFTYPE_STRING");
	/*NOTREACHED*/
    }
    return(client_conf[np->parm].v.s);
}

/*
** ------------------------
**  Internal routines
** ------------------------
*/


static void
init_defaults(void)
{
    char *s;

    /* defaults for exported variables */

#ifdef DEFAULT_CONFIG
    s = DEFAULT_CONFIG;
#else
    s = "";
#endif
    conf_init_string(&client_conf[CLN_CONF], s);

#ifdef DEFAULT_SERVER
    s = DEFAULT_SERVER;
#else
    s = "";
#endif
    conf_init_string(&client_conf[CLN_INDEX_SERVER], s);


#ifdef DEFAULT_TAPE_SERVER
    s = DEFAULT_TAPE_SERVER;
#else
#ifdef DEFAULT_SERVER
    s = DEFAULT_SERVER;
#else
    s = "";
#endif
#endif
    conf_init_string(&client_conf[CLN_TAPE_SERVER], s);

#ifdef DEFAULT_TAPE_DEVICE
    s = DEFAULT_TAPE_DEVICE;
#else
    s = NULL;
#endif
    conf_init_string(&client_conf[CLN_TAPEDEV], s);

    conf_init_string(&client_conf[CLN_AUTH], "bsd");
    conf_init_string(&client_conf[CLN_SSH_KEYS], "");
    conf_init_string(&client_conf[CLN_AMANDAD_PATH], "");
    conf_init_string(&client_conf[CLN_CLIENT_USERNAME], "");
#ifdef GNUTAR_LISTED_INCREMENTAL_DIR
    conf_init_string(&client_conf[CLN_GNUTAR_LIST_DIR],
		     GNUTAR_LISTED_INCREMENTAL_DIR);
#else
    conf_init_string(&client_conf[CLN_GNUTAR_LIST_DIR], NULL);
#endif
#ifdef AMANDATES_FILE
    conf_init_string(&client_conf[CLN_AMANDATES], AMANDATES_FILE);
#else
    conf_init_string(&client_conf[CLN_AMANDATES], NULL);
#endif

    /* defaults for internal variables */

    conf_line_num = got_parserror = 0;
    allow_overwrites = 0;
    token_pushed = 0;

}

static void
read_conffile_recursively(
    char *	filename)
{
    /* Save globals used in read_confline(), elsewhere. */
    int  save_line_num  = conf_line_num;
    FILE *save_conf     = conf_conf;
    char *save_confname = conf_confname;
    int	rc;

    if (*filename == '/' || cln_config_dir == NULL) {
	conf_confname = stralloc(filename);
    } else {
	conf_confname = stralloc2(cln_config_dir, filename);
    }

    if((conf_conf = fopen(conf_confname, "r")) == NULL) {
	dbprintf(("Could not open conf file \"%s\": %s\n", conf_confname,
		  strerror(errno)));
	amfree(conf_confname);
	got_parserror = -1;
	return;
    }
    dbprintf(("Reading conf file \"%s\".\n", conf_confname));

    conf_line_num = 0;

    /* read_confline() can invoke us recursively via "includefile" */
    do {
	rc = read_confline();
    } while (rc != 0);
    afclose(conf_conf);

    amfree(conf_confname);

    /* Restore globals */
    conf_line_num = save_line_num;
    conf_conf     = save_conf;
    conf_confname = save_confname;
}


/* ------------------------ */


static int
read_confline(void)
{
    t_conf_var *np;

    keytable = client_keytab;

    conf_line_num += 1;
    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_INCLUDEFILE:
	{
	    char *fn;

	    get_conftoken(CONF_STRING);
	    fn = tokenval.v.s;
	    read_conffile_recursively(fn);
	}
	break;

    case CONF_NL:	/* empty line */
	break;

    case CONF_END:	/* end of file */
	return 0;

    default:
	{
	    for(np = client_var; np->token != CONF_UNKNOWN; np++)
		if(np->token == tok) break;

	    if(np->token == CONF_UNKNOWN) {
		conf_parserror("configuration keyword expected");
	    } else {
		np->read_function(np, &client_conf[np->parm]);
		if(np->validate)
		    np->validate(np, &client_conf[np->parm]);
	    }
	}
    }
    if(tok != CONF_NL)
	get_conftoken(CONF_NL);
    return 1;
}



/* ------------------------ */

#ifdef TEST

static char *cln_config_name = NULL;
static char *cln_config_dir = NULL;

void
dump_client_configuration(
    char *filename)
{
    printf("AMANDA CLIENT CONFIGURATION FROM FILE \"%s\":\n\n", filename);

    printf("cln_conf = \"%s\"\n", client_getconf_str(CLN_CONF));
    printf("cln_index_server = \"%s\"\n", client_getconf_str(CLN_INDEX_SERVER));
    printf("cln_tape_server = \"%s\"\n", client_getconf_str(CLN_TAPE_SERVER));
    printf("cln_tapedev = \"%s\"\n", client_getconf_str(CLN_TAPEDEV));
    printf("cln_auth = \"%s\"\n", client_getconf_str(CLN_AUTH));
    printf("cln_ssh_keys = \"%s\"\n", client_getconf_str(CLN_SSH_KEYS));
}

int
main(
    int		argc,
    char **	argv)
{
  char *conffile;
  char *diskfile;
  disklist_t lst;
  int result;
  unsigned long malloc_hist_1, malloc_size_1;
  unsigned long malloc_hist_2, malloc_size_2;

  safe_fd(-1, 0);

  set_pname("conffile");

  /* Don't die when child closes pipe */
  signal(SIGPIPE, SIG_IGN);

  malloc_size_1 = malloc_inuse(&malloc_hist_1);

  startclock();

  if (argc > 1) {
    if (argv[1][0] == '/') {
      cln_config_dir = stralloc(argv[1]);
      cln_config_name = strrchr(cln_config_dir, '/') + 1;
      cln_config_name[-1] = '\0';
      cln_config_dir = newstralloc2(cln_config_dir, cln_config_dir, "/");
    } else {
      cln_config_name = stralloc(argv[1]);
      cln_config_dir = vstralloc(CONFIG_DIR, "/", cln_config_name, "/", NULL);
    }
  } else {
    char my_cwd[STR_SIZE];

    if (getcwd(my_cwd, SIZEOF(my_cwd)) == NULL) {
      error("cannot determine current working directory");
    }
    cln_config_dir = stralloc2(my_cwd, "/");
    if ((cln_config_name = strrchr(my_cwd, '/')) != NULL) {
      cln_config_name = stralloc(cln_config_name + 1);
    }
  }

  conffile = stralloc2(cln_config_dir, CONFFILE_NAME);
  result = read_conffile(conffile);
  if (result == 0) {
      diskfile = client_getconf_str(CNF_DISKFILE);
      if (diskfile != NULL && access(diskfile, R_OK) == 0) {
	  result = read_diskfile(diskfile, &lst);
      }
  }
  dump_client_configuration(CONFFILE_NAME);
  amfree(conffile);

  malloc_size_2 = malloc_inuse(&malloc_hist_2);

  if(malloc_size_1 != malloc_size_2) {
    malloc_list(fileno(stderr), malloc_hist_1, malloc_hist_2);
  }

  return result;
}

#endif /* TEST */

char *
generic_client_get_security_conf(
    char *	string,
    void *	arg)
{
	(void)arg;	/* Quiet unused parameter warning */

	if(!string || !*string)
		return(NULL);

	if(strcmp(string, "conf")==0) {
		return(client_getconf_str(CLN_CONF));
	} else if(strcmp(string, "index_server")==0) {
		return(client_getconf_str(CLN_INDEX_SERVER));
	} else if(strcmp(string, "tape_server")==0) {
		return(client_getconf_str(CLN_TAPE_SERVER));
	} else if(strcmp(string, "tapedev")==0) {
		return(client_getconf_str(CLN_TAPEDEV));
	} else if(strcmp(string, "auth")==0) {
		return(client_getconf_str(CLN_AUTH));
	} else if(strcmp(string, "ssh_keys")==0) {
		return(client_getconf_str(CLN_SSH_KEYS));
	} else if(strcmp(string, "amandad_path")==0) {
		return(client_getconf_str(CLN_AMANDAD_PATH));
	} else if(strcmp(string, "client_username")==0) {
		return(client_getconf_str(CLN_CLIENT_USERNAME));
	} else if(strcmp(string, "gnutar_list_dir")==0) {
		return(client_getconf_str(CLN_GNUTAR_LIST_DIR));
	} else if(strcmp(string, "amandates")==0) {
		return(client_getconf_str(CLN_AMANDATES));
/*
	} else if(strcmp(string, "krb5principal")==0) {
		return(client_getconf_str(CNF_KRB5PRINCIPAL));
	} else if(strcmp(string, "krb5keytab")==0) {
		return(client_getconf_str(CNF_KRB5KEYTAB));
*/
	}
	return(NULL);
}


void
parse_client_conf(
    int parse_argc,
    char **parse_argv,
    int *new_argc,
    char ***new_argv)
{
    int i;
    char **my_argv;
    char *myarg, *value;
    command_option_t *client_option;

    client_options = alloc((size_t)(parse_argc+1) * SIZEOF(*client_options));
    client_options_size = parse_argc+1;
    client_option = client_options;
    client_option->name = NULL;

    my_argv = alloc((size_t)parse_argc * SIZEOF(char *));
    *new_argv = my_argv;
    *new_argc = 0;
    i=0;
    while(i<parse_argc) {
	if(strncmp(parse_argv[i],"-o",2) == 0) {
	    if(strlen(parse_argv[i]) > 2)
		myarg = &parse_argv[i][2];
	    else {
		i++;
		if(i >= parse_argc)
		    error("expect something after -o");
		myarg = parse_argv[i];
	    }
	    value = index(myarg,'=');
	    if (value == NULL) {
		conf_parserror("Must specify a value for %s.\n", myarg);
	    } else {
		*value = '\0';
		value++;
		client_option->used = 0;
		client_option->name = stralloc(myarg);
		client_option->value = stralloc(value);
		client_option++;
		client_option->name = NULL;
	    }
	}
	else {
	    my_argv[*new_argc] = stralloc(parse_argv[i]);
	    *new_argc += 1;
	}
	i++;
    }
}

/* return  0 on success             */
/* return -1 if it is already there */
/* return -2 if other failure       */
int
add_client_conf(
    cconfparm_t parm,
    char *value)
{
    t_conf_var *np;
    keytab_t *kt;
    command_option_t *command_option;
    int nb_option;

    for(np = client_var; np->token != CONF_UNKNOWN; np++)
	if(np->parm == (int)parm) break;

    if(np->token == CONF_UNKNOWN) return -2;

    for(kt = client_keytab; kt->token != CONF_UNKNOWN; kt++)
	if(kt->token == np->token) break;

    if(kt->token == CONF_UNKNOWN) return -2;

    /* Try to find it */
    nb_option = 0;
    for(command_option = client_options; command_option->name != NULL;
							command_option++) {
	if(strcasecmp(command_option->name, kt->keyword) == 0) {
	    return -1;
	}
	nb_option++;
    }

    /* Increase size of client_options if needed */
    if(nb_option >= client_options_size-1) {
	client_options_size *= 2;
	client_options = realloc(client_options,
			        client_options_size * SIZEOF(*client_options));
	if (client_options == NULL) {
	    error("Can't realloc client_options: %s\n", strerror(errno));
	    /*NOTREACHED*/
	}
	for(command_option = client_options; command_option->name != NULL;
							command_option++) {
	}
    }

    /* add it */
    command_option->used = 0;
    command_option->name = stralloc(kt->keyword);
    command_option->value = stralloc(value);
    command_option++;
    command_option->name = NULL;
    return 0;
}

void
report_bad_client_arg(void)
{
    command_option_t *command_option;

    for(command_option = client_options; command_option->name != NULL;
							command_option++) {
	if(command_option->used == 0) {
	    fprintf(stderr,"argument -o%s=%s not used\n",
		    command_option->name, command_option->value);
	}
    }
}
