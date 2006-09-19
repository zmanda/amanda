#include "amanda.h"
#include "pipespawn.h"
#include "arglist.h"
#include "clock.h"
#include "util.h"

char skip_argument[1];

pid_t pipespawnv_passwd(char *prog, int pipedef,
                  int *stdinfd, int *stdoutfd, int *stderrfd,
                  char **my_argv);


/*
 * this used to be a function in it's own write but became a wrapper around
 * pipespawnv to eliminate redundancy...
 */
pid_t
pipespawn(
    char *	prog,
    int		pipedef,
    int *	stdinfd,
    int *	stdoutfd,
    int *	stderrfd,
    ...)
{
    va_list ap;
    int argc = 0, i;
    pid_t pid;
    char **argv;

    /* count args */
    arglist_start(ap, stderrfd);
    while(arglist_val(ap, char *) != NULL) {
	argc++;
    }
    arglist_end(ap);

    /*
     * Create the argument vector.
     */
    arglist_start(ap, stderrfd);
    argv = (char **)alloc((argc + 1) * SIZEOF(*argv));
    i = 0;
    while((argv[i] = arglist_val(ap, char *)) != NULL) {
        if (argv[i] != skip_argument) {
	    i++;
        }
    }
    arglist_end(ap);

    pid = pipespawnv_passwd(prog, pipedef, stdinfd, stdoutfd, stderrfd, argv);
    amfree(argv);
    return pid;
}

pid_t
pipespawnv(
    char *	prog,
    int		pipedef,
    int *	stdinfd,
    int *	stdoutfd,
    int *	stderrfd,
    char **	my_argv)
{
    return pipespawnv_passwd(prog, pipedef, stdinfd, stdoutfd, stderrfd,
	my_argv);
}

pid_t
pipespawnv_passwd(
    char *	prog,
    int		pipedef,
    int *	stdinfd,
    int *	stdoutfd,
    int *	stderrfd,
    char **	my_argv)
{
    int argc;
    pid_t pid;
    int i, inpipe[2], outpipe[2], errpipe[2], passwdpipe[2];
    char number[NUM_STR_SIZE];
    char **arg;
    char *e;
    char **env;
    char **newenv;
    char *passwdvar = NULL;
    int  *passwdfd = NULL;

    /*
     * Log the command line and count the args.
     */
    dbprintf(("%s: spawning %s in pipeline\n", debug_prefix_time(NULL), prog));
    dbprintf(("%s: argument list:", debug_prefix_time(NULL)));
    if ((pipedef & PASSWD_PIPE) != 0) {
	passwdvar = *my_argv++;
	passwdfd  = (int *)*my_argv++;
    }
    memset(inpipe, -1, SIZEOF(inpipe));
    memset(outpipe, -1, SIZEOF(outpipe));
    memset(errpipe, -1, SIZEOF(errpipe));
    memset(passwdpipe, -1, SIZEOF(passwdpipe));
    argc = 0;
    for(arg = my_argv; *arg != NULL; arg++) {
	char *quoted;

	if (*arg != skip_argument) {
	    argc++;
	    quoted = quote_string(*arg);
	    dbprintf((" %s", quoted));
	    amfree(quoted);
	}
    }
    dbprintf(("\n"));

    /*
     * Create the pipes
     */
    if ((pipedef & STDIN_PIPE) != 0) {
	if(pipe(inpipe) == -1) {
	    error("error [open pipe to %s: %s]", prog, strerror(errno));
	    /*NOTREACHED*/
	}
    }
    if ((pipedef & STDOUT_PIPE) != 0) {
	if(pipe(outpipe) == -1) {
	    error("error [open pipe to %s: %s]", prog, strerror(errno));
	    /*NOTREACHED*/
	}
    }
    if ((pipedef & STDERR_PIPE) != 0) {
	if(pipe(errpipe) == -1) {
	    error("error [open pipe to %s: %s]", prog, strerror(errno));
	    /*NOTREACHED*/
	}
    }
    if ((pipedef & PASSWD_PIPE) != 0) {
	if(pipe(passwdpipe) == -1) {
	    error("error [open pipe to %s: %s]", prog, strerror(errno));
	    /*NOTREACHED*/
	}
    }

    /*
     * Fork and set up the return or run the program.
     */
    switch(pid = fork()) {
    case -1:
	e = strerror(errno);
	error("error [fork %s: %s]", prog, e);
	/*NOTREACHED*/

    default:	/* parent process */
	if ((pipedef & STDIN_PIPE) != 0) {
	    aclose(inpipe[0]);		/* close input side of pipe */
	    *stdinfd = inpipe[1];
	}
	if ((pipedef & STDOUT_PIPE) != 0) {
	    aclose(outpipe[1]);		/* close output side of pipe */
	    *stdoutfd = outpipe[0];
	}
	if ((pipedef & STDERR_PIPE) != 0) {
	    aclose(errpipe[1]);		/* close output side of pipe */
	    *stderrfd = errpipe[0];
	}
	if ((pipedef & PASSWD_PIPE) != 0) {
	    aclose(passwdpipe[0]);	/* close input side of pipe */
	    *passwdfd = passwdpipe[1];
	}
	break;
    case 0:		/* child process */
	if ((pipedef & STDIN_PIPE) != 0) {
	    aclose(inpipe[1]);		/* close output side of pipe */
	} else {
	    inpipe[0] = *stdinfd;
	}
	if ((pipedef & STDOUT_PIPE) != 0) {
	    aclose(outpipe[0]);		/* close input side of pipe */
	} else {
	    outpipe[1] = *stdoutfd;
	}
	if ((pipedef & STDERR_PIPE) != 0) {
	    aclose(errpipe[0]);		/* close input side of pipe */
	} else {
	    errpipe[1] = *stderrfd;
	}
        if ((pipedef & PASSWD_PIPE) != 0) { 
            aclose(passwdpipe[1]);      /* close output side of pipe */
        }

	/*
	 * Shift the pipes to the standard file descriptors as requested.
	 */
	if(dup2(inpipe[0], 0) == -1) {
	    error("error [spawn %s: dup2 in: %s]", prog, strerror(errno));
	    /*NOTREACHED*/
	}
	if(dup2(outpipe[1], 1) == -1) {
	    error("error [spawn %s: dup2 out: %s]", prog, strerror(errno));
	    /*NOTREACHED*/
	}
	if(dup2(errpipe[1], 2) == -1) {
	    error("error [spawn %s: dup2 err: %s]", prog, strerror(errno));
	    /*NOTREACHED*/
	}

	/*
	 * Get the "safe" environment.  If we are sending a password to
	 * the child via a pipe, add the environment variable for that.
	 */
	env = safe_env();
	if ((pipedef & PASSWD_PIPE) != 0) {
	    for (i = 0; env[i] != NULL; i++)
		(void)i; /* make lint happy and do nothing */	
	    newenv = (char **)alloc((i + 1 + 1) * SIZEOF(*newenv));
	    snprintf(number, SIZEOF(number), "%d", passwdpipe[0]);
	    newenv[0] = vstralloc(passwdvar, "=", number, NULL);
	    for(i = 0; env[i] != NULL; i++)
	    	newenv[i + 1] = env[i];
	    newenv[i + 1] = NULL;
	    amfree(env);
	    env = newenv;
	}

	execve(prog, my_argv, env);
	e = strerror(errno);
	error("error [exec %s: %s]", prog, e);
	/*NOTREACHED*/
    }
    return pid;
}
