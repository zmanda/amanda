/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#include "amanda.h"
#include "amxfer.h"
#include "event.h"
#include "util.h"

/*
 * Class declaration
 *
 * This declaration is entirely private; nothing but xfer_filter_process() references
 * it directly.
 */

GType xfer_filter_process_get_type(void);
#define XFER_FILTER_PROCESS_TYPE (xfer_filter_process_get_type())
#define XFER_FILTER_PROCESS(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_filter_process_get_type(), XferFilterProcess)
#define XFER_FILTER_PROCESS_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_filter_process_get_type(), XferFilterProcess const)
#define XFER_FILTER_PROCESS_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_filter_process_get_type(), XferFilterProcessClass)
#define IS_XFER_FILTER_PROCESS(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_filter_process_get_type ())
#define XFER_FILTER_PROCESS_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_filter_process_get_type(), XferFilterProcessClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferFilterProcess {
    XferElement __parent__;

    gchar **argv;
    gboolean need_root;
    int pipe_err[2];

    pid_t child_pid;
    GSource *child_watch;
    gboolean child_killed;
} XferFilterProcess;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
    int (*get_err_fd)(XferFilterProcess *elt);

} XferFilterProcessClass;

/*
 * Implementation
 */

static void
child_watch_callback(
    pid_t pid,
    gint status,
    gpointer data)
{
    XferFilterProcess *self = XFER_FILTER_PROCESS(data);
    XferElement *elt = (XferElement *)self;
    XMsg *msg;
    char *errmsg = NULL;

    g_assert(pid == self->child_pid);
    self->child_pid = -1; /* it's gone now.. */

    if (WIFEXITED(status)) {
	int exitcode = WEXITSTATUS(status);
	g_debug("%s: process exited with status %d", xfer_element_repr(elt), exitcode);
	if (exitcode != 0) {
	    errmsg = g_strdup_printf("%s exited with status %d",
		self->argv[0], exitcode);
	}
    } else if (WIFSIGNALED(status)) {
	int signal = WTERMSIG(status);
	if (signal != SIGKILL || !self->child_killed) {
	    errmsg = g_strdup_printf("%s died on signal %d", self->argv[0], signal);
	    g_debug("%s: %s", xfer_element_repr(elt), errmsg);
	}
    }

    /* if this is an error exit, send an XMSG_ERROR and cancel */
    if (errmsg) {
	msg = xmsg_new(XFER_ELEMENT(self), XMSG_ERROR, 0);
	msg->message = errmsg;
	xfer_queue_message(XFER_ELEMENT(self)->xfer, msg);

	xfer_cancel(elt->xfer);

	/* this element is as good as cancelled already, so fall through to XMSG_DONE */
    }

    xfer_queue_message(XFER_ELEMENT(self)->xfer, xmsg_new(XFER_ELEMENT(self), XMSG_DONE, 0));
}

static int
get_err_fd_impl(
    XferFilterProcess *xfp)
{
    return xfp->pipe_err[0];
}

static gboolean
start_impl(
    XferElement *elt)
{
    XferFilterProcess *self = (XferFilterProcess *)elt;
    char *cmd_str;
    char **argv;
    char *errmsg;
    char **env;
    int rfd, wfd;

    /* first build up a log message of what we're going to do, properly shell quoted */
    argv = self->argv;
    cmd_str = g_shell_quote(*(argv++));
    while (*argv) {
	char *qarg = g_shell_quote(*(argv++));
	cmd_str = newvstralloc(cmd_str, cmd_str, " ", qarg, NULL);
	g_free(qarg);
    }
    g_debug("%s spawning: %s", xfer_element_repr(elt), cmd_str);

    rfd = xfer_element_swap_output_fd(elt->upstream, -1);
    wfd = xfer_element_swap_input_fd(elt->downstream, -1);

    /* now fork off the child and connect the pipes */
    switch (self->child_pid = fork()) {
	case -1:
	    error("cannot fork: %s", strerror(errno));
	    /* NOTREACHED */

	case 0: /* child */
	    /* first, copy our fd's out of the stdio range */
	    while (rfd <= STDERR_FILENO)
		rfd = dup(rfd);
	    while (wfd <= STDERR_FILENO)
		wfd = dup(wfd);

	    /* set up stdin, stdout, and stderr, overwriting anything already open
	     * on those fd's */
	    dup2(rfd, STDIN_FILENO);
	    dup2(wfd, STDOUT_FILENO);
	    dup2(self->pipe_err[1], STDERR_FILENO);

	    /* and close everything else */
	    safe_fd(-1, 0);
	    env = safe_env();

	    if (self->need_root && !become_root()) {
		errmsg = g_strdup_printf("could not become root: %s\n", strerror(errno));
		full_write(STDERR_FILENO, errmsg, strlen(errmsg));
		exit(1);
	    }

	    execve(self->argv[0], self->argv, env);
	    errmsg = g_strdup_printf("exec failed: %s\n", strerror(errno));
	    full_write(STDERR_FILENO, errmsg, strlen(errmsg));
	    exit(1);

	default: /* parent */
	    break;
    }
    g_free(cmd_str);

    /* close the pipe fd's */
    close(rfd);
    close(wfd);
    close(self->pipe_err[1]);

    /* watch for child death */
    self->child_watch = new_child_watch_source(self->child_pid);
    g_source_set_callback(self->child_watch,
	    (GSourceFunc)child_watch_callback, self, NULL);
    g_source_attach(self->child_watch, NULL);
    g_source_unref(self->child_watch);

    return TRUE;
}

static gboolean
cancel_impl(
    XferElement *elt,
    gboolean expect_eof)
{
    XferFilterProcess *self = (XferFilterProcess *)elt;

    /* chain up first */
    XFER_ELEMENT_CLASS(parent_class)->cancel(elt, expect_eof);

    /* if the process is running as root, we can't do anything but wait until
     * we get an upstream EOF, or downstream does something to trigger a
     * SIGPIPE */
    if (self->need_root)
	return expect_eof;

    /* avoid the risk of SIGPIPEs by not killing the process if it is already
     * expecting an EOF */
    if (expect_eof) {
	return expect_eof;
    }

    /* and kill the process, if it's not already dead; this will likely send
     * SIGPIPE to anything upstream. */
    if (self->child_pid != -1) {
	g_debug("%s: killing child process", xfer_element_repr(elt));
	if (kill(self->child_pid, SIGKILL) < 0) {
	    /* log but ignore */
	    g_debug("while killing child process: %s", strerror(errno));
	    return FALSE; /* downstream should not expect EOF */
	}

	/* make sure we don't send an XMSG_ERROR about this */
	self->child_killed = 1;
    }

    return TRUE; /* downstream should expect an EOF */
}

static void
instance_init(
    XferElement *elt)
{
    XferFilterProcess *self = (XferFilterProcess *)elt;

    /* we can generate an EOF *unless* the process is running as root */
    elt->can_generate_eof = !self->need_root;

    self->argv = NULL;
    self->child_pid = -1;
    self->child_killed = FALSE;
}

static void
finalize_impl(
    GObject * obj_self)
{
    XferFilterProcess *self = XFER_FILTER_PROCESS(obj_self);

    if (self->argv)
	g_strfreev(self->argv);

    /* chain up */
    G_OBJECT_CLASS(parent_class)->finalize(obj_self);
}

static void
class_init(
    XferFilterProcessClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    GObjectClass *goc = (GObjectClass*) klass;
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_READFD, XFER_MECH_WRITEFD, XFER_NROPS(1), XFER_NTHREADS(0) },
	{ XFER_MECH_NONE, XFER_MECH_NONE, XFER_NROPS(0), XFER_NTHREADS(0) },
    };

    klass->start = start_impl;
    klass->cancel = cancel_impl;

    klass->perl_class = "Amanda::Xfer::Filter::Process";
    klass->mech_pairs = mech_pairs;
    selfc->get_err_fd = get_err_fd_impl;

    goc->finalize = finalize_impl;

    parent_class = g_type_class_peek_parent(selfc);
}

GType
xfer_filter_process_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferFilterProcessClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferFilterProcess),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferFilterProcess", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_filter_process(
    gchar **argv,
    gboolean need_root)
{
    XferFilterProcess *xfp = (XferFilterProcess *)g_object_new(XFER_FILTER_PROCESS_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(xfp);

    if (!argv || !*argv)
	error("xfer_filter_process got a NULL or empty argv");

    xfp->argv = argv;
    xfp->need_root = need_root;
    if (pipe(xfp->pipe_err) < 0) {
	g_critical(_("Can't create pipe: %s"), strerror(errno));
    }
    return elt;
}

int get_err_fd(XferElement *elt);
int get_err_fd(
    XferElement *elt)
{
    XferFilterProcessClass *klass;
    g_assert(IS_XFER_FILTER_PROCESS(elt));

    klass = XFER_FILTER_PROCESS_GET_CLASS(elt);
    if (klass->get_err_fd)
	return klass->get_err_fd(XFER_FILTER_PROCESS(elt));
    else
        return 0;
}
