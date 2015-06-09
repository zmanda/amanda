/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1999 University of Maryland
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
 * $Id$
 *
 * ssl-security.c - security and transport over ssl or a ssl-like command.
 *
 * XXX still need to check for initial keyword on connect so we can skip
 * over shell garbage and other stuff that ssl might want to spew out.
 */

#include "amanda.h"
#include "amutil.h"
#include "event.h"
#include "packet.h"
#include "security.h"
#include "security-util.h"
#include "sockaddr-util.h"
#include "stream.h"
#include "version.h"
#include <openssl/crypto.h>
#include <openssl/ssl.h>
#include <openssl/err.h>

/*
 * Number of seconds ssl has to start up
 */
#define	CONNECT_TIMEOUT	20

/*
 * Interface functions
 */
static void ssl_accept(const struct security_driver *,
    char *(*)(char *, void *),
    int, int,
    void (*)(security_handle_t *, pkt_t *),
    void *);
static void ssl_connect(const char *,
    char *(*)(char *, void *),
    void (*)(void *, security_handle_t *, security_status_t), void *, void *);
static ssize_t ssl_data_write(void *c, struct iovec *iov, int iovcnt);
static ssize_t ssl_data_read(void *c, void *bug, size_t size, int timeout);
static void init_ssl(void);

/*
 * This is our interface to the outside world.
 */
const security_driver_t ssl_security_driver = {
    "SSL",
    ssl_connect,
    ssl_accept,
    sec_get_authenticated_peer_name_hostname,
    sec_close,
    stream_sendpkt,
    stream_recvpkt,
    stream_recvpkt_cancel,
    tcpma_stream_server,
    tcpma_stream_accept,
    tcpma_stream_client,
    tcpma_stream_close,
    sec_stream_auth,
    sec_stream_id,
    tcpm_stream_write,
    tcpm_stream_read,
    tcpm_stream_read_sync,
    tcpm_stream_read_cancel,
    tcpm_close_connection,
    NULL,
    NULL,
    ssl_data_write,
    ssl_data_read
};

static int newhandle = 1;

/*
 * Local functions
 */
static int runssl(struct sec_handle *, in_port_t port,
		  char *src_ip,
                  char *ssl_fingerprint_file, char *ssl_cert_file,
                  char *ssl_key_file, char *ssl_ca_cert_file,
                  char *ssl_cipher_list, int ssl_check_certificate_host);


/*
 * ssl version of a security handle allocator.  Logically sets
 * up a network "connection".
 */
static void
ssl_connect(
    const char *hostname,
    char *	(*conf_fn)(char *, void *),
    void	(*fn)(void *, security_handle_t *, security_status_t),
    void *	arg,
    void *	datap)
{
    struct sec_handle *rh;
    int result;
    char *canonname;
    char *service;
    in_port_t port;
    char *src_ip = NULL;
    char *ssl_dir = NULL;
    char *ssl_fingerprint_file = NULL;
    char *ssl_cert_file = NULL;
    char *ssl_key_file = NULL;
    char *ssl_ca_cert_file = NULL;
    char *ssl_cipher_list = NULL;
    int   ssl_check_certificate_host = 1;

    assert(fn != NULL);
    assert(hostname != NULL);

    auth_debug(1, _("ssl: ssl_connect: %s\n"), hostname);

    rh = g_new0(struct sec_handle, 1);
    security_handleinit(&rh->sech, &ssl_security_driver);
    rh->hostname = NULL;
    rh->rs = NULL;
    rh->ev_timeout = NULL;
    rh->rc = NULL;

    result = resolve_hostname(hostname, 0, NULL, &canonname);
    if(result != 0) {
	g_debug(_("resolve_hostname(%s): %s"), hostname, gai_strerror(result));
	security_seterror(&rh->sech, _("resolve_hostname(%s): %s\n"), hostname,
			  gai_strerror(result));
	(*fn)(arg, &rh->sech, S_ERROR);
	return;
    }
    if (canonname == NULL) {
	g_debug(_("resolve_hostname(%s) did not return a canonical name"), hostname);
	security_seterror(&rh->sech,
	        _("resolve_hostname(%s) did not return a canonical name\n"), hostname);
	(*fn)(arg, &rh->sech, S_ERROR);
       return;
    }

    rh->hostname = canonname;	/* will be replaced */
    canonname = NULL; /* steal reference */
    rh->rs = tcpma_stream_client(rh, newhandle++);
    rh->rc->recv_security_ok = &bsd_recv_security_ok;
    rh->rc->prefix_packet = &bsd_prefix_packet;
    rh->rc->need_priv_port = 0;

    if (rh->rs == NULL)
	goto error;

    amfree(rh->hostname);
    rh->hostname = g_strdup(rh->rs->rc->hostname);

    ssl_dir = getconf_str(CNF_SSL_DIR);
    if (conf_fn) {
	service = conf_fn("remote_port", datap);
	if (!service || strlen(service) <= 1)
	    service = AMANDA_SERVICE_NAME;
	g_debug("Connecting to service '%s'", service);
	src_ip = conf_fn("src_ip", datap);
	ssl_fingerprint_file = g_strdup(conf_fn("ssl_fingerprint_file", datap));
	ssl_cert_file        = g_strdup(conf_fn("ssl_cert_file", datap));
	ssl_key_file         = g_strdup(conf_fn("ssl_key_file", datap));
	ssl_ca_cert_file     = g_strdup(conf_fn("ssl_ca_cert_file", datap));
	ssl_cipher_list      = conf_fn("ssl_cipher_list", datap);
	ssl_check_certificate_host =
			    atoi(conf_fn("ssl_check_certificate_host", datap));
    } else {
	service = AMANDA_SERVICE_NAME;
    }

    if (ssl_dir) {
	if (!ssl_cert_file || ssl_cert_file == '\0') {
	    ssl_cert_file = g_strdup_printf("%s/me/crt.pem", ssl_dir);
	}
	if (!ssl_key_file || ssl_key_file == '\0') {
	    ssl_key_file = g_strdup_printf("%s/me/private/key.pem", ssl_dir);
	}
	if (!ssl_ca_cert_file || ssl_ca_cert_file == '\0') {
	    ssl_ca_cert_file = g_strdup_printf("%s/CA/crt.pem", ssl_dir);
	}
	if (!ssl_fingerprint_file || ssl_fingerprint_file == '\0') {
	    struct stat  statbuf;
	    ssl_fingerprint_file = g_strdup_printf("%s/remote/%s/fingerprint", ssl_dir, rh->hostname);
	    if (stat(ssl_fingerprint_file, &statbuf) == -1) {
		g_free(ssl_fingerprint_file);
		ssl_fingerprint_file = NULL;
	    }
	}
    }

    port = find_port_for_service(service, "tcp");
    if (port == 0) {
	security_seterror(&rh->sech, _("%s/tcp unknown protocol"), service);
	goto error;
    }

    /*
     * We need to open a new connection.
     */
    if(rh->rc->read == -1) {
	if (runssl(rh, port, src_ip, ssl_fingerprint_file,
                   ssl_cert_file, ssl_key_file,
		   ssl_ca_cert_file, ssl_cipher_list,
		   ssl_check_certificate_host) < 0)
	    goto error;
	rh->rc->refcnt++;
    }

    g_free(ssl_fingerprint_file);
    g_free(ssl_cert_file);
    g_free(ssl_key_file);
    g_free(ssl_ca_cert_file);
    /*
     * The socket will be opened async so hosts that are down won't
     * block everything.  We need to register a write event
     * so we will know when the socket comes alive.
     *
     * Overload rh->rs->ev_read to provide a write event handle.
     * We also register a timeout.
     */
    rh->fn.connect = fn;
    rh->arg = arg;
    rh->rs->ev_read = event_register((event_id_t)(rh->rs->rc->write),
	EV_WRITEFD, sec_connect_callback, rh);
    rh->ev_timeout = event_register(CONNECT_TIMEOUT, EV_TIME,
	sec_connect_timeout, rh);

    return;

error:
    (*fn)(arg, &rh->sech, S_ERROR);
}

static char *validate_fingerprints(X509 *cert, char *ssl_fingerprint_file);

static char *
validate_fingerprints(
    X509 *cert,
    char *ssl_fingerprint_file)
{
    FILE *fingers;
    char fingerprint[32768];
    char *errmsg = NULL;

    unsigned char  md5[EVP_MAX_MD_SIZE + 1];
    unsigned int   len_md5;
    const EVP_MD  *evp_md5;
    char *md5_fingerprint;
    unsigned char  sha1[EVP_MAX_MD_SIZE + 1];
    unsigned int   len_sha1;
    const EVP_MD  *evp_sha1;
    char *sha1_fingerprint;
    char *fp;
    unsigned int   i;

    const char *md5_const  = "MD5 Fingerprint=";
    const char *sha1_const = "SHA1 Fingerprint=";
    const size_t md5_const_len = strlen(md5_const);
    const size_t sha1_const_len = strlen(sha1_const);

    if (ssl_fingerprint_file == NULL) {
	g_debug("No fingerprint_file set");
	return NULL;
    }

    evp_md5 = EVP_get_digestbyname("MD5");
    if (!evp_md5) {
	auth_debug(1, _("EVP_get_digestbyname(MD5) failed"));
    }
    if (!X509_digest(cert, evp_md5, md5, &len_md5)) {
	auth_debug(1, _("cannot get MD5 digest"));
    }

    md5_fingerprint  = malloc(len_md5*3 + 1);
    fp = md5_fingerprint;
    for (i=0; i < len_md5; i++) {
	snprintf(fp, 4, "%02X:", (unsigned) md5[i]);
	fp+=3;
    }
    /* remove latest ':' */
    fp --;
    *fp = '\0';
    auth_debug(1, _("md5: %s\n"), md5_fingerprint);

    evp_sha1 = EVP_get_digestbyname("SHA1");
    if (!evp_sha1) {
	auth_debug(1, _("EVP_get_digestbyname(SHA1) failed"));
    }
    if (!X509_digest(cert, evp_sha1, sha1, &len_sha1)) {
	auth_debug(1, _("cannot get SHA1 digest"));
    }

    sha1_fingerprint  = malloc(len_sha1*3 + 1);
    fp = sha1_fingerprint;
    for (i=0; i < len_sha1; i++) {
	snprintf(fp, 4, "%02X:", (unsigned) sha1[i]);
	fp+=3;
    }
    /* remove latest ':' */
    fp --;
    *fp = '\0';
    auth_debug(1, _("sha1: %s\n"), sha1_fingerprint);

    fingers = fopen(ssl_fingerprint_file, "r");
    if (!fingers) {
	errmsg = g_strdup_printf("Failed open of %s: %s",
				 ssl_fingerprint_file, strerror(errno));
	g_debug("%s", errmsg);
	g_free(md5_fingerprint);
	g_free(sha1_fingerprint);
	return errmsg;
    }

    while (fgets(fingerprint, 32768, fingers) != NULL) {
	int len = strlen(fingerprint)-1;
	if (len > 0 && fingerprint[len] == '\n')
	    fingerprint[len] = '\0';
	if (strncmp(md5_const, fingerprint, md5_const_len) == 0) {
	    if (strcmp(md5_fingerprint, fingerprint+md5_const_len) == 0) {
		g_debug("MD5 fingerprint '%s' match", md5_fingerprint);
		g_free(md5_fingerprint);
		g_free(sha1_fingerprint);
		fclose(fingers);
		return NULL;
	    }
	} else if (strncmp(sha1_const, fingerprint, sha1_const_len) == 0) {
	    if (strcmp(sha1_fingerprint, fingerprint+sha1_const_len) == 0) {
		g_debug("SHA1 fingerprint '%s' match", sha1_fingerprint);
		g_free(md5_fingerprint);
		g_free(sha1_fingerprint);
		fclose(fingers);
		return NULL;
	    }
	}
	auth_debug(1, _("Fingerprint '%s' doesn't match\n"), fingerprint);
    }
    g_free(md5_fingerprint);
    g_free(sha1_fingerprint);
    fclose(fingers);
    return g_strdup_printf("No fingerprint match");;
}

/*
 * Setup to handle new incoming connections
 */
static void
ssl_accept(
    const struct security_driver *driver,
    char *	(*conf_fn)(char *, void *),
    int		in,
    int		out,
    void	(*fn)(security_handle_t *, pkt_t *),
    void       *datap)
{
    sockaddr_union sin;
    socklen_t_equiv len = sizeof(struct sockaddr);
    struct tcp_conn *rc;
    char hostname[NI_MAXHOST];
    int result;
    char *errmsg = NULL;
    int   err;
    X509 *remote_cert;
    char *str;
    X509_NAME *x509_name;
    char *cert_hostname;
    SSL_CTX            *ctx;
    SSL                *ssl;
    int loc;
    char *ssl_dir = getconf_str(CNF_SSL_DIR);
    char *ssl_fingerprint_file = conf_fn("ssl_fingerprint_file", datap);
    char *ssl_cert_file        = conf_fn("ssl_cert_file", datap);
    char *ssl_key_file         = conf_fn("ssl_key_file", datap);
    char *ssl_ca_cert_file     = conf_fn("ssl_ca_cert_file", datap);
    char *ssl_cipher_list      = conf_fn("ssl_cipher_list", datap);
    int   ssl_check_host       = atoi(conf_fn("ssl_check_host", datap));
    int   ssl_check_certificate_host = atoi(conf_fn("ssl_check_certificate_host", datap));

    if (getpeername(in, (struct sockaddr *)&sin, &len) < 0) {
	g_debug(_("getpeername returned: %s"), strerror(errno));
	return;
    }
    if ((result = getnameinfo((struct sockaddr *)&sin, len,
			      hostname, NI_MAXHOST, NULL, 0, 0) != 0)) {
	g_debug(_("getnameinfo failed: %s"),
		  gai_strerror(result));
	return;
    }

    if (ssl_check_host && check_name_give_sockaddr(hostname,
				 (struct sockaddr *)&sin, &errmsg) < 0) {
	amfree(errmsg);
	return;
    }

    if (ssl_dir) {
	if (!ssl_cert_file || ssl_cert_file == '\0') {
	    ssl_cert_file = g_strdup_printf("%s/me/crt.pem", ssl_dir);
	}
	if (!ssl_key_file || ssl_key_file == '\0') {
	    ssl_key_file = g_strdup_printf("%s/me/private/key.pem", ssl_dir);
	}
	if (!ssl_ca_cert_file || ssl_ca_cert_file == '\0') {
	    ssl_ca_cert_file = g_strdup_printf("%s/CA/crt.pem", ssl_dir);
	}
    }

    if (!ssl_cert_file) {
	g_debug(_("ssl-cert-file must be set in amanda-remote.conf"));
	return;
    }

    if (!ssl_key_file) {
	g_debug(_("ssl-key-file must be set in amanda-remote.conf"));
	return;
    }

    if (!ssl_ca_cert_file) {
	g_debug(_("ssl_ca_cert_file must be set in amanda-remote.conf"));
	return;
    }

    len = sizeof(sin);
    init_ssl();

    /* Create a SSL_CTX structure */
    ctx = SSL_CTX_new(SSLv3_server_method());
    if (!ctx) {
	g_debug(_("SSL_CTX_new failed: %s"),
		 ERR_error_string(ERR_get_error(), NULL));
	return;
    }
    SSL_CTX_set_mode(ctx, SSL_MODE_AUTO_RETRY);

    if (ssl_cipher_list) {
	g_debug("Set ssl_cipher_list to %s", ssl_cipher_list);
	if (SSL_CTX_set_cipher_list(ctx, ssl_cipher_list) == 0) {
	    g_debug(_("SSL_CTX_set_cipher_list failed: %s"),
		     ERR_error_string(ERR_get_error(), NULL));
	    return;
	}
    }

    /* Load the me certificate into the SSL_CTX structure */
    g_debug(_("Loading ssl-cert-file certificate %s"), ssl_cert_file);
    if (SSL_CTX_use_certificate_file(ctx, ssl_cert_file,
				     SSL_FILETYPE_PEM) <= 0) {
	g_debug(_("Load ssl-cert-file failed: %s"),
		 ERR_error_string(ERR_get_error(), NULL));
	return;
    }

    /* Load the private-key corresponding to the me certificate */
    g_debug(_("Loading ssl-key-file private-key %s"), ssl_key_file);
    if (SSL_CTX_use_PrivateKey_file(ctx, ssl_key_file,
				    SSL_FILETYPE_PEM) <= 0) {
	g_debug(_("Load ssl-key-file failed: %s"),
		 ERR_error_string(ERR_get_error(), NULL));
	return;
    }

    if (ssl_ca_cert_file) {
        /* Load the RSA CA certificate into the SSL_CTX structure */
	g_debug(_("Loading ssl-ca-cert-file ca certificate %s"),
		 ssl_ca_cert_file);
        if (!SSL_CTX_load_verify_locations(ctx, ssl_ca_cert_file, NULL)) {
	    g_debug(_("Load ssl-ca-cert-file failed: %s"),
		     ERR_error_string(ERR_get_error(), NULL));
	    return;
        }

	/* Set to require peer (remote) certificate verification */
	SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);

	/* Set the verification depth to 1 */
	SSL_CTX_set_verify_depth(ctx,1);
    }

    ssl = SSL_new(ctx);
    if (!ssl) {
	g_debug(_("SSL_new failed: %s"),
		 ERR_error_string(ERR_get_error(), NULL));
	return;
    }
    SSL_set_accept_state(ssl);

    /* Assign the socket into the SSL structure (SSL and socket without BIO) */
    SSL_set_fd(ssl, in);

    /* Perform SSL Handshake on the SSL me */
    err = SSL_accept(ssl);
    if (err == -1) {
	g_debug(_("SSL_accept failed: %s"),
		 ERR_error_string(ERR_get_error(), NULL));
	return;
    }

    /* Get the me's certificate (optional) */
    remote_cert = SSL_get_peer_certificate (ssl);

    if (remote_cert == NULL) {
	g_debug(_("remote doesn't sent a certificate"));
	return;
    }

    x509_name = X509_get_subject_name(remote_cert);
    str = X509_NAME_oneline(X509_get_subject_name(remote_cert), 0, 0);
    auth_debug(1, _("\t subject: %s\n"), str);
    amfree (str);

    str = X509_NAME_oneline(X509_get_issuer_name(remote_cert), 0, 0);
    auth_debug(1, _("\t issuer: %s\n"), str);
    amfree(str);

    loc = -1;
    loc = X509_NAME_get_index_by_NID(x509_name, NID_commonName, loc);
    if (loc != -1) {
	X509_NAME_ENTRY *x509_entry = X509_NAME_get_entry(x509_name, loc);
	ASN1_STRING *asn1_string = X509_NAME_ENTRY_get_data(x509_entry);
	cert_hostname = (char *)ASN1_STRING_data(asn1_string);
	auth_debug(1, "common_name: %s\n", cert_hostname);

	if (ssl_check_certificate_host &&
	    check_name_give_sockaddr((char*)cert_hostname,
			 (struct sockaddr *)&sin, &errmsg) < 0) {
	    g_debug("Common name of certicate (%s) doesn't resolv to IP (%s)", cert_hostname, str_sockaddr(&sin));
	    amfree(errmsg);
	    X509_free(remote_cert);
	    return;
	}
    } else {
	g_debug("Certificate have no common name");
	X509_free(remote_cert);
	return;
    }

    if (ssl_dir) {
	if (!ssl_fingerprint_file || ssl_fingerprint_file == '\0') {
	    struct stat  statbuf;
	    ssl_fingerprint_file = g_strdup_printf("%s/remote/%s/fingerprint", ssl_dir, cert_hostname);
	    if (stat(ssl_fingerprint_file, &statbuf) == -1) {
		g_free(ssl_fingerprint_file);
		ssl_fingerprint_file = NULL;
	    }
	}
    }

    if (ssl_fingerprint_file) {
        g_debug(_("Loading ssl-fingerprint-file %s"), ssl_fingerprint_file);
	str = validate_fingerprints(remote_cert, ssl_fingerprint_file);
	if (str) {
	    g_debug("%s", str);
	    amfree(str);
	    X509_free(remote_cert);
	    return;
	}
    }
    X509_free(remote_cert);

    rc = sec_tcp_conn_get(hostname, 0);
    rc->recv_security_ok = &bsd_recv_security_ok;
    rc->prefix_packet = &bsd_prefix_packet;
    rc->need_priv_port = 0;
    copy_sockaddr(&rc->peer, &sin);
    rc->read = in;
    rc->write = out;
    rc->accept_fn = fn;
    rc->driver = driver;
    rc->conf_fn = conf_fn;
    rc->datap = datap;
    rc->ctx = ctx;
    rc->ssl = ssl;
    strncpy(rc->hostname, cert_hostname, sizeof(rc->hostname)-1);

    g_debug(_("SSL_cipher: %s"), SSL_get_cipher(rc->ssl));

    sec_tcp_conn_read(rc);
}

/*
 * Open a ssl connection to the host listed in rc->hostname
 * Returns negative on error, with an errmsg in rc->errmsg.
 */
static int
runssl(
    struct sec_handle *	rh,
    in_port_t port,
    char *src_ip,
    char *ssl_fingerprint_file,
    char *ssl_cert_file,
    char *ssl_key_file,
    char *ssl_ca_cert_file,
    char *ssl_cipher_list,
    int   ssl_check_certificate_host)
{
    int		     my_socket;
    in_port_t	     my_port;
    struct tcp_conn *rc = rh->rc;
    int              err;
    X509            *remote_cert;
    sockaddr_union   sin;
    socklen_t_equiv  len;

    if (!ssl_key_file) {
	security_seterror(&rh->sech, _("ssl-key-file must be set"));
	return -1;
    }

    if (!ssl_cert_file) {
	security_seterror(&rh->sech, _("ssl-cert-file must be set"));
	return -1;
    }

    my_socket = stream_client(src_ip,
				  rc->hostname,
				  port,
				  STREAM_BUFSIZE,
				  STREAM_BUFSIZE,
				  &my_port,
				  0);

    if(my_socket < 0) {
	security_seterror(&rh->sech,
	    "%s", strerror(errno));

	return -1;
    }

    rc->read = rc->write = my_socket;

    len = sizeof(sin);
    if (getpeername(my_socket, (struct sockaddr *)&sin, &len) < 0) {
	security_seterror(&rh->sech, _("getpeername returned: %s\n"), strerror(errno));
	return -1;
    }
    copy_sockaddr(&rc->peer, &sin);

    init_ssl();

    /* Create an SSL_CTX structure */
    rc->ctx = SSL_CTX_new(SSLv3_client_method());
    if (!rc->ctx) {
	security_seterror(&rh->sech, "%s",
			  ERR_error_string(ERR_get_error(), NULL));
	return -1;
    }
    SSL_CTX_set_mode(rc->ctx, SSL_MODE_AUTO_RETRY);

    if (ssl_cipher_list) {
	g_debug("Set ssl_cipher_list to %s", ssl_cipher_list);
	if (SSL_CTX_set_cipher_list(rc->ctx, ssl_cipher_list) == 0) {
	    security_seterror(&rh->sech, "%s",
		              ERR_error_string(ERR_get_error(), NULL));
	    return -1;
	}
    }

    /* Load the private-key corresponding to the remote certificate */
    g_debug("Loading ssl-key-file private-key %s", ssl_key_file);
    if (SSL_CTX_use_PrivateKey_file(rc->ctx, ssl_key_file,
				    SSL_FILETYPE_PEM) <= 0) {
	security_seterror(&rh->sech, "%s",
			  ERR_error_string(ERR_get_error(), NULL));
	return -1;
    }

    /* Load the me certificate into the SSL_CTX structure */
    g_debug("Loading ssl-cert-file certificate %s", ssl_cert_file);
    if (SSL_CTX_use_certificate_file(rc->ctx, ssl_cert_file,
				     SSL_FILETYPE_PEM) <= 0) {
	security_seterror(&rh->sech, "%s",
			  ERR_error_string(ERR_get_error(), NULL));
	return -1;
    }

    /* Check if the remote certificate and private-key matches */
    if (ssl_cert_file) {
	if (!SSL_CTX_check_private_key(rc->ctx)) {
	security_seterror(&rh->sech,
		_("Private key does not match the certificate public key"));
	return -1;
	}
    }

    if (ssl_ca_cert_file) {
        /* Load the RSA CA certificate into the SSL_CTX structure */
        /* This will allow this remote to verify the me's     */
        /* certificate.                                           */
	g_debug("Loading ssl-ca-cert-file ca %s", ssl_ca_cert_file);
        if (!SSL_CTX_load_verify_locations(rc->ctx, ssl_ca_cert_file, NULL)) {
	    security_seterror(&rh->sech, "%s",
			      ERR_error_string(ERR_get_error(), NULL));
	    return -1;
        }
    } else {
	g_debug(_("no ssl-ca-cert-file defined"));
    }

    /* Set flag in context to require peer (me) certificate */
    /* verification */
    if (ssl_ca_cert_file) {
	g_debug("Enabling certification verification");
	SSL_CTX_set_verify(rc->ctx, SSL_VERIFY_PEER, NULL);
	SSL_CTX_set_verify_depth(rc->ctx, 1);
    } else {
	g_debug("Not enabling certification verification");
    }

    /* ----------------------------------------------- */
    rc->ssl = SSL_new(rc->ctx);
    if (!rc->ssl) {
	security_seterror(&rh->sech, _("SSL_new failed: %s"),
			  ERR_error_string(ERR_get_error(), NULL));
	return -1;
    }
    SSL_set_connect_state(rc->ssl);

    /* Assign the socket into the SSL structure (SSL and socket without BIO) */
    SSL_set_fd(rc->ssl, my_socket);

    /* Perform SSL Handshake on the SSL remote */
    err = SSL_connect(rc->ssl);
    if (err == -1) {
	security_seterror(&rh->sech, _("SSL_connect failed: %s"),
			  ERR_error_string(ERR_get_error(), NULL));
	return -1;
    }

    /* Get the me's certificate (optional) */
    remote_cert = SSL_get_peer_certificate(rc->ssl);

    if (remote_cert == NULL) {
	security_seterror(&rh->sech, _("server have no certificate"));
	return -1;
    } else {
        char *str;

        str = X509_NAME_oneline(X509_get_subject_name(remote_cert), 0, 0);
        auth_debug(1, _("\t subject: %s\n"), str);
        amfree (str);

        str = X509_NAME_oneline(X509_get_issuer_name(remote_cert), 0, 0);
        auth_debug(1, _("\t issuer: %s\n"), str);
        amfree(str);

	if (ssl_check_certificate_host) {
	    int   loc = -1;
	    char *errmsg = NULL;
	    X509_NAME *x509_name = X509_get_subject_name(remote_cert);

	    loc = X509_NAME_get_index_by_NID(x509_name, NID_commonName, loc);
	    if (loc != -1) {
		X509_NAME_ENTRY *x509_entry = X509_NAME_get_entry(x509_name, loc);
		ASN1_STRING *asn1_string = X509_NAME_ENTRY_get_data(x509_entry);
		char *cert_hostname =  (char *)ASN1_STRING_data(asn1_string);
		auth_debug(1, "common_name: %s\n", cert_hostname);

		if (check_name_give_sockaddr((char*)cert_hostname,
				 (struct sockaddr *)&rc->peer, &errmsg) < 0) {
		    security_seterror(&rh->sech,
		       _("Common name of certicate (%s) doesn't resolv to IP (%s): %s"),
		       cert_hostname, str_sockaddr(&rc->peer), errmsg);
		    amfree(errmsg);
		    return -1;
		}
		auth_debug(1,
		         _("Certificate common name (%s) resolve to IP (%s)\n"),
			 cert_hostname, str_sockaddr(&rc->peer));
	    } else {
		security_seterror(&rh->sech,
				  _("Certificate have no common name"));
		g_debug("Certificate have no common name");
		return -1;
	    }
	}

/*
	if (ssl_dir) {
	    if (!ssl_fingerprint_file || ssl_fingerprint_file == '\0') {
		struct stat  statbuf;
		ssl_fingerprint_file = g_strdup_printf("%s/remote/%s/fingerprint", ssl_dir, cert_hostname);
		if (stat(ssl_fingerprint_file, &statbuf) == -1) {
		    g_free(ssl_fingerprint_file);
		    ssl_fingerprint_file = NULL;
		}
	    }
	}
*/

	if (ssl_fingerprint_file) {
            g_debug(_("run_ssl: Loading ssl-fingerprint-file %s"), ssl_fingerprint_file);
	    str = validate_fingerprints(remote_cert, ssl_fingerprint_file);
	    if (str) {
		security_seterror(&rh->sech, "%s", str);
		amfree(str);
		return -1;
	    }
	}
	X509_free (remote_cert);
    }

    g_debug(_("SSL_cipher: %s"), SSL_get_cipher(rc->ssl));

    return 0;
}

static ssize_t
ssl_data_write(
    void         *c,
    struct iovec *iov,
    int           iovcnt)
{
    struct tcp_conn *rc = c;
    int              i;
    int              size;

    size = 0;
    for (i=0; i < iovcnt; i++) {
	size += SSL_write(rc->ssl, iov[i].iov_base, iov[i].iov_len);
    }
    return size;
    //return full_writev(rc->write, iov, iovcnt);
}

static ssize_t
ssl_data_read(
    void    *c,
    void    *buf,
    size_t   size,
    int      timeout G_GNUC_UNUSED)
{
    struct tcp_conn *rc = c;

    return SSL_read(rc->ssl, buf, size);
}

static void
init_ssl(void)
{
    static int init_done = 0;

    if (init_done == 0) {
	/* Load encryption & hashing algorithms for the SSL program */
	SSL_library_init();

	/* Load the error strings for SSL & CRYPTO APIs */
	SSL_load_error_strings();

	init_done = 1;
    }
}
