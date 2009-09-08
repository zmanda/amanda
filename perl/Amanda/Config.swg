/*
 * Copyright (c) 2007,2008,2009 Zmanda, Inc.  All Rights Reserved.
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

%module "Amanda::Config"
%include "amglue/amglue.swg"
%include "exception.i"

%include "Amanda/Config.pod"

%{
#include "conffile.h"
%}

/*
 * Parameter access
*/

/* All of the CNF_ flags from conffile.h */

amglue_add_enum_tag_fns(confparm_key);
amglue_add_constant(CNF_ORG, confparm_key);
amglue_add_constant(CNF_CONF, confparm_key);
amglue_add_constant(CNF_INDEX_SERVER, confparm_key);
amglue_add_constant(CNF_TAPE_SERVER, confparm_key);
amglue_add_constant(CNF_AUTH, confparm_key);
amglue_add_constant(CNF_SSH_KEYS, confparm_key);
amglue_add_constant(CNF_AMANDAD_PATH, confparm_key);
amglue_add_constant(CNF_CLIENT_USERNAME, confparm_key);
amglue_add_constant(CNF_CLIENT_PORT, confparm_key);
amglue_add_constant(CNF_GNUTAR_LIST_DIR, confparm_key);
amglue_add_constant(CNF_AMANDATES, confparm_key);
amglue_add_constant(CNF_MAILER, confparm_key);
amglue_add_constant(CNF_MAILTO, confparm_key);
amglue_add_constant(CNF_DUMPUSER, confparm_key);
amglue_add_constant(CNF_TAPEDEV, confparm_key);
amglue_add_constant(CNF_DEVICE_PROPERTY, confparm_key);
amglue_add_constant(CNF_PROPERTY, confparm_key);
amglue_add_constant(CNF_CHANGERDEV, confparm_key);
amglue_add_constant(CNF_CHANGERFILE, confparm_key);
amglue_add_constant(CNF_LABELSTR, confparm_key);
amglue_add_constant(CNF_TAPELIST, confparm_key);
amglue_add_constant(CNF_DISKFILE, confparm_key);
amglue_add_constant(CNF_INFOFILE, confparm_key);
amglue_add_constant(CNF_LOGDIR, confparm_key);
amglue_add_constant(CNF_INDEXDIR, confparm_key);
amglue_add_constant(CNF_TAPETYPE, confparm_key);
amglue_add_constant(CNF_DUMPCYCLE, confparm_key);
amglue_add_constant(CNF_RUNSPERCYCLE, confparm_key);
amglue_add_constant(CNF_TAPECYCLE, confparm_key);
amglue_add_constant(CNF_NETUSAGE, confparm_key);
amglue_add_constant(CNF_INPARALLEL, confparm_key);
amglue_add_constant(CNF_DUMPORDER, confparm_key);
amglue_add_constant(CNF_BUMPPERCENT, confparm_key);
amglue_add_constant(CNF_BUMPSIZE, confparm_key);
amglue_add_constant(CNF_BUMPMULT, confparm_key);
amglue_add_constant(CNF_BUMPDAYS, confparm_key);
amglue_add_constant(CNF_TPCHANGER, confparm_key);
amglue_add_constant(CNF_RUNTAPES, confparm_key);
amglue_add_constant(CNF_MAXDUMPS, confparm_key);
amglue_add_constant(CNF_ETIMEOUT, confparm_key);
amglue_add_constant(CNF_DTIMEOUT, confparm_key);
amglue_add_constant(CNF_CTIMEOUT, confparm_key);
amglue_add_constant(CNF_TAPEBUFS, confparm_key);
amglue_add_constant(CNF_DEVICE_OUTPUT_BUFFER_SIZE, confparm_key);
amglue_add_constant(CNF_PRINTER, confparm_key);
amglue_add_constant(CNF_AUTOFLUSH, confparm_key);
amglue_add_constant(CNF_RESERVE, confparm_key);
amglue_add_constant(CNF_MAXDUMPSIZE, confparm_key);
amglue_add_constant(CNF_COLUMNSPEC, confparm_key);
amglue_add_constant(CNF_AMRECOVER_DO_FSF, confparm_key);
amglue_add_constant(CNF_AMRECOVER_CHECK_LABEL, confparm_key);
amglue_add_constant(CNF_AMRECOVER_CHANGER, confparm_key);
amglue_add_constant(CNF_TAPERALGO, confparm_key);
amglue_add_constant(CNF_FLUSH_THRESHOLD_DUMPED, confparm_key);
amglue_add_constant(CNF_FLUSH_THRESHOLD_SCHEDULED, confparm_key);
amglue_add_constant(CNF_TAPERFLUSH, confparm_key);
amglue_add_constant(CNF_DISPLAYUNIT, confparm_key);
amglue_add_constant(CNF_KRB5KEYTAB, confparm_key);
amglue_add_constant(CNF_KRB5PRINCIPAL, confparm_key);
amglue_add_constant(CNF_LABEL_NEW_TAPES, confparm_key);
amglue_add_constant(CNF_USETIMESTAMPS, confparm_key);
amglue_add_constant(CNF_REP_TRIES, confparm_key);
amglue_add_constant(CNF_CONNECT_TRIES, confparm_key);
amglue_add_constant(CNF_REQ_TRIES, confparm_key);
amglue_add_constant(CNF_DEBUG_AMANDAD, confparm_key);
amglue_add_constant(CNF_DEBUG_AMIDXTAPED, confparm_key);
amglue_add_constant(CNF_DEBUG_AMINDEXD, confparm_key);
amglue_add_constant(CNF_DEBUG_AMRECOVER, confparm_key);
amglue_add_constant(CNF_DEBUG_AUTH, confparm_key);
amglue_add_constant(CNF_DEBUG_EVENT, confparm_key);
amglue_add_constant(CNF_DEBUG_HOLDING, confparm_key);
amglue_add_constant(CNF_DEBUG_PROTOCOL, confparm_key);
amglue_add_constant(CNF_DEBUG_PLANNER, confparm_key);
amglue_add_constant(CNF_DEBUG_DRIVER, confparm_key);
amglue_add_constant(CNF_DEBUG_DUMPER, confparm_key);
amglue_add_constant(CNF_DEBUG_CHUNKER, confparm_key);
amglue_add_constant(CNF_DEBUG_TAPER, confparm_key);
amglue_add_constant(CNF_DEBUG_SELFCHECK, confparm_key);
amglue_add_constant(CNF_DEBUG_SENDSIZE, confparm_key);
amglue_add_constant(CNF_DEBUG_SENDBACKUP, confparm_key);
amglue_add_constant(CNF_RESERVED_UDP_PORT, confparm_key);
amglue_add_constant(CNF_RESERVED_TCP_PORT, confparm_key);
amglue_add_constant(CNF_UNRESERVED_TCP_PORT, confparm_key);
amglue_add_constant(CNF_HOLDINGDISK, confparm_key);
amglue_add_constant(CNF_NDMP_PROXY_PORT, confparm_key);
amglue_add_constant(CNF_NDMP_PROXY_DEBUG_FILE, confparm_key);
amglue_add_constant(CNF_NDMP_PROXY_DEBUG_LEVEL, confparm_key);
amglue_copy_to_tag(confparm_key, getconf);

amglue_add_enum_tag_fns(tapetype_key);
amglue_add_constant(TAPETYPE_COMMENT, tapetype_key);
amglue_add_constant(TAPETYPE_LBL_TEMPL, tapetype_key);
amglue_add_constant(TAPETYPE_BLOCKSIZE, tapetype_key);
amglue_add_constant(TAPETYPE_READBLOCKSIZE, tapetype_key);
amglue_add_constant(TAPETYPE_LENGTH, tapetype_key);
amglue_add_constant(TAPETYPE_FILEMARK, tapetype_key);
amglue_add_constant(TAPETYPE_SPEED, tapetype_key);
amglue_add_constant(TAPETYPE_FILE_PAD, tapetype_key);
amglue_copy_to_tag(tapetype_key, getconf);

amglue_add_enum_tag_fns(dumptype_key);
amglue_add_constant(DUMPTYPE_COMMENT, dumptype_key);
amglue_add_constant(DUMPTYPE_PROGRAM, dumptype_key);
amglue_add_constant(DUMPTYPE_SRVCOMPPROG, dumptype_key);
amglue_add_constant(DUMPTYPE_CLNTCOMPPROG, dumptype_key);
amglue_add_constant(DUMPTYPE_SRV_ENCRYPT, dumptype_key);
amglue_add_constant(DUMPTYPE_CLNT_ENCRYPT, dumptype_key);
amglue_add_constant(DUMPTYPE_AMANDAD_PATH, dumptype_key);
amglue_add_constant(DUMPTYPE_CLIENT_USERNAME, dumptype_key);
amglue_add_constant(DUMPTYPE_CLIENT_PORT, dumptype_key);
amglue_add_constant(DUMPTYPE_SSH_KEYS, dumptype_key);
amglue_add_constant(DUMPTYPE_AUTH, dumptype_key);
amglue_add_constant(DUMPTYPE_EXCLUDE, dumptype_key);
amglue_add_constant(DUMPTYPE_INCLUDE, dumptype_key);
amglue_add_constant(DUMPTYPE_PRIORITY, dumptype_key);
amglue_add_constant(DUMPTYPE_DUMPCYCLE, dumptype_key);
amglue_add_constant(DUMPTYPE_MAXDUMPS, dumptype_key);
amglue_add_constant(DUMPTYPE_MAXPROMOTEDAY, dumptype_key);
amglue_add_constant(DUMPTYPE_BUMPPERCENT, dumptype_key);
amglue_add_constant(DUMPTYPE_BUMPSIZE, dumptype_key);
amglue_add_constant(DUMPTYPE_BUMPDAYS, dumptype_key);
amglue_add_constant(DUMPTYPE_BUMPMULT, dumptype_key);
amglue_add_constant(DUMPTYPE_STARTTIME, dumptype_key);
amglue_add_constant(DUMPTYPE_STRATEGY, dumptype_key);
amglue_add_constant(DUMPTYPE_ESTIMATELIST, dumptype_key);
amglue_add_constant(DUMPTYPE_COMPRESS, dumptype_key);
amglue_add_constant(DUMPTYPE_ENCRYPT, dumptype_key);
amglue_add_constant(DUMPTYPE_SRV_DECRYPT_OPT, dumptype_key);
amglue_add_constant(DUMPTYPE_CLNT_DECRYPT_OPT, dumptype_key);
amglue_add_constant(DUMPTYPE_COMPRATE, dumptype_key);
amglue_add_constant(DUMPTYPE_TAPE_SPLITSIZE, dumptype_key);
amglue_add_constant(DUMPTYPE_FALLBACK_SPLITSIZE, dumptype_key);
amglue_add_constant(DUMPTYPE_SPLIT_DISKBUFFER, dumptype_key);
amglue_add_constant(DUMPTYPE_RECORD, dumptype_key);
amglue_add_constant(DUMPTYPE_SKIP_INCR, dumptype_key);
amglue_add_constant(DUMPTYPE_SKIP_FULL, dumptype_key);
amglue_add_constant(DUMPTYPE_HOLDINGDISK, dumptype_key);
amglue_add_constant(DUMPTYPE_KENCRYPT, dumptype_key);
amglue_add_constant(DUMPTYPE_IGNORE, dumptype_key);
amglue_add_constant(DUMPTYPE_INDEX, dumptype_key);
amglue_add_constant(DUMPTYPE_APPLICATION, dumptype_key);
amglue_add_constant(DUMPTYPE_SCRIPTLIST, dumptype_key);
amglue_add_constant(DUMPTYPE_PROPERTY, dumptype_key);
amglue_copy_to_tag(dumptype_key, getconf);

amglue_add_enum_tag_fns(interface_key);
amglue_add_constant(INTER_COMMENT, interface_key);
amglue_add_constant(INTER_MAXUSAGE, interface_key);
amglue_copy_to_tag(interface_key, getconf);

amglue_add_enum_tag_fns(holdingdisk_key);
amglue_add_constant(HOLDING_COMMENT, holdingdisk_key);
amglue_add_constant(HOLDING_DISKDIR, holdingdisk_key);
amglue_add_constant(HOLDING_DISKSIZE, holdingdisk_key);
amglue_add_constant(HOLDING_CHUNKSIZE, holdingdisk_key);
amglue_copy_to_tag(holdingdisk_key, getconf);

amglue_add_enum_tag_fns(application_key);
amglue_add_constant(APPLICATION_COMMENT, application_key);
amglue_add_constant(APPLICATION_PLUGIN, application_key);
amglue_add_constant(APPLICATION_PROPERTY, application_key);
amglue_copy_to_tag(application_key, getconf);

amglue_add_enum_tag_fns(pp_script_key);
amglue_add_constant(PP_SCRIPT_COMMENT, pp_script_key);
amglue_add_constant(PP_SCRIPT_PLUGIN, pp_script_key);
amglue_add_constant(PP_SCRIPT_PROPERTY, pp_script_key);
amglue_add_constant(PP_SCRIPT_EXECUTE_ON, pp_script_key);
amglue_add_constant(PP_SCRIPT_EXECUTE_WHERE, pp_script_key);
amglue_add_constant(PP_SCRIPT_ORDER, pp_script_key);
amglue_copy_to_tag(pp_script_key, getconf);

amglue_add_enum_tag_fns(device_config_key);
amglue_add_constant(DEVICE_CONFIG_COMMENT, device_config_key);
amglue_add_constant(DEVICE_CONFIG_TAPEDEV, device_config_key);
amglue_add_constant(DEVICE_CONFIG_DEVICE_PROPERTY, device_config_key);
amglue_copy_to_tag(device_config_key, getconf);

amglue_add_enum_tag_fns(changer_config_key);
amglue_add_constant(CHANGER_CONFIG_COMMENT, changer_config_key);
amglue_add_constant(CHANGER_CONFIG_TAPEDEV, changer_config_key);
amglue_add_constant(CHANGER_CONFIG_TPCHANGER, changer_config_key);
amglue_add_constant(CHANGER_CONFIG_CHANGERDEV, changer_config_key);
amglue_add_constant(CHANGER_CONFIG_CHANGERFILE, changer_config_key);
amglue_add_constant(CHANGER_CONFIG_PROPERTY, changer_config_key);
amglue_add_constant(CHANGER_CONFIG_DEVICE_PROPERTY, changer_config_key);
amglue_copy_to_tag(changer_config_key, getconf);

/*
 * Various enumerated conftypes
 */

amglue_add_enum_tag_fns(dump_holdingdisk_t);
amglue_add_constant(HOLD_NEVER, dump_holdingdisk_t);
amglue_add_constant(HOLD_AUTO, dump_holdingdisk_t);
amglue_add_constant(HOLD_REQUIRED, dump_holdingdisk_t);
amglue_copy_to_tag(dump_holdingdisk_t, getconf);

amglue_add_enum_tag_fns(comp_t);
amglue_add_constant(COMP_NONE, comp_t);
amglue_add_constant(COMP_FAST, comp_t);
amglue_add_constant(COMP_BEST, comp_t);
amglue_add_constant(COMP_CUST, comp_t);
amglue_add_constant(COMP_SERVER_FAST, comp_t);
amglue_add_constant(COMP_SERVER_BEST, comp_t);
amglue_add_constant(COMP_SERVER_CUST, comp_t);
amglue_copy_to_tag(comp_t, getconf);

amglue_add_enum_tag_fns(encrypt_t);
amglue_add_constant(ENCRYPT_NONE, encrypt_t);
amglue_add_constant(ENCRYPT_CUST, encrypt_t);
amglue_add_constant(ENCRYPT_SERV_CUST, encrypt_t);
amglue_copy_to_tag(encrypt_t, getconf);

amglue_add_enum_tag_fns(strategy_t);
amglue_add_constant(DS_SKIP, strategy_t);
amglue_add_constant(DS_STANDARD, strategy_t);
amglue_add_constant(DS_NOFULL, strategy_t);
amglue_add_constant(DS_NOINC, strategy_t);
amglue_add_constant(DS_4, strategy_t);
amglue_add_constant(DS_5, strategy_t);
amglue_add_constant(DS_HANOI, strategy_t);
amglue_add_constant(DS_INCRONLY, strategy_t);
amglue_copy_to_tag(strategy_t, getconf);

amglue_add_enum_tag_fns(estimate_t);
amglue_add_constant(ES_CLIENT, estimate_t);
amglue_add_constant(ES_SERVER, estimate_t);
amglue_add_constant(ES_CALCSIZE, estimate_t);
amglue_copy_to_tag(estimate_t, getconf);

amglue_add_enum_tag_fns(taperalgo_t);
amglue_add_constant(ALGO_FIRST, taperalgo_t);
amglue_add_constant(ALGO_FIRSTFIT, taperalgo_t);
amglue_add_constant(ALGO_LARGEST, taperalgo_t);
amglue_add_constant(ALGO_LARGESTFIT, taperalgo_t);
amglue_add_constant(ALGO_SMALLEST, taperalgo_t);
amglue_add_constant(ALGO_LAST, taperalgo_t);
amglue_copy_to_tag(taperalgo_t, getconf);

amglue_add_enum_tag_fns(execute_on_t);
amglue_add_constant(EXECUTE_ON_PRE_DLE_AMCHECK, execute_on_t);
amglue_add_constant(EXECUTE_ON_PRE_HOST_AMCHECK, execute_on_t);
amglue_add_constant(EXECUTE_ON_POST_DLE_AMCHECK, execute_on_t);
amglue_add_constant(EXECUTE_ON_POST_HOST_AMCHECK, execute_on_t);
amglue_add_constant(EXECUTE_ON_PRE_DLE_ESTIMATE, execute_on_t);
amglue_add_constant(EXECUTE_ON_PRE_HOST_ESTIMATE, execute_on_t);
amglue_add_constant(EXECUTE_ON_POST_DLE_ESTIMATE, execute_on_t);
amglue_add_constant(EXECUTE_ON_POST_HOST_ESTIMATE, execute_on_t);
amglue_add_constant(EXECUTE_ON_PRE_DLE_BACKUP, execute_on_t);
amglue_add_constant(EXECUTE_ON_PRE_HOST_BACKUP, execute_on_t);
amglue_add_constant(EXECUTE_ON_POST_DLE_BACKUP, execute_on_t);
amglue_add_constant(EXECUTE_ON_POST_HOST_BACKUP, execute_on_t);
amglue_copy_to_tag(execute_on_t, getconf);

amglue_add_enum_tag_fns(send_amreport_on_t);
amglue_add_constant(SEND_AMREPORT_ALL, send_amreport_on_t);
amglue_add_constant(SEND_AMREPORT_STRANGE, send_amreport_on_t);
amglue_add_constant(SEND_AMREPORT_ERROR, send_amreport_on_t);
amglue_add_constant(SEND_AMREPORT_NEVER, send_amreport_on_t);
amglue_copy_to_tag(send_amreport_on_t, getconf);

/*
 * val_t typemaps
 */

/* Typemap to convert a val_t, the union in which config values are
 * stored, to a Perl value of the appropriate type.  This converts:
 *  - CONFTYPE_SIZE, CONFTYPE_INT, CONFTYPE_INT64,
 *    CONFTYPE_BOOLEAN -> IV
 *  - CONFTYPE_REAL -> NV
 *  - CONFTYPE_STR, CONFTYPE_IDENT -> PV
 *  - CONFTYPE_APPLICATION -> PV
 *  - CONFTYPE_TIME -> IV (epoch timestamp)
 *  - CONFTYPE_COMPRESS, CONFTYPE_ENCRYPT, CONFTYPE_ESTIMATELIST, CONFTYPE_STRATEGY,
 *    CONFTYPE_TAPERALGO, CONFTYPE_PRIORITY, CONFTYPE_HOLDING, CONFTYPE_EXECUTE_ON,
 *    CONFTYPE_EXECUTE_WHERE, SEND_AMREPORT_ON -> IV (enums)
 *  - CONFTYPE_RATE -> list of two NVs
 *  - CONFTYPE_INTRANGE -> list of two IVs
 *  - CONFTYPE_EXINCLUDE -> hashref with keys 'list' (listref), 'file' (listref), 
 *    and 'optional' (int)
 *  - CONFTYPE_PROPLIST -> hashref of hashref with keys 'append' (IV), 'priority' (IV),
 *							'values' (listref)
 */
%typemap (out) val_t * {
    if (!$1) {
	$result = &PL_sv_undef;
	argvi++;
    } else {
	switch ($1->type) {
	    case CONFTYPE_RATE: {
		$result= sv_newmortal();
		sv_setnv($result, val_t__rate($1)[0]);
		argvi++;

		$result= sv_newmortal();
		sv_setnv($result, val_t__rate($1)[1]);
		argvi++;
		break;
	    }

	    case CONFTYPE_INTRANGE: {
		$result= sv_newmortal();
		sv_setiv($result, val_t__intrange($1)[0]);
		argvi++;

		$result= sv_newmortal();
		sv_setiv($result, val_t__intrange($1)[1]);
		argvi++;
		break;
	    }

	    case CONFTYPE_EXINCLUDE: {
		/* exincludes are represented in perl as {
		 *	'list' : [ 'list1', 'list2', ..],
		 *	'file' : [ 'file1', 'file2', ..],
		 *	'optional' : 1,
		 * }
		 */
		exinclude_t *ei = &val_t__exinclude($1);
		AV *list_entries = (AV *)sv_2mortal((SV *)newAV());
		AV *file_entries = (AV *)sv_2mortal((SV *)newAV());
		SV *optional = sv_newmortal();
		HV *hv;
		sle_t *iter;

		/* first set up each of the hash values */

		if (ei->sl_list) {
		    for (iter = ei->sl_list->first; iter != NULL; iter = iter->next) {
			av_push(list_entries, newSVpv(iter->name, 0));
		    }
		}

		if(ei->sl_file) {
		    for (iter = ei->sl_file->first; iter != NULL; iter = iter->next) {
			av_push(file_entries, newSVpv(iter->name, 0));
		    }
		}

		sv_setiv(optional, ei->optional);

		/* now build the hash */
		hv = (HV *)sv_2mortal((SV *)newHV());
		
		hv_store(hv, "file", 4, newRV((SV *)file_entries), 0);
		hv_store(hv, "list", 4, newRV((SV *)list_entries), 0);
		hv_store(hv, "optional", 8, optional, 0);
		SvREFCNT_inc(optional);

		$result = sv_2mortal(newRV((SV *)hv));
		argvi++;
		break;
	    }

	    case CONFTYPE_PROPLIST:
		$result = sv_2mortal(g_hash_table_to_hashref_property(val_t__proplist($1)));
		argvi++;
		break;

	    case CONFTYPE_SIZE:
		$result = sv_2mortal(amglue_newSVi64(val_t__size($1)));
		argvi++;
		break;

	    case CONFTYPE_INT64:
		$result = sv_2mortal(amglue_newSVi64(val_t__int64($1)));
		argvi++;
		break;

	    case CONFTYPE_BOOLEAN:	    /* all same as INT.. */
	    case CONFTYPE_COMPRESS:
	    case CONFTYPE_ENCRYPT:
	    case CONFTYPE_STRATEGY:
	    case CONFTYPE_TAPERALGO:
	    case CONFTYPE_PRIORITY:
	    case CONFTYPE_HOLDING:
	    case CONFTYPE_EXECUTE_ON:
	    case CONFTYPE_EXECUTE_WHERE:
	    case CONFTYPE_SEND_AMREPORT_ON:
	    case CONFTYPE_INT:
		$result = sv_2mortal(amglue_newSVi64(val_t__int($1)));
		argvi++;
		break;

           case CONFTYPE_ESTIMATELIST: {
		#AV *elist = (AV *)sv_2mortal((SV *)newAV());
		AV *elist = newAV();
		estimatelist_t el;
		for (el=val_t__estimatelist($1); el != NULL; el = el->next) {
		    av_push(elist, amglue_newSVi64(GPOINTER_TO_INT(el->data)));
		}
		$result = sv_2mortal(newRV_noinc((SV *)elist));
		argvi++;
		break;
	    }

	    case CONFTYPE_TIME:
		$result = sv_2mortal(amglue_newSVi64(val_t__time($1)));
		argvi++;
		break;

	    case CONFTYPE_REAL:
		$result = sv_newmortal();
		sv_setnv($result, val_t__real($1));
		argvi++;
		break;

	    case CONFTYPE_IDENT:	    /* same as STRING */
	    case CONFTYPE_STR:
	    case CONFTYPE_APPLICATION:
		$result = sv_newmortal();
		sv_setpv($result, val_t__str($1));
		argvi++;
		break;

	    case CONFTYPE_IDENTLIST: {
		AV *ilist = newAV();

		identlist_t il;
		for (il=val_t__identlist($1); il != NULL; il = il->next) {
		    av_push(ilist, newSVpv((char *)il->data, 0));
		}

		$result = sv_2mortal(newRV_noinc((SV *)ilist));
		argvi++;
		break;
	    }

	    /* No match yet -> not one of the "complex" types */
	    default:
		SWIG_exception(SWIG_TypeError, "Unknown val_t conftype");
		break;
	}
    }
}

/* Typemap for the return value of getconf_list; this assumes that
 * the GSList contains strings, and that it should be freed; both
 * are true for getconf_list.
 */
%typemap (out) GSList * {
    GSList *it = $1;

    while (it) {
	$result = sv_2mortal(newSVpv(it->data, 0));
	argvi++;
	it = it->next;
    }

    g_slist_free($1);
}

/* typedef and typemap for getconf_byname_strs, which is like getconf_byname, 
 * but converts the result with val_t_dispaly_strs
 */
%typemap (out) val_t_strs {
    char **it = $1;

    while (it && *it) {
	$result = sv_2mortal(newSVpv(*it, 0));
	argvi++;
	it++;
    }
    g_strfreev($1);
}

val_t *getconf(confparm_key key);
gboolean getconf_seen(confparm_key key);
val_t *getconf_byname(char *key);
GSList *getconf_list(char *listname);
%inline %{
typedef char **val_t_strs;
val_t_strs getconf_byname_strs(char *key, int str_needs_quotes) {
    val_t *val = getconf_byname(key);
    if (!val) return NULL;
    return val_t_display_strs(val, str_needs_quotes);
}
%}

amglue_export_tag(getconf,
    getconf getconf_seen 
    getconf_byname getconf_byname_strs
    getconf_list
);

tapetype_t *lookup_tapetype(char *identifier);
val_t *tapetype_getconf(tapetype_t *ttyp, tapetype_key key);
char *tapetype_name(tapetype_t *ttyp);
gboolean tapetype_seen(tapetype_t *ttyp, tapetype_key key);
amglue_export_tag(getconf,
    lookup_tapetype tapetype_getconf tapetype_name
    tapetype_seen tapetype_seen
);

dumptype_t *lookup_dumptype(char *identifier);
val_t *dumptype_getconf(dumptype_t *dtyp, dumptype_key key);
char *dumptype_name(dumptype_t *dtyp);
gboolean dumptype_seen(dumptype_t *dtyp, dumptype_key key);
amglue_export_tag(getconf,
    lookup_dumptype dumptype_getconf dumptype_name
    dumptype_seen dumptype_seen
);

interface_t *lookup_interface(char *identifier);
val_t *interface_getconf(interface_t *iface, interface_key key);
char *interface_name(interface_t *iface);
gboolean interface_seen(interface_t *iface, interface_key key);
amglue_export_tag(getconf,
    lookup_interface interface_getconf interface_name
    interface_seen interface_seen
);

holdingdisk_t *lookup_holdingdisk(char *identifier);
GSList *getconf_holdingdisks(void);
val_t *holdingdisk_getconf(holdingdisk_t *hdisk, holdingdisk_key key);
char *holdingdisk_name(holdingdisk_t *hdisk);
gboolean holdingdisk_seen(holdingdisk_t *hdisk, holdingdisk_key key);
amglue_export_tag(getconf,
    lookup_holdingdisk holdingdisk_getconf holdingdisk_name
    getconf_holdingdisks
    holdingdisk_seen holdingdisk_seen
);

application_t *lookup_application(char *identifier);
val_t *application_getconf(application_t *app, application_key key);
char *application_name(application_t *app);
gboolean application_seen(application_t *app, application_key key);
amglue_export_tag(getconf,
    lookup_application application_getconf application_name
    application_seen application_seen
);

pp_script_t *lookup_pp_script(char *identifier);
val_t *pp_script_getconf(pp_script_t *pps, pp_script_key key);
char *pp_script_name(pp_script_t *pps);
gboolean pp_script_seen(pp_script_t *app, pp_script_key key);
amglue_export_tag(getconf,
    lookup_pp_script pp_script_getconf pp_script_name
    pp_script_seen pp_script_seen
);

device_config_t *lookup_device_config(char *identifier);
val_t *device_config_getconf(device_config_t *pps, device_config_key key);
char *device_config_name(device_config_t *pps);
gboolean device_config_seen(device_config_t *app, device_config_key key);
amglue_export_tag(getconf,
    lookup_device_config device_config_getconf device_config_name
    device_config_seen device_config_seen
);

changer_config_t *lookup_changer_config(char *identifier);
val_t *changer_config_getconf(changer_config_t *pps, changer_config_key key);
char *changer_config_name(changer_config_t *pps);
gboolean changer_config_seen(changer_config_t *app, changer_config_key key);
amglue_export_tag(getconf,
    lookup_changer_config changer_config_getconf changer_config_name
    changer_config_seen changer_config_seen
);

%perlcode %{
# only those keys with a value of '1' should be shown; the
# others are deprecated
our %subsection_names = (
    "tapetype" => 1,
    "dumptype" => 1,
    "interface" => 1,
    "holdingdisk" => 1,
    "application" => 1,
    "script" => 1,
    "application-tool" => 0,
    "script-tool" => 0,
    "device" => 1,
    "changer" => 1,
);
%}
amglue_export_tag(getconf, %subsection_names);

long int getconf_unit_divisor(void);

extern int debug_amandad;
extern int debug_amidxtaped;
extern int debug_amindexd;
extern int debug_amrecover;
extern int debug_auth;
extern int debug_event;
extern int debug_holding;
extern int debug_protocol;
extern int debug_planner;
extern int debug_driver;
extern int debug_dumper;
extern int debug_chunker;
extern int debug_taper;
extern int debug_selfcheck;
extern int debug_sendsize;
extern int debug_sendbackup;
amglue_export_tag(getconf,
    getconf_unit_divisor

    $debug_amandad $debug_amidxtaped $debug_amindexd $debug_amrecover
    $debug_auth $debug_event $debug_holding $debug_protocol
    $debug_planner $debug_driver $debug_dumper $debug_chunker
    $debug_taper $debug_selfcheck $debug_sendsize $debug_sendbackup
);

/*
 * Initialization
 */

amglue_add_enum_tag_fns(cfgerr_level_t);
amglue_add_constant(CFGERR_OK, cfgerr_level_t);
amglue_add_constant(CFGERR_WARNINGS, cfgerr_level_t);
amglue_add_constant(CFGERR_ERRORS, cfgerr_level_t);
amglue_copy_to_tag(cfgerr_level_t, init);

amglue_add_flag_tag_fns(config_init_flags);
amglue_add_constant(CONFIG_INIT_EXPLICIT_NAME, config_init_flags);
amglue_add_constant(CONFIG_INIT_USE_CWD, config_init_flags);
amglue_add_constant(CONFIG_INIT_CLIENT, config_init_flags);
amglue_add_constant(CONFIG_INIT_OVERLAY, config_init_flags);
amglue_copy_to_tag(config_init_flags, init);

gboolean config_init(config_init_flags flags,
		     char *arg_config_name);
void config_uninit(void);
char **get_config_options(int first);
char *get_config_name(void);
char *get_config_dir(void);
char *get_config_filename(void);

void config_print_errors(void);
void config_clear_errors(void);

/* Typemap for config_errors' result parameter; this is a GSList of strings
 * which should *not* be freed. */
%typemap(in, numinputs=0) GSList **ERRLIST (GSList *templist) {
   templist = NULL;
   $1 = &templist;
}

%typemap (argout) GSList **ERRLIST {
    GSList *it = *$1;

    while (it) {
	$result = sv_2mortal(newSVpv(it->data, 0));
	argvi++;
	it = it->next;
    }
}
cfgerr_level_t config_errors(GSList **ERRLIST);


config_overrides_t *new_config_overrides(int size_estimate);
void free_config_overrides(config_overrides_t *co);
void add_config_override(config_overrides_t *co,
			 char *key,
			 char *value);
void add_config_override_opt(config_overrides_t *co,
			      char *optarg);
cfgerr_level_t apply_config_overrides(config_overrides_t *co);
cfgerr_level_t reapply_config_overrides(void);

amglue_export_tag(init,
    config_init config_uninit get_config_options
    get_config_name get_config_dir get_config_filename
    config_print_errors config_clear_errors config_errors
    new_config_overrides free_config_overrides add_config_override
    add_config_override_opt apply_config_overrides
    reapply_config_overrides
);

/*
 * Miscellaneous
 */

void dump_configuration(void);
%newobject config_dir_relative;
char *config_dir_relative(char *filename);
char *taperalgo2str(taperalgo_t taperalgo);
gint64 find_multiplier(char * casestr);

amglue_export_ok(
    dump_configuration config_dir_relative taperalgo2str find_multiplier
);
