/*
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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

%module "Amanda::Config"
%include "amglue/amglue.swg"
%include "exception.i"

%include "Amanda/Config.pod"

%{
#include "conffile.h"
%}

/*
 * Documentation Macro
 */

%define constant_pod(DESCRIPTION, FORALL_FN)
%perlcode %{

use Amanda::Config::FoldingHash;

=pod

DESCRIPTION: amglue_constants_list(FORALL_FN)

=cut
%}
%enddef

/*
 * Parameter access
*/

/* All of the CNF_ flags from conffile.h */

#define FOR_ALL_CONFPARM_KEY(APPLY)\
APPLY(CNF_ORG)\
APPLY(CNF_CONF)\
APPLY(CNF_AMDUMP_SERVER)\
APPLY(CNF_INDEX_SERVER)\
APPLY(CNF_TAPE_SERVER)\
APPLY(CNF_AUTH)\
APPLY(CNF_SSH_KEYS)\
APPLY(CNF_AMANDAD_PATH)\
APPLY(CNF_CLIENT_USERNAME)\
APPLY(CNF_CLIENT_PORT)\
APPLY(CNF_GNUTAR_LIST_DIR)\
APPLY(CNF_AMANDATES)\
APPLY(CNF_MAILER)\
APPLY(CNF_MAILTO)\
APPLY(CNF_DUMPUSER)\
APPLY(CNF_TAPEDEV)\
APPLY(CNF_DEVICE_PROPERTY)\
APPLY(CNF_PROPERTY)\
APPLY(CNF_CHANGERDEV)\
APPLY(CNF_CHANGERFILE)\
APPLY(CNF_LABELSTR)\
APPLY(CNF_AUTOLABEL)\
APPLY(CNF_META_AUTOLABEL)\
APPLY(CNF_TAPELIST)\
APPLY(CNF_DISKFILE)\
APPLY(CNF_INFOFILE)\
APPLY(CNF_LOGDIR)\
APPLY(CNF_INDEXDIR)\
APPLY(CNF_TAPETYPE)\
APPLY(CNF_DUMPCYCLE)\
APPLY(CNF_RUNSPERCYCLE)\
APPLY(CNF_TAPECYCLE)\
APPLY(CNF_NETUSAGE)\
APPLY(CNF_INPARALLEL)\
APPLY(CNF_DUMPORDER)\
APPLY(CNF_BUMPPERCENT)\
APPLY(CNF_BUMPSIZE)\
APPLY(CNF_BUMPMULT)\
APPLY(CNF_BUMPDAYS)\
APPLY(CNF_TPCHANGER)\
APPLY(CNF_RUNTAPES)\
APPLY(CNF_MAX_DLE_BY_VOLUME)\
APPLY(CNF_MAXDUMPS)\
APPLY(CNF_ETIMEOUT)\
APPLY(CNF_DTIMEOUT)\
APPLY(CNF_CTIMEOUT)\
APPLY(CNF_DEVICE_OUTPUT_BUFFER_SIZE)\
APPLY(CNF_PRINTER)\
APPLY(CNF_AUTOFLUSH)\
APPLY(CNF_RESERVE)\
APPLY(CNF_MAXDUMPSIZE)\
APPLY(CNF_COLUMNSPEC)\
APPLY(CNF_AMRECOVER_DO_FSF)\
APPLY(CNF_AMRECOVER_CHECK_LABEL)\
APPLY(CNF_AMRECOVER_CHANGER)\
APPLY(CNF_TAPERALGO)\
APPLY(CNF_FLUSH_THRESHOLD_DUMPED)\
APPLY(CNF_FLUSH_THRESHOLD_SCHEDULED)\
APPLY(CNF_TAPERFLUSH)\
APPLY(CNF_DISPLAYUNIT)\
APPLY(CNF_KRB5KEYTAB)\
APPLY(CNF_KRB5PRINCIPAL)\
APPLY(CNF_LABEL_NEW_TAPES)\
APPLY(CNF_USETIMESTAMPS)\
APPLY(CNF_REP_TRIES)\
APPLY(CNF_CONNECT_TRIES)\
APPLY(CNF_REQ_TRIES)\
APPLY(CNF_DEBUG_AMANDAD)\
APPLY(CNF_DEBUG_RECOVERY)\
APPLY(CNF_DEBUG_AMIDXTAPED)\
APPLY(CNF_DEBUG_AMINDEXD)\
APPLY(CNF_DEBUG_AMRECOVER)\
APPLY(CNF_DEBUG_AUTH)\
APPLY(CNF_DEBUG_EVENT)\
APPLY(CNF_DEBUG_HOLDING)\
APPLY(CNF_DEBUG_PROTOCOL)\
APPLY(CNF_DEBUG_PLANNER)\
APPLY(CNF_DEBUG_DRIVER)\
APPLY(CNF_DEBUG_DUMPER)\
APPLY(CNF_DEBUG_CHUNKER)\
APPLY(CNF_DEBUG_TAPER)\
APPLY(CNF_DEBUG_SELFCHECK)\
APPLY(CNF_DEBUG_SENDSIZE)\
APPLY(CNF_DEBUG_SENDBACKUP)\
APPLY(CNF_RESERVED_UDP_PORT)\
APPLY(CNF_RESERVED_TCP_PORT)\
APPLY(CNF_UNRESERVED_TCP_PORT)\
APPLY(CNF_HOLDINGDISK)\
APPLY(CNF_SEND_AMREPORT_ON)\
APPLY(CNF_TAPER_PARALLEL_WRITE)\
APPLY(CNF_RECOVERY_LIMIT) \
APPLY(CNF_INTERACTIVITY) \
APPLY(CNF_TAPERSCAN) \
APPLY(CNF_EJECT_VOLUME) \
APPLY(CNF_TMPDIR)

amglue_add_enum_tag_fns(confparm_key);
amglue_add_constants(FOR_ALL_CONFPARM_KEY, confparm_key);
amglue_copy_to_tag(confparm_key, getconf);
constant_pod(Global Parameters, FOR_ALL_CONFPARM_KEY)

#define FOR_ALL_TAPETYPE_KEY(APPLY)\
APPLY(TAPETYPE_COMMENT)\
APPLY(TAPETYPE_LBL_TEMPL)\
APPLY(TAPETYPE_BLOCKSIZE)\
APPLY(TAPETYPE_READBLOCKSIZE)\
APPLY(TAPETYPE_LENGTH)\
APPLY(TAPETYPE_FILEMARK)\
APPLY(TAPETYPE_SPEED)\
APPLY(TAPETYPE_PART_SIZE)\
APPLY(TAPETYPE_PART_CACHE_TYPE)\
APPLY(TAPETYPE_PART_CACHE_DIR)\
APPLY(TAPETYPE_PART_CACHE_MAX_SIZE)

amglue_add_enum_tag_fns(tapetype_key);
amglue_add_constants(FOR_ALL_TAPETYPE_KEY, tapetype_key);
amglue_copy_to_tag(tapetype_key, getconf);
constant_pod(Tapetype Parameters, FOR_ALL_TAPETYPE_KEY)

#define FOR_ALL_DUMPTYPE_KEY(APPLY)\
APPLY(DUMPTYPE_COMMENT)\
APPLY(DUMPTYPE_PROGRAM)\
APPLY(DUMPTYPE_SRVCOMPPROG)\
APPLY(DUMPTYPE_CLNTCOMPPROG)\
APPLY(DUMPTYPE_SRV_ENCRYPT)\
APPLY(DUMPTYPE_CLNT_ENCRYPT)\
APPLY(DUMPTYPE_AMANDAD_PATH)\
APPLY(DUMPTYPE_CLIENT_USERNAME)\
APPLY(DUMPTYPE_CLIENT_PORT)\
APPLY(DUMPTYPE_SSH_KEYS)\
APPLY(DUMPTYPE_AUTH)\
APPLY(DUMPTYPE_EXCLUDE)\
APPLY(DUMPTYPE_INCLUDE)\
APPLY(DUMPTYPE_PRIORITY)\
APPLY(DUMPTYPE_DUMPCYCLE)\
APPLY(DUMPTYPE_MAXDUMPS)\
APPLY(DUMPTYPE_MAXPROMOTEDAY)\
APPLY(DUMPTYPE_BUMPPERCENT)\
APPLY(DUMPTYPE_BUMPSIZE)\
APPLY(DUMPTYPE_BUMPDAYS)\
APPLY(DUMPTYPE_BUMPMULT)\
APPLY(DUMPTYPE_STARTTIME)\
APPLY(DUMPTYPE_STRATEGY)\
APPLY(DUMPTYPE_ESTIMATELIST)\
APPLY(DUMPTYPE_COMPRESS)\
APPLY(DUMPTYPE_ENCRYPT)\
APPLY(DUMPTYPE_SRV_DECRYPT_OPT)\
APPLY(DUMPTYPE_CLNT_DECRYPT_OPT)\
APPLY(DUMPTYPE_COMPRATE)\
APPLY(DUMPTYPE_TAPE_SPLITSIZE)\
APPLY(DUMPTYPE_FALLBACK_SPLITSIZE)\
APPLY(DUMPTYPE_SPLIT_DISKBUFFER)\
APPLY(DUMPTYPE_RECORD)\
APPLY(DUMPTYPE_SKIP_INCR)\
APPLY(DUMPTYPE_SKIP_FULL)\
APPLY(DUMPTYPE_HOLDINGDISK)\
APPLY(DUMPTYPE_KENCRYPT)\
APPLY(DUMPTYPE_IGNORE)\
APPLY(DUMPTYPE_INDEX)\
APPLY(DUMPTYPE_APPLICATION)\
APPLY(DUMPTYPE_SCRIPTLIST)\
APPLY(DUMPTYPE_PROPERTY)\
APPLY(DUMPTYPE_DATA_PATH)\
APPLY(DUMPTYPE_ALLOW_SPLIT)\
APPLY(DUMPTYPE_MAX_WARNINGS)\
APPLY(DUMPTYPE_RECOVERY_LIMIT) \
APPLY(DUMPTYPE_DUMP_LIMIT)

amglue_add_enum_tag_fns(dumptype_key);
amglue_add_constants(FOR_ALL_DUMPTYPE_KEY, dumptype_key);
amglue_copy_to_tag(dumptype_key, getconf);
constant_pod(Dumptype Parameters, FOR_ALL_DUMPTYPE_KEY)

#define FOR_ALL_INTERFACE_KEY(APPLY)\
APPLY(INTER_COMMENT)\
APPLY(INTER_MAXUSAGE)

amglue_add_enum_tag_fns(interface_key);
amglue_add_constants(FOR_ALL_INTERFACE_KEY, interface_key);
amglue_copy_to_tag(interface_key, getconf);
constant_pod(Dumptype Parameters, FOR_ALL_INTERFACE_KEY)

#define FOR_ALL_HOLDINGDISK_KEY(APPLY)\
APPLY(HOLDING_COMMENT)\
APPLY(HOLDING_DISKDIR)\
APPLY(HOLDING_DISKSIZE)\
APPLY(HOLDING_CHUNKSIZE)

amglue_add_enum_tag_fns(holdingdisk_key);
amglue_add_constants(FOR_ALL_HOLDINGDISK_KEY, holdingdisk_key);
amglue_copy_to_tag(holdingdisk_key, getconf);
constant_pod(Holdingdisk Parameters, FOR_ALL_HOLDINGDISK_KEY)

#define FOR_ALL_APPLICATION_KEY(APPLY)\
APPLY(APPLICATION_COMMENT)\
APPLY(APPLICATION_PLUGIN)\
APPLY(APPLICATION_PROPERTY) \
APPLY(APPLICATION_CLIENT_NAME)

amglue_add_enum_tag_fns(application_key);
amglue_add_constants(FOR_ALL_APPLICATION_KEY, application_key);
amglue_copy_to_tag(application_key, getconf);
constant_pod(Application Parameters, FOR_ALL_APPLICATION_KEY)

#define FOR_ALL_PP_SCRIPT_KEY(APPLY)\
APPLY(PP_SCRIPT_COMMENT)\
APPLY(PP_SCRIPT_PLUGIN)\
APPLY(PP_SCRIPT_PROPERTY)\
APPLY(PP_SCRIPT_EXECUTE_ON)\
APPLY(PP_SCRIPT_EXECUTE_WHERE)\
APPLY(PP_SCRIPT_ORDER)\
APPLY(PP_SCRIPT_SINGLE_EXECUTION)\
APPLY(PP_SCRIPT_CLIENT_NAME)

amglue_add_enum_tag_fns(pp_script_key);
amglue_add_constants(FOR_ALL_PP_SCRIPT_KEY, pp_script_key);
amglue_copy_to_tag(pp_script_key, getconf);
constant_pod(Pre/Post-Script Parameters, FOR_ALL_PP_SCRIPT_KEY)

#define FOR_ALL_DEVICE_CONFIG_KEY(APPLY)\
APPLY(DEVICE_CONFIG_COMMENT)\
APPLY(DEVICE_CONFIG_TAPEDEV)\
APPLY(DEVICE_CONFIG_DEVICE_PROPERTY)

amglue_add_enum_tag_fns(device_config_key);
amglue_add_constants(FOR_ALL_DEVICE_CONFIG_KEY, device_config_key);
amglue_copy_to_tag(device_config_key, getconf);
constant_pod(Pre/Post-Script Parameters, FOR_ALL_DEVICE_CONFIG_KEY)

#define FOR_ALL_CHANGER_CONFIG_KEY(APPLY)\
APPLY(CHANGER_CONFIG_COMMENT)\
APPLY(CHANGER_CONFIG_TAPEDEV)\
APPLY(CHANGER_CONFIG_TPCHANGER)\
APPLY(CHANGER_CONFIG_CHANGERDEV)\
APPLY(CHANGER_CONFIG_CHANGERFILE)\
APPLY(CHANGER_CONFIG_PROPERTY)\
APPLY(CHANGER_CONFIG_DEVICE_PROPERTY)

amglue_add_enum_tag_fns(changer_config_key);
amglue_add_constants(FOR_ALL_CHANGER_CONFIG_KEY, changer_config_key);
amglue_copy_to_tag(changer_config_key, getconf);
constant_pod(Pre/Post-Script Parameters, FOR_ALL_CHANGER_CONFIG_KEY)

#define FOR_ALL_INTERACTIVITY_KEY(APPLY)\
APPLY(INTERACTIVITY_COMMENT)\
APPLY(INTERACTIVITY_PLUGIN)\
APPLY(INTERACTIVITY_PROPERTY)

amglue_add_enum_tag_fns(interactivity_key);
amglue_add_constants(FOR_ALL_INTERACTIVITY_KEY, interactivity_key);
amglue_copy_to_tag(interactivity_key, getconf);
constant_pod(Interactivity Parameters, FOR_ALL_INTERACTIVITY_KEY)

#define FOR_ALL_TAPERSCAN_KEY(APPLY)\
APPLY(TAPERSCAN_COMMENT)\
APPLY(TAPERSCAN_PLUGIN)\
APPLY(TAPERSCAN_PROPERTY)

amglue_add_enum_tag_fns(taperscan_key);
amglue_add_constants(FOR_ALL_TAPERSCAN_KEY, taperscan_key);
amglue_copy_to_tag(taperscan_key, getconf);
constant_pod(Taperscan Parameters, FOR_ALL_TAPERSCAN_KEY)

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

amglue_add_enum_tag_fns(autolabel_enum_t);
amglue_add_constant(AL_OTHER_CONFIG, autolabel_enum_t);
amglue_add_constant(AL_NON_AMANDA, autolabel_enum_t);
amglue_add_constant(AL_VOLUME_ERROR, autolabel_enum_t);
amglue_add_constant(AL_EMPTY, autolabel_enum_t);
amglue_copy_to_tag(autolabel_enum_t, getconf);

amglue_add_enum_tag_fns(taperalgo_t);
amglue_add_constant(ALGO_FIRST, taperalgo_t);
amglue_add_constant(ALGO_FIRSTFIT, taperalgo_t);
amglue_add_constant(ALGO_LARGEST, taperalgo_t);
amglue_add_constant(ALGO_LARGESTFIT, taperalgo_t);
amglue_add_constant(ALGO_SMALLEST, taperalgo_t);
amglue_add_constant(ALGO_LAST, taperalgo_t);
amglue_copy_to_tag(taperalgo_t, getconf);

amglue_add_enum_tag_fns(execute_on_t);
amglue_add_constant(EXECUTE_ON_PRE_AMCHECK, execute_on_t);
amglue_add_constant(EXECUTE_ON_PRE_DLE_AMCHECK, execute_on_t);
amglue_add_constant(EXECUTE_ON_PRE_HOST_AMCHECK, execute_on_t);
amglue_add_constant(EXECUTE_ON_POST_AMCHECK, execute_on_t);
amglue_add_constant(EXECUTE_ON_POST_DLE_AMCHECK, execute_on_t);
amglue_add_constant(EXECUTE_ON_POST_HOST_AMCHECK, execute_on_t);
amglue_add_constant(EXECUTE_ON_PRE_ESTIMATE, execute_on_t);
amglue_add_constant(EXECUTE_ON_PRE_DLE_ESTIMATE, execute_on_t);
amglue_add_constant(EXECUTE_ON_PRE_HOST_ESTIMATE, execute_on_t);
amglue_add_constant(EXECUTE_ON_POST_ESTIMATE, execute_on_t);
amglue_add_constant(EXECUTE_ON_POST_DLE_ESTIMATE, execute_on_t);
amglue_add_constant(EXECUTE_ON_POST_HOST_ESTIMATE, execute_on_t);
amglue_add_constant(EXECUTE_ON_PRE_BACKUP, execute_on_t);
amglue_add_constant(EXECUTE_ON_PRE_DLE_BACKUP, execute_on_t);
amglue_add_constant(EXECUTE_ON_PRE_HOST_BACKUP, execute_on_t);
amglue_add_constant(EXECUTE_ON_POST_BACKUP, execute_on_t);
amglue_add_constant(EXECUTE_ON_POST_DLE_BACKUP, execute_on_t);
amglue_add_constant(EXECUTE_ON_POST_HOST_BACKUP, execute_on_t);
amglue_copy_to_tag(execute_on_t, getconf);

amglue_add_enum_tag_fns(send_amreport_on_t);
amglue_add_constant(SEND_AMREPORT_ALL, send_amreport_on_t);
amglue_add_constant(SEND_AMREPORT_STRANGE, send_amreport_on_t);
amglue_add_constant(SEND_AMREPORT_ERROR, send_amreport_on_t);
amglue_add_constant(SEND_AMREPORT_NEVER, send_amreport_on_t);
amglue_copy_to_tag(send_amreport_on_t, getconf);

amglue_add_enum_tag_fns(data_path_t);
amglue_add_constant(DATA_PATH_AMANDA, data_path_t);
amglue_add_constant(DATA_PATH_DIRECTTCP, data_path_t);
amglue_copy_to_tag(data_path_t, getconf);

amglue_add_enum_tag_fns(part_cache_type_t);
amglue_add_constant(PART_CACHE_TYPE_NONE, part_cache_type_t);
amglue_add_constant(PART_CACHE_TYPE_DISK, part_cache_type_t);
amglue_add_constant(PART_CACHE_TYPE_MEMORY, part_cache_type_t);
amglue_copy_to_tag(part_cache_type_t, getconf);

/*
 * val_t typemaps
 */

/* Typemap to convert a val_t to an SV, using a static function since it's huge.  This converts:
 *  - CONFTYPE_SIZE, CONFTYPE_INT, CONFTYPE_INT64, CONFTYPE_NO_YES_ALL,
 *    CONFTYPE_BOOLEAN -> IV
 *  - CONFTYPE_REAL -> NV
 *  - CONFTYPE_STR, CONFTYPE_IDENT -> PV
 *  - CONFTYPE_APPLICATION -> PV
 *  - CONFTYPE_TIME -> IV (epoch timestamp)
 *  - CONFTYPE_COMPRESS, CONFTYPE_ENCRYPT, CONFTYPE_ESTIMATELIST, CONFTYPE_STRATEGY,
 *    CONFTYPE_TAPERALGO, CONFTYPE_PRIORITY, CONFTYPE_HOLDING, CONFTYPE_EXECUTE_ON,
 *    CONFTYPE_EXECUTE_WHERE, CONFTYPE_SEND_AMREPORT_ON,
 *    CONFTYPE_DATA_PATH, CONFTYPE_PART_CACHE_TYPE -> IV (enums)
 *  - CONFTYPE_RATE -> list of two NVs
 *  - CONFTYPE_INTRANGE -> list of two IVs
 *  - CONFTYPE_EXINCLUDE -> hashref with keys 'list' (listref), 'file' (listref),
 *    and 'optional' (int)
 *  - CONFTYPE_PROPLIST -> hashref of hashref with keys 'append' (IV), 'priority' (IV),
 *							'values' (listref)
 *  - CONFTYPE_HOST_LIMIT -> listref of match expressions;
 *           "SAMEHOST-SAMEHOST-SAMEHOST" in list is taken to mean 'same-host'
 *           "SERVER-SERVER-SERVER" in list is taken to mean 'server'
 */

%perlcode %{
$LIMIT_SAMEHOST="SAMEHOST-SAMEHOST-SAMEHOST";
$LIMIT_SERVER="SERVER-SERVER-SERVER";
%}

%{
static int
val_t_to_sv(val_t *val, SV **results) {
    if (!val) {
	results[0] = &PL_sv_undef;
	return 1;
    } else {
	switch (val->type) {
	    case CONFTYPE_RATE: {
		results[0] = sv_newmortal();
		sv_setnv(results[0], val_t__rate(val)[0]);

		results[1] = sv_newmortal();
		sv_setnv(results[1], val_t__rate(val)[1]);
		return 2;
	    }

	    case CONFTYPE_INTRANGE: {
		results[0] = sv_newmortal();
		sv_setiv(results[0], val_t__intrange(val)[0]);

		results[1] = sv_newmortal();
		sv_setiv(results[1], val_t__intrange(val)[1]);
		return 2;
	    }

	    case CONFTYPE_EXINCLUDE: {
		/* exincludes are represented in perl as {
		 *	'list' : [ 'list1', 'list2', ..],
		 *	'file' : [ 'file1', 'file2', ..],
		 *	'optional' : 1,
		 * }
		 */
		exinclude_t *ei = &val_t__exinclude(val);
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

		results[0] = sv_2mortal(newRV((SV *)hv));
		return 1;
	    }

	    case CONFTYPE_PROPLIST:
		results[0] = sv_2mortal(g_hash_table_to_hashref_property(val_t__proplist(val)));
		return 1;

	    case CONFTYPE_SIZE:
		results[0] = sv_2mortal(amglue_newSVi64(val_t__size(val)));
		return 1;

	    case CONFTYPE_INT64:
		results[0] = sv_2mortal(amglue_newSVi64(val_t__int64(val)));
		return 1;

	    case CONFTYPE_BOOLEAN:	    /* all same as INT.. */
	    case CONFTYPE_NO_YES_ALL:
	    case CONFTYPE_COMPRESS:
	    case CONFTYPE_ENCRYPT:
	    case CONFTYPE_STRATEGY:
	    case CONFTYPE_TAPERALGO:
	    case CONFTYPE_PRIORITY:
	    case CONFTYPE_HOLDING:
	    case CONFTYPE_EXECUTE_ON:
	    case CONFTYPE_EXECUTE_WHERE:
	    case CONFTYPE_SEND_AMREPORT_ON:
	    case CONFTYPE_DATA_PATH:
	    case CONFTYPE_PART_CACHE_TYPE:
	    case CONFTYPE_INT:
		results[0] = sv_2mortal(amglue_newSVi64(val_t__int(val)));
		return 1;

           case CONFTYPE_ESTIMATELIST: {
		AV *elist = newAV();
		estimatelist_t el;
		for (el=val_t__estimatelist(val); el != NULL; el = el->next) {
		    av_push(elist, newSVuv(GPOINTER_TO_INT(el->data)));
		}
		results[0] = sv_2mortal(newRV_noinc((SV *)elist));
		return 1;
	    }

	    case CONFTYPE_TIME:
		results[0] = sv_2mortal(amglue_newSVi64(val_t__time(val)));
		return 1;

	    case CONFTYPE_REAL:
		results[0] = sv_newmortal();
		sv_setnv(results[0], val_t__real(val));
		return 1;

	    case CONFTYPE_IDENT:	    /* same as STRING */
	    case CONFTYPE_STR:
	    case CONFTYPE_APPLICATION:
		results[0] = sv_newmortal();
		sv_setpv(results[0], val_t__str(val));
		return 1;

	    case CONFTYPE_IDENTLIST: {
		AV *ilist = newAV();

		identlist_t il;
		for (il=val_t__identlist(val); il != NULL; il = il->next) {
		    av_push(ilist, newSVpv((char *)il->data, 0));
		}

		results[0] = sv_2mortal(newRV_noinc((SV *)ilist));
		return 1;
	    }

	    case CONFTYPE_HOST_LIMIT: {
		AV *av;
		GSList *iter;
		host_limit_t *rl = &val_t__host_limit(val);

		av = newAV();
		if (rl->same_host)
		    av_push(av, newSVpv("SAMEHOST-SAMEHOST-SAMEHOST", 0));
		if (rl->server)
		    av_push(av, newSVpv("SERVER-SERVER-SERVER", 0));
		for (iter=rl->match_pats; iter != NULL; iter = iter->next) {
		    av_push(av, newSVpv((char *)iter->data, 0));
		}

		results[0] = sv_2mortal(newRV_noinc((SV *)av));
		return 1;
	    }

	    case CONFTYPE_AUTOLABEL: {
		autolabel_t *autolabel = &val_t__autolabel(val);
		HV *hv;

		/* now build the hash */
		hv = (HV *)sv_2mortal((SV *)newHV());
		hv_store(hv, "template", 8,
			(autolabel->template)? newSVpv(autolabel->template, 0) : newSV(0),
			0);
		hv_store(hv, "other_config", 12,
			(autolabel->autolabel & AL_OTHER_CONFIG)? &PL_sv_yes : &PL_sv_no,
			0);
		hv_store(hv, "non_amanda", 10,
			(autolabel->autolabel & AL_NON_AMANDA)? &PL_sv_yes : &PL_sv_no,
			0);
		hv_store(hv, "volume_error", 12,
			(autolabel->autolabel & AL_VOLUME_ERROR)? &PL_sv_yes : &PL_sv_no,
			0);
		hv_store(hv, "empty", 5,
			(autolabel->autolabel & AL_EMPTY)? &PL_sv_yes : &PL_sv_no,
			0);

		results[0] = sv_2mortal(newRV((SV *)hv));
		return 1;
	    }

	    /* No match yet -> not one of the "complex" types */
	    default:
		SWIG_exception(SWIG_TypeError, "Unknown val_t conftype");
		break;
	}
    }

    return 0;

fail:
    SWIG_croak_null();
}
%}

%typemap (out) val_t * {
    SV *results[3], **iter;
    int nresults;

    /* save the stack, since val_t_to_sv may invoke arbitrary Perl code */
    SP += argvi; PUTBACK;
    nresults = val_t_to_sv($1, results);
    SPAGAIN; SP -= argvi;

    /* add val_t_to_sv's results to the stack */
    for (iter = results; nresults; iter++, nresults--) {
	$result = *iter;
	argvi++;
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

data_path_t data_path_from_string(char *data);

val_t *getconf(confparm_key key);
gboolean getconf_seen(confparm_key key);
int getconf_linenum(confparm_key key);
val_t *getconf_byname(char *key);
GSList *getconf_list(char *listname);
%inline %{
typedef char **val_t_strs;
val_t_strs getconf_byname_strs(char *key, int str_needs_quotes) {
    val_t *val = getconf_byname(key);
    if (!val) return NULL;
    return val_t_display_strs(val, str_needs_quotes, FALSE, FALSE);
}
%}

amglue_export_tag(getconf,
    getconf getconf_seen getconf_linenum
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

interactivity_t *lookup_interactivity(char *identifier);
val_t *interactivity_getconf(interactivity_t *app, interactivity_key key);
char *interactivity_name(interactivity_t *app);
gboolean interactivity_seen(interactivity_t *app, interactivity_key key);
amglue_export_tag(getconf,
    lookup_interactivity interactivity_getconf interactivity_name
    interactivity_seen interactivity_seen
);

taperscan_t *lookup_taperscan(char *identifier);
val_t *taperscan_getconf(taperscan_t *app, taperscan_key key);
char *taperscan_name(taperscan_t *app);
gboolean taperscan_seen(taperscan_t *app, taperscan_key key);
amglue_export_tag(getconf,
    lookup_taperscan taperscan_getconf taperscan_name
    taperscan_seen taperscan_seen
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
extern int debug_recovery;
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

    $debug_amandad $debug_recovery $debug_amidxtaped $debug_amindexd $debug_amrecover
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

cfgerr_level_t config_init(config_init_flags flags,
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
void set_config_overrides(config_overrides_t *co);

amglue_export_tag(init,
    config_init config_uninit get_config_options
    get_config_name get_config_dir get_config_filename
    config_print_errors config_clear_errors config_errors
    new_config_overrides free_config_overrides add_config_override
    add_config_override_opt set_config_overrides
);

/*
 * Miscellaneous
 */

void dump_configuration(gboolean print_default, gboolean print_source);
%newobject config_dir_relative;
char *config_dir_relative(char *filename);
char *taperalgo2str(taperalgo_t taperalgo);
gint64 find_multiplier(char * casestr);

amglue_export_ok(
    dump_configuration config_dir_relative taperalgo2str find_multiplier
);

%rename(C_string_to_boolean) string_to_boolean;
int string_to_boolean(const char *str);
%perlcode %{

sub string_to_boolean {
    my ($str) = @_;
    my $ret = C_string_to_boolean($str);
    return undef unless $ret >= 0;
    return $ret;
}

%}

amglue_export_ok(string_to_boolean);

%newobject amandaify_property_name;
gchar * amandaify_property_name(const gchar *name);
amglue_export_ok(amandaify_property_name);
