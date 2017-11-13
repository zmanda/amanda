/*
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
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
 * Contact information: Carbonite Inc., 756 N Pastoria Ave
 * Sunnyvale, CA 94085, or: http://www.zmanda.com
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

require Amanda::Config::FoldingHash;

=pod

DESCRIPTION: amglue_constants_list(FORALL_FN)

=cut
%}
%enddef

/*
 * Constants
 */

amglue_add_flag_tag_fns(RetentionType);
amglue_add_constant_short(RETENTION_NO, "retention-no", RetentionType);
amglue_add_constant_short(RETENTION_NO_REUSE, "no-reuse", RetentionType);
amglue_add_constant_short(RETENTION_TAPES, "retention-tapes", RetentionType);
amglue_add_constant_short(RETENTION_DAYS, "retention-days", RetentionType);
amglue_add_constant_short(RETENTION_RECOVER, "retention-recover", RetentionType);
amglue_add_constant_short(RETENTION_FULL, "retention-full", RetentionType);
amglue_add_constant_short(RETENTION_CMD_COPY, "command-copy", RetentionType);
amglue_add_constant_short(RETENTION_CMD_FLUSH, "command-flush", RetentionType);
amglue_add_constant_short(RETENTION_CMD_RESTORE, "command-restore", RetentionType);
amglue_add_constant_short(RETENTION_OTHER_CONFIG, "other-config", RetentionType);
amglue_add_constant_short(RETENTION_ALL, "retention-all", RetentionType);
amglue_copy_to_tag(RetentionType, constants)

%perlcode %{
sub get_retention_name {
    my $retention_type = shift;

    if ($retention_type == $RETENTION_NO) {
	return "retention-no";
    } elsif ($retention_type == $RETENTION_NO_REUSE) {
	return "no-reuse";
    } elsif ($retention_type == $RETENTION_TAPES) {
	return "retention-tapes";
    } elsif ($retention_type == $RETENTION_DAYS) {
	return "retention-days";
    } elsif ($retention_type == $RETENTION_RECOVER) {
	return "retention-recover";
    } elsif ($retention_type == $RETENTION_FULL) {
	return "retention-full";
    } elsif ($retention_type == $RETENTION_CMD_COPY) {
	return "command-copy";
    } elsif ($retention_type == $RETENTION_CMD_FLUSH) {
	return "command-hlush";
    } elsif ($retention_type == $RETENTION_CMD_RESTORE) {
	return "command-restore";
    } elsif ($retention_type == $RETENTION_OTHER_CONFIG) {
	return "other config";
    } else {
	return "unknown retention $retention_type";
    }
}
%}

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
APPLY(CNF_TMPDIR) \
APPLY(CNF_REPORT_USE_MEDIA) \
APPLY(CNF_REPORT_NEXT_MEDIA) \
APPLY(CNF_REPORT_FORMAT) \
APPLY(CNF_STORAGE) \
APPLY(CNF_VAULT_STORAGE) \
APPLY(CNF_CMDFILE) \
APPLY(CNF_REST_API_PORT) \
APPLY(CNF_REST_SSL_CERT) \
APPLY(CNF_REST_SSL_KEY) \
APPLY(CNF_COMPRESS_INDEX) \
APPLY(CNF_SORT_INDEX) \
APPLY(CNF_SSL_DIR) \
APPLY(CNF_SSL_CHECK_FINGERPRINT) \
APPLY(CNF_SSL_CERT_FILE) \
APPLY(CNF_SSL_KEY_FILE) \
APPLY(CNF_SSL_CA_CERT_FILE) \
APPLY(CNF_SSL_CIPHER_LIST) \
APPLY(CNF_SSL_CHECK_HOST) \
APPLY(CNF_SSL_CHECK_CERTIFICATE_HOST) \
APPLY(CNF_HOSTNAME) \
APPLY(CNF_CATALOG)

amglue_add_enum_tag_fns(confparm_key);
amglue_add_constants(FOR_ALL_CONFPARM_KEY, confparm_key);
amglue_copy_to_tag(confparm_key, getconf);
amglue_add_enum_list(confparm_key);
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
amglue_add_enum_list(tapetype_key);
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
APPLY(DUMPTYPE_DUMP_LIMIT) \
APPLY(DUMPTYPE_RETRY_DUMP) \
APPLY(DUMPTYPE_TAG)

amglue_add_enum_tag_fns(dumptype_key);
amglue_add_constants(FOR_ALL_DUMPTYPE_KEY, dumptype_key);
amglue_copy_to_tag(dumptype_key, getconf);
amglue_add_enum_list(dumptype_key);
constant_pod(Dumptype Parameters, FOR_ALL_DUMPTYPE_KEY)

#define FOR_ALL_INTERFACE_KEY(APPLY)\
APPLY(INTER_COMMENT)\
APPLY(INTER_MAXUSAGE)

amglue_add_enum_tag_fns(interface_key);
amglue_add_constants(FOR_ALL_INTERFACE_KEY, interface_key);
amglue_copy_to_tag(interface_key, getconf);
amglue_add_enum_list(interface_key);
constant_pod(Interface Parameters, FOR_ALL_INTERFACE_KEY)

#define FOR_ALL_HOLDINGDISK_KEY(APPLY)\
APPLY(HOLDING_COMMENT)\
APPLY(HOLDING_DISKDIR)\
APPLY(HOLDING_DISKSIZE)\
APPLY(HOLDING_CHUNKSIZE)

amglue_add_enum_tag_fns(holdingdisk_key);
amglue_add_constants(FOR_ALL_HOLDINGDISK_KEY, holdingdisk_key);
amglue_copy_to_tag(holdingdisk_key, getconf);
amglue_add_enum_list(holdingdisk_key);
constant_pod(Holdingdisk Parameters, FOR_ALL_HOLDINGDISK_KEY)

#define FOR_ALL_APPLICATION_KEY(APPLY)\
APPLY(APPLICATION_COMMENT)\
APPLY(APPLICATION_PLUGIN)\
APPLY(APPLICATION_PROPERTY) \
APPLY(APPLICATION_CLIENT_NAME)

amglue_add_enum_tag_fns(application_key);
amglue_add_constants(FOR_ALL_APPLICATION_KEY, application_key);
amglue_copy_to_tag(application_key, getconf);
amglue_add_enum_list(application_key);
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
amglue_add_enum_list(pp_script_key);
constant_pod(Pre/Post-Script Parameters, FOR_ALL_PP_SCRIPT_KEY)

#define FOR_ALL_DEVICE_CONFIG_KEY(APPLY)\
APPLY(DEVICE_CONFIG_COMMENT)\
APPLY(DEVICE_CONFIG_TAPEDEV)\
APPLY(DEVICE_CONFIG_DEVICE_PROPERTY)

amglue_add_enum_tag_fns(device_config_key);
amglue_add_constants(FOR_ALL_DEVICE_CONFIG_KEY, device_config_key);
amglue_copy_to_tag(device_config_key, getconf);
amglue_add_enum_list(device_config_key);
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
amglue_add_enum_list(changer_config_key);
constant_pod(Pre/Post-Script Parameters, FOR_ALL_CHANGER_CONFIG_KEY)

#define FOR_ALL_INTERACTIVITY_KEY(APPLY)\
APPLY(INTERACTIVITY_COMMENT)\
APPLY(INTERACTIVITY_PLUGIN)\
APPLY(INTERACTIVITY_PROPERTY)

amglue_add_enum_tag_fns(interactivity_key);
amglue_add_constants(FOR_ALL_INTERACTIVITY_KEY, interactivity_key);
amglue_copy_to_tag(interactivity_key, getconf);
amglue_add_enum_list(interactivity_key);
constant_pod(Interactivity Parameters, FOR_ALL_INTERACTIVITY_KEY)

#define FOR_ALL_TAPERSCAN_KEY(APPLY)\
APPLY(TAPERSCAN_COMMENT)\
APPLY(TAPERSCAN_PLUGIN)\
APPLY(TAPERSCAN_PROPERTY)

amglue_add_enum_tag_fns(taperscan_key);
amglue_add_constants(FOR_ALL_TAPERSCAN_KEY, taperscan_key);
amglue_copy_to_tag(taperscan_key, getconf);
amglue_add_enum_list(taperscan_key);
constant_pod(Taperscan Parameters, FOR_ALL_TAPERSCAN_KEY)

#define FOR_ALL_POLICY_KEY(APPLY)\
APPLY(POLICY_COMMENT)\
APPLY(POLICY_RETENTION_TAPES)\
APPLY(POLICY_RETENTION_DAYS)\
APPLY(POLICY_RETENTION_RECOVER)\
APPLY(POLICY_RETENTION_FULL)

amglue_add_enum_tag_fns(policy_key);
amglue_add_constants(FOR_ALL_POLICY_KEY, policy_key);
amglue_copy_to_tag(policy_key, getconf);
amglue_add_enum_list(policy_key);
constant_pod(Policy Parameters, FOR_ALL_POLICY_KEY)

#define FOR_ALL_STORAGE_KEY(APPLY)\
APPLY(STORAGE_COMMENT)\
APPLY(STORAGE_TAPEDEV)\
APPLY(STORAGE_TPCHANGER)\
APPLY(STORAGE_POLICY)\
APPLY(STORAGE_LABELSTR)\
APPLY(STORAGE_AUTOLABEL)\
APPLY(STORAGE_META_AUTOLABEL) \
APPLY(STORAGE_TAPEPOOL) \
APPLY(STORAGE_RUNTAPES) \
APPLY(STORAGE_TAPERSCAN) \
APPLY(STORAGE_TAPETYPE) \
APPLY(STORAGE_MAX_DLE_BY_VOLUME) \
APPLY(STORAGE_TAPERALGO) \
APPLY(STORAGE_TAPER_PARALLEL_WRITE) \
APPLY(STORAGE_EJECT_VOLUME) \
APPLY(STORAGE_ERASE_VOLUME) \
APPLY(STORAGE_DEVICE_OUTPUT_BUFFER_SIZE) \
APPLY(STORAGE_AUTOFLUSH) \
APPLY(STORAGE_FLUSH_THRESHOLD_DUMPED) \
APPLY(STORAGE_FLUSH_THRESHOLD_SCHEDULED) \
APPLY(STORAGE_TAPERFLUSH) \
APPLY(STORAGE_REPORT_USE_MEDIA) \
APPLY(STORAGE_REPORT_NEXT_MEDIA) \
APPLY(STORAGE_INTERACTIVITY) \
APPLY(STORAGE_SET_NO_REUSE) \
APPLY(STORAGE_DUMP_SELECTION) \
APPLY(STORAGE_ERASE_ON_FAILURE) \
APPLY(STORAGE_ERASE_ON_FULL)

amglue_add_enum_tag_fns(storage_key);
amglue_add_constants(FOR_ALL_STORAGE_KEY, storage_key);
amglue_copy_to_tag(storage_key, getconf);
amglue_add_enum_list(storage_key);
constant_pod(Storage Parameters, FOR_ALL_STORAGE_KEY)

#define FOR_ALL_CATALOG_KEY(APPLY)\
APPLY(CATALOG_COMMENT)\
APPLY(CATALOG_PLUGIN)\
APPLY(CATALOG_PROPERTY)

amglue_add_enum_tag_fns(catalog_key);
amglue_add_constants(FOR_ALL_CATALOG_KEY, catalog_key);
amglue_copy_to_tag(catalog_key, getconf);
constant_pod(Catalog Parameters, FOR_ALL_CATALOG_KEY)

/*
 * Various enumerated conftypes
 */

amglue_add_enum_and_string_tag_fns(no_yes_all);
amglue_add_constant_and_string(VALUE_NO , "NO" , no_yes_all);
amglue_add_constant_and_string(VALUE_YES, "YES", no_yes_all);
amglue_add_constant_and_string(VALUE_ALL, "ALL", no_yes_all);
amglue_copy_to_tag(no_yes_all, getconf);

amglue_add_enum_and_string_tag_fns(priority);
amglue_add_constant_and_string(PRIORITY_LOW, "LOW", priority);
amglue_add_constant_and_string(PRIORITY_MEDIUM, "MEDIUM", priority);
amglue_add_constant_and_string(PRIORITY_HIGH, "HIGH", priority);
amglue_copy_to_tag(priority, getconf);

amglue_add_enum_and_string_tag_fns(holdingdisk);
amglue_add_constant_and_string(HOLD_NEVER, "NEVER", holdingdisk);
amglue_add_constant_and_string(HOLD_AUTO,  "AUTO", holdingdisk);
amglue_add_constant_and_string(HOLD_REQUIRED, "REQUIRED", holdingdisk);
amglue_copy_to_tag(holdingdisk, getconf);

amglue_add_enum_and_string_tag_fns(comp);
amglue_add_constant_and_string(COMP_NONE, "NONE", comp);
amglue_add_constant_and_string(COMP_FAST, "FAST", comp);
amglue_add_constant_and_string(COMP_BEST, "BEST", comp);
amglue_add_constant_and_string(COMP_CUST, "CUSTOM", comp);
amglue_add_constant_and_string(COMP_SERVER_FAST, "SERVER FAST", comp);
amglue_add_constant_and_string(COMP_SERVER_BEST, "SERVER BEST", comp);
amglue_add_constant_and_string(COMP_SERVER_CUST, "SERVER CUSTOM", comp);
amglue_copy_to_tag(comp, getconf);

amglue_add_enum_and_string_tag_fns(encrypt);
amglue_add_constant_and_string(ENCRYPT_NONE, "NONE", encrypt);
amglue_add_constant_and_string(ENCRYPT_CUST, "CUSTOM", encrypt);
amglue_add_constant_and_string(ENCRYPT_SERV_CUST, "SERVER CUSTOM", encrypt);
amglue_copy_to_tag(encrypt, getconf);

amglue_add_enum_and_string_tag_fns(strategy);
amglue_add_constant_and_string(DS_SKIP, "SKIP", strategy);
amglue_add_constant_and_string(DS_STANDARD, "STANDARD", strategy);
amglue_add_constant_and_string(DS_NOFULL, "NOFULL", strategy);
amglue_add_constant_and_string(DS_NOINC, "NOINC", strategy);
amglue_add_constant_and_string(DS_4, "DS4", strategy);
amglue_add_constant_and_string(DS_5, "DS5", strategy);
amglue_add_constant_and_string(DS_HANOI, "HANOI", strategy);
amglue_add_constant_and_string(DS_INCRONLY, "INCRONLY", strategy);
amglue_copy_to_tag(strategy, getconf);

amglue_add_enum_and_string_tag_fns(estimate);
amglue_add_constant_and_string(ES_CLIENT, "CLIENT", estimate);
amglue_add_constant_and_string(ES_SERVER, "SERVER", estimate);
amglue_add_constant_and_string(ES_CALCSIZE, "CALCSIZE", estimate);

%perlcode %{
sub estimate_list_to_values {
    my $estimates = shift;
    my @estimate_list_value;

    foreach my $estimate_string ( split(' ', $estimates)) {
	push @estimate_list_value, estimate_to_value($estimate_string);
    }
    return @estimate_list_value;
}
push @EXPORT_OK, qw(estimate_list_to_values);
push @{$EXPORT_TAGS{"estimate"}}, qw(estimate_list_to_values);
%}
amglue_copy_to_tag(estimate, getconf);

amglue_add_enum_and_string_tag_fns(autolabel_enum);
amglue_add_constant_and_string(AL_OTHER_CONFIG, "OTHER-CONFIG", autolabel_enum);
amglue_add_constant_and_string(AL_NON_AMANDA, "NON-AMANDA", autolabel_enum);
amglue_add_constant_and_string(AL_VOLUME_ERROR, "VOLUME-ERROR", autolabel_enum);
amglue_add_constant_and_string(AL_EMPTY, "EMPTY", autolabel_enum);
%perlcode %{
sub autolabel_to_value {
    my $autolabel = shift;
    my $new_autolabel;

    $new_autolabel->{'template'} = $autolabel->{'template'};
    $new_autolabel->{'autolabel'} = 0;
    $new_autolabel->{'autolabel'} |= $AL_OTHER_CONFIG if $autolabel->{'other_config'};
    $new_autolabel->{'autolabel'} |= $AL_NON_AMANDA   if $autolabel->{'non_amanda'};
    $new_autolabel->{'autolabel'} |= $AL_VOLUME_ERROR if $autolabel->{'volume_error'};
    $new_autolabel->{'autolabel'} |= $AL_EMPTY        if $autolabel->{'empty'};
    return $new_autolabel;
}
push @EXPORT_OK, qw(autolabel_to_value);
push @{$EXPORT_TAGS{"autolabel_enum"}}, qw(autolabel_to_value);
%}
amglue_copy_to_tag(autolabel_enum, getconf);

amglue_add_enum_and_string_tag_fns(taperalgo);
amglue_add_constant_and_string(ALGO_FIRST, "FIRST", taperalgo);
amglue_add_constant_and_string(ALGO_FIRSTFIT, "FIRSTFIT", taperalgo);
amglue_add_constant_and_string(ALGO_LARGEST, "LARGEST", taperalgo);
amglue_add_constant_and_string(ALGO_LARGESTFIT, "LARGESTFIT", taperalgo);
amglue_add_constant_and_string(ALGO_SMALLEST, "SMALLEST", taperalgo);
amglue_add_constant_and_string(ALGO_SMALLESTFIT, "SMALLESTFIT", taperalgo);
amglue_add_constant_and_string(ALGO_LAST, "LAST", taperalgo);
amglue_add_constant_and_string(ALGO_LASTFIT, "LASTFIT", taperalgo);
amglue_copy_to_tag(taperalgo, getconf);

amglue_add_enum_and_string_tag_fns(execute_on);
amglue_add_constant_and_string(EXECUTE_ON_PRE_AMCHECK,        "PRE-AMCHECK", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_PRE_DLE_AMCHECK,    "PRE-DLE-AMCHECK", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_PRE_HOST_AMCHECK,   "PRE-HOST-AMCHECK", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_POST_AMCHECK,       "POST-AMCHECK", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_POST_DLE_AMCHECK,   "POST-DLE-AMCHECK", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_POST_HOST_AMCHECK,  "POST-HOST-AMCHECK", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_PRE_ESTIMATE,       "PRE-ESTIMATE", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_PRE_DLE_ESTIMATE,   "PRE-DLE-ESTIMATE", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_PRE_HOST_ESTIMATE,  "PRE-HOST-ESTIMATE", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_POST_ESTIMATE,      "POST-ESTIMATE", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_POST_DLE_ESTIMATE,  "POST-DLE-ESTIMATE", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_POST_HOST_ESTIMATE, "POST-HOST-ESTIMATE", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_PRE_BACKUP,         "PRE-BACKUP", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_PRE_DLE_BACKUP,     "PRE-DLE-BACKUP", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_PRE_HOST_BACKUP,    "PRE-HOST-BACKUP", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_POST_BACKUP,        "POST-BACKUP", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_POST_DLE_BACKUP,    "POST-DLE-BACKUP", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_POST_HOST_BACKUP,   "POST-HOST-BACKUP", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_PRE_RECOVER,        "PRE-RECOVER", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_POST_RECOVER,       "POST-RECOVER", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_PRE_LEVEL_RECOVER,  "PRE-LEVEL-RECOVER", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_POST_LEVEL_RECOVER, "POST-LEVEL-RECOVER", execute_on);
amglue_add_constant_and_string(EXECUTE_ON_INTER_LEVEL_RECOVER,"INTER-LEVEL-RECOVER", execute_on);
%perlcode %{
sub execute_on_list{
    my $execute_on = shift;
    my $new_execute_on;

    foreach my $execute_on_string ( split(' ', $execute_on)) {
	$new_execute_on |= execute_on_to_value($execute_on_string);
    }
    return $new_execute_on;
}
push @EXPORT_OK, qw(execute_on_list);
push @{$EXPORT_TAGS{"execute_on"}}, qw(execute_on_list);
%}
amglue_copy_to_tag(execute_on, getconf);

amglue_add_enum_and_string_tag_fns(execute_where);
amglue_add_constant_and_string(EXECUTE_WHERE_CLIENT, "CLIENT", execute_where);
amglue_add_constant_and_string(EXECUTE_WHERE_SERVER, "SERVER", execute_where);
amglue_copy_to_tag(execute_where, getconf);

amglue_add_enum_and_string_tag_fns(send_amreport_on);
amglue_add_constant_and_string(SEND_AMREPORT_ALL, "ALL", send_amreport_on);
amglue_add_constant_and_string(SEND_AMREPORT_STRANGE, "STRANGE", send_amreport_on);
amglue_add_constant_and_string(SEND_AMREPORT_ERROR, "ERROR", send_amreport_on);
amglue_add_constant_and_string(SEND_AMREPORT_NEVER, "NEVER", send_amreport_on);
amglue_copy_to_tag(send_amreport_on, getconf);

amglue_add_enum_and_string_tag_fns(data_path);
amglue_add_constant_and_string(DATA_PATH_AMANDA, "AMANDA", data_path);
amglue_add_constant_and_string(DATA_PATH_DIRECTTCP, "DIRECTTCP", data_path);
amglue_copy_to_tag(data_path, getconf);

amglue_add_enum_and_string_tag_fns(part_cache_type);
amglue_add_constant_and_string(PART_CACHE_TYPE_NONE, "NONE", part_cache_type);
amglue_add_constant_and_string(PART_CACHE_TYPE_DISK, "DISK", part_cache_type);
amglue_add_constant_and_string(PART_CACHE_TYPE_MEMORY, "MEMORY", part_cache_type);
amglue_copy_to_tag(part_cache_type, getconf);

amglue_add_enum_and_string_tag_fns(dumptype_tag);
amglue_add_constant_and_string(TAG_NAME, "NAME", dumptype_tag);
amglue_add_constant_and_string(TAG_ALL, "ALL", dumptype_tag);
amglue_add_constant_and_string(TAG_OTHER, "OTHER", dumptype_tag);
amglue_copy_to_tag(dumptype_tag, getconf);

amglue_add_enum_and_string_tag_fns(level_type);
amglue_add_constant_and_string(LEVEL_ALL, "ALL", level_type);
amglue_add_constant_and_string(LEVEL_FULL, "FULL", level_type);
amglue_add_constant_and_string(LEVEL_INCR, "INCR", level_type);
amglue_copy_to_tag(level_type, getconf);

%perlcode %{
sub host_limit_to_value {
    my $limits = shift;
    my $new_limits;

    $new_limits->{'server'} = 0;
    $new_limits->{'same_host'} = 0;
    $new_limits->{'match_pats'} = ();
    foreach my $limit (@$limits) {
	if ($limit eq "SERVER-SERVER-SERVER") {
	   $new_limits->{'server'} = 1;
	} elsif ($limit eq "SAMEHOST-SAMEHOST-SAMEHOST") {
	   $new_limits->{'same_host'} = 1;
	} else {
	    push @{$new_limits->{'match_pats'}}, $limit;
	}
    }
    return $new_limits;
}
push @EXPORT_OK, qw(host_limit_to_value);
push @{$EXPORT_TAGS{"getconf"}}, qw(host_limit_to_value);
%}

%perlcode %{
sub labelstr_to_value {
    my $labelstr = shift;
    my $new_labelstr;

    $new_labelstr->{'template'} = $labelstr->{'template'};
    $new_labelstr->{'match_autolabel'} = string_to_boolean($labelstr->{'match_autolabel'});
    return $new_labelstr;
}
push @EXPORT_OK, qw(labelstr_to_value);
push @{$EXPORT_TAGS{"getconf"}}, qw(labelstr_to_value);
%}

%perlcode %{
sub dump_selection_to_value {
    my $dump_selections = shift;
    my $new_dump_selections;

    foreach my $dump_selection (@$dump_selections) {
	my $new_dump_selection;
	$new_dump_selection->{'tag'} = $dump_selection->{'tag'};
	$new_dump_selection->{'tag_type'} = dumptype_tag_to_value($dump_selection->{'tag_type'});
	$new_dump_selection->{'level'} = level_type_to_value($dump_selection->{'level'});
	push @$new_dump_selections, $new_dump_selection;
    }
    return $new_dump_selections;
}
push @EXPORT_OK, qw(dump_selection_to_value);
push @{$EXPORT_TAGS{"getconf"}}, qw(dump_selection_to_value);
%}

%perlcode %{
sub vault_list_to_value {
    my $vault_list = shift;
    return $vault_list;
}
push @EXPORT_OK, qw(vault_list_to_value);
push @{$EXPORT_TAGS{"getconf"}}, qw(vault_list_to_value);

%}

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
 *  - CONFTYPE_PROPLIST -> hashref of hashref with keys 'append' (IV), 'visible' (IV),
							'priority' (IV), 'values' (listref)
 *  - CONFTYPE_HOST_LIMIT -> listref of match expressions;
 *           "SAMEHOST-SAMEHOST-SAMEHOST" in list is taken to mean 'same-host'
 *           "SERVER-SERVER-SERVER" in list is taken to mean 'server'
 *  - CONFTYPE_DUMP_SELECTION -> listref of hashref with keys 'tag' (PV), tag_type (IV),
							      'level' (IV)
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

	    case CONFTYPE_IDENTLIST:
	    case CONFTYPE_STR_LIST: {
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

	    case CONFTYPE_DUMP_SELECTION: {
		AV *av;
		dump_selection_list_t dsl;
		av = newAV();
		for (dsl = val_t__dump_selection(val); dsl != NULL;
		     dsl = dsl->next) {
		    dump_selection_t *ds = dsl->data;
		    HV *hv;

		    hv = (HV *)sv_2mortal((SV *)newHV());

		    if (ds->tag) {
			hv_store(hv, "tag", strlen("tag"), newSVpv(ds->tag, 0), 0);
		    }
		    hv_store(hv, "tag_type", strlen("tag_type"), newSViv(ds->tag_type), 0);
		    hv_store(hv, "level", strlen("level"), newSViv(ds->level), 0);
		    av_push(av, newRV_inc((SV*)hv));
		}

		results[0] = sv_2mortal(newRV_noinc((SV *)av));
		return 1;
	    }

	    case CONFTYPE_VAULT_LIST: {
		AV *av;
		vault_list_t vl;
		av = newAV();
		for (vl = val_t__vault_list(val); vl != NULL; vl = vl->next) {
		    vault_el_t *v = vl->data;
		    HV *hv;

		    hv = (HV *)sv_2mortal((SV *)newHV());

		    hv_store(hv, "storage", strlen("storage"), newSVpv(v->storage, 0), 0);
		    hv_store(hv, "days", strlen("says"), newSViv(v->days), 0);
		    av_push(av, newRV_inc((SV*)hv));
		}

		results[0] = sv_2mortal(newRV_noinc((SV *)av));
		return 1;
	    }

	    case CONFTYPE_AUTOLABEL: {
		autolabel_t *autolabel = val_t__autolabel(val);
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

	    case CONFTYPE_LABELSTR: {
		labelstr_s *labelstr = val_t__labelstr(val);
		HV *hv;

		/* now build the hash */
		hv = (HV *)sv_2mortal((SV *)newHV());
		hv_store(hv, "template", 8,
			(labelstr->template)? newSVpv(labelstr->template, 0) : newSV(0),
			0);
		hv_store(hv, "match_autolabel", 15,
			(labelstr->match_autolabel)? &PL_sv_yes : &PL_sv_no,
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

/* Typemap to convert a val_t to an SV for a human readable format, using a static function
 * since it's huge.  This converts:
 *  - CONFTYPE_SIZE, CONFTYPE_INT, CONFTYPE_INT64 -> IV
 *  - CONFTYPE_NO_YES_ALL -> PV,
 *  - CONFTYPE_BOOLEAN -> PV
 *  - CONFTYPE_REAL -> NV
 *  - CONFTYPE_STR, CONFTYPE_IDENT -> PV
 *  - CONFTYPE_APPLICATION -> PV
 *  - CONFTYPE_TIME -> IV (epoch timestamp)
 *  - CONFTYPE_COMPRESS, CONFTYPE_ENCRYPT, CONFTYPE_ESTIMATELIST, CONFTYPE_STRATEGY,
 *    CONFTYPE_TAPERALGO, CONFTYPE_PRIORITY, CONFTYPE_HOLDING, CONFTYPE_EXECUTE_ON,
 *    CONFTYPE_EXECUTE_WHERE, CONFTYPE_SEND_AMREPORT_ON,
 *    CONFTYPE_DATA_PATH, CONFTYPE_PART_CACHE_TYPE -> PV (string of the enum)
 *  - CONFTYPE_RATE -> list of two NVs
 *  - CONFTYPE_INTRANGE -> list of two IVs
 *  - CONFTYPE_EXINCLUDE -> hashref with keys 'list' (listref), 'file' (listref),
 *    and 'optional' (int)
 *  - CONFTYPE_PROPLIST -> hashref of hashref with keys 'append' (IV), 'visible' (IV),
							'priority' (IV), 'values' (listref)
 *  - CONFTYPE_HOST_LIMIT -> listref of match expressions;
 *           "SAMEHOST-SAMEHOST-SAMEHOST" in list is taken to mean 'same-host'
 *           "SERVER-SERVER-SERVER" in list is taken to mean 'server'
 *  - CONFTYPE_DUMP_SELECTION -> listref of hashref with keys 'tag' (PV), tag_type (IV),
							      'level' (IV)
 */

%{
static int
val_t_to_print(val_t *val, SV **results) {
    if (!val) {
	results[0] = &PL_sv_undef;
	return 1;
    } else {
	switch (val->type) {
	    case CONFTYPE_RATE: {
		AV *rlist = newAV();
		av_push(rlist, newSVuv(val_t__rate(val)[0]));
		av_push(rlist, newSVuv(val_t__rate(val)[1]));
		results[0] = sv_2mortal(newRV_noinc((SV *)rlist));
		return 1;
	    }

	    case CONFTYPE_INTRANGE: {
		AV *ilist = newAV();
		av_push(ilist, newSVuv(val_t__intrange(val)[0]));
		av_push(ilist, newSVuv(val_t__intrange(val)[1]));
		results[0] = sv_2mortal(newRV_noinc((SV *)ilist));
		return 1;
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

	    case CONFTYPE_BOOLEAN:
		results[0] = sv_newmortal();
		sv_setpv(results[0], val_t__boolean(val)==VALUE_NO?"NO":"YES");
		return 1;

	    case CONFTYPE_NO_YES_ALL:
		results[0] = sv_newmortal();
		switch val_t__no_yes_all(val) {
			case VALUE_NO : sv_setpv(results[0], "NO" ); break;
			case VALUE_YES: sv_setpv(results[0], "YES"); break;
			case VALUE_ALL: sv_setpv(results[0], "ALL"); break;
		}
		return 1;

	    case CONFTYPE_COMPRESS:
		results[0] = sv_newmortal();
		switch val_t__compress(val) {
			case COMP_NONE: sv_setpv(results[0], "NONE"); break;
			case COMP_FAST: sv_setpv(results[0], "CLIENT FAST"); break;
			case COMP_BEST: sv_setpv(results[0], "CLIENT BEST"); break;
			case COMP_CUST: sv_setpv(results[0], "CLIENT CUSTOM"); break;
			case COMP_SERVER_FAST: sv_setpv(results[0], "SERVER FAST"); break;
			case COMP_SERVER_BEST: sv_setpv(results[0], "SERVER BEST"); break;
			case COMP_SERVER_CUST: sv_setpv(results[0], "SERVER CUSTOM"); break;
		}
		return 1;

	    case CONFTYPE_ENCRYPT:
		results[0] = sv_newmortal();
		switch val_t__encrypt(val) {
			case ENCRYPT_NONE: sv_setpv(results[0], "NONE"); break;
			case ENCRYPT_CUST: sv_setpv(results[0], "CLIENT"); break;
			case ENCRYPT_SERV_CUST: sv_setpv(results[0], "SERVER"); break;
		}
		return 1;

	    case CONFTYPE_STRATEGY:
		results[0] = sv_newmortal();
		switch val_t__strategy(val) {
			case DS_SKIP: sv_setpv(results[0], "SKIP"); break;
			case DS_STANDARD: sv_setpv(results[0], "STANDARD"); break;
			case DS_NOFULL: sv_setpv(results[0], "NOFULL"); break;
			case DS_NOINC: sv_setpv(results[0], "NOINC"); break;
			case DS_4: sv_setpv(results[0], "DS4"); break;
			case DS_5: sv_setpv(results[0], "DS5"); break;
			case DS_HANOI: sv_setpv(results[0], "HANOI"); break;
			case DS_INCRONLY: sv_setpv(results[0], "INCRONLY"); break;
		}
		return 1;

	    case CONFTYPE_TAPERALGO:
		results[0] = sv_newmortal();
		switch val_t__taperalgo(val) {
			case ALGO_FIRST: sv_setpv(results[0], "FIRST"); break;
			case ALGO_FIRSTFIT: sv_setpv(results[0], "FIRSTFIT"); break;
			case ALGO_LARGEST: sv_setpv(results[0], "LARGEST"); break;
			case ALGO_LARGESTFIT: sv_setpv(results[0], "LARGESTFIT"); break;
			case ALGO_SMALLEST: sv_setpv(results[0], "SMALLEST"); break;
			case ALGO_SMALLESTFIT: sv_setpv(results[0], "SMALLESTFIT"); break;
			case ALGO_LAST: sv_setpv(results[0], "LAST"); break;
			case ALGO_LASTFIT: sv_setpv(results[0], "LASTFIT"); break;
		}
		return 1;

	    case CONFTYPE_PRIORITY:
		results[0] = sv_newmortal();
		switch val_t__priority(val) {
			case PRIORITY_LOW: sv_setpv(results[0], "LOW"); break;
			case PRIORITY_MEDIUM: sv_setpv(results[0], "MEDIUM"); break;
			case PRIORITY_HIGH: sv_setpv(results[0], "HIGH"); break;
			default: results[0] = sv_2mortal(amglue_newSVi64(val_t__size(val))); break;
		}
		return 1;

	    case CONFTYPE_HOLDING:
		results[0] = sv_newmortal();
		switch val_t__holding(val) {
			case HOLD_NEVER: sv_setpv(results[0], "NEVER"); break;
			case HOLD_AUTO: sv_setpv(results[0], "AUTO"); break;
			case HOLD_REQUIRED: sv_setpv(results[0], "REQUIRED"); break;
		}
		return 1;

	    case CONFTYPE_EXECUTE_ON: {
		AV *elist = newAV();
		execute_on_t execute_on = val_t__execute_on(val);
		if (execute_on & EXECUTE_ON_PRE_AMCHECK)
			av_push(elist, newSVpv("PRE-AMCHECK", 0));
		if (execute_on & EXECUTE_ON_PRE_DLE_AMCHECK)
			av_push(elist, newSVpv("PRE-DLE-AMCHECK", 0));
		if (execute_on & EXECUTE_ON_PRE_HOST_AMCHECK)
			av_push(elist, newSVpv("PRE-HOST-AMCHECK", 0));
		if (execute_on & EXECUTE_ON_POST_AMCHECK)
			av_push(elist, newSVpv("POST-AMCHECK", 0));
		if (execute_on & EXECUTE_ON_POST_DLE_AMCHECK)
			av_push(elist, newSVpv("POST-DLE-AMCHECK", 0));
		if (execute_on & EXECUTE_ON_POST_HOST_AMCHECK)
			av_push(elist, newSVpv("POST-HOST-AMCHECK", 0));
		if (execute_on & EXECUTE_ON_PRE_ESTIMATE)
			av_push(elist, newSVpv("PRE-ESTIMATE", 0));
		if (execute_on & EXECUTE_ON_PRE_DLE_ESTIMATE)
			av_push(elist, newSVpv("PRE-DLE-ESTIMATE", 0));
		if (execute_on & EXECUTE_ON_PRE_HOST_ESTIMATE)
			av_push(elist, newSVpv("PRE-HOST-ESTIMATE", 0));
		if (execute_on & EXECUTE_ON_POST_ESTIMATE)
			av_push(elist, newSVpv("POST-ESTIMATE", 0));
		if (execute_on & EXECUTE_ON_POST_DLE_ESTIMATE)
			av_push(elist, newSVpv("POST-DLE-ESTIMATE", 0));
		if (execute_on & EXECUTE_ON_POST_HOST_ESTIMATE)
			av_push(elist, newSVpv("POST-HOST-ESTIMATE", 0));
		if (execute_on & EXECUTE_ON_PRE_BACKUP)
			av_push(elist, newSVpv("PRE-BACKUP", 0));
		if (execute_on & EXECUTE_ON_PRE_DLE_BACKUP)
			av_push(elist, newSVpv("PRE-DLE-BACKUP", 0));
		if (execute_on & EXECUTE_ON_PRE_HOST_BACKUP)
			av_push(elist, newSVpv("PRE-HOST-BACKUP", 0));
		if (execute_on & EXECUTE_ON_POST_BACKUP)
			av_push(elist, newSVpv("POST-BACKUP", 0));
		if (execute_on & EXECUTE_ON_POST_DLE_BACKUP)
			av_push(elist, newSVpv("POST-DLE-BACKUP", 0));
		if (execute_on & EXECUTE_ON_POST_HOST_BACKUP)
			av_push(elist, newSVpv("POST-HOST-BACKUP", 0));
		if (execute_on & EXECUTE_ON_PRE_RECOVER)
			av_push(elist, newSVpv("PRE-RECOVER", 0));
		if (execute_on & EXECUTE_ON_POST_RECOVER)
			av_push(elist, newSVpv("POST-RECOVER", 0));
		if (execute_on & EXECUTE_ON_PRE_LEVEL_RECOVER)
			av_push(elist, newSVpv("PRE-LEVEL-RECOVER", 0));
		if (execute_on & EXECUTE_ON_POST_LEVEL_RECOVER)
			av_push(elist, newSVpv("POST-LEVEL-RECOVER", 0));
		if (execute_on & EXECUTE_ON_INTER_LEVEL_RECOVER)
			av_push(elist, newSVpv("INTER-LEVEL-RECOVER", 0));
		results[0] = sv_2mortal(newRV_noinc((SV *)elist));
		return 1;
	    }

	    case CONFTYPE_EXECUTE_WHERE:
		results[0] = sv_newmortal();
		switch val_t__execute_where(val) {
			case EXECUTE_WHERE_CLIENT: sv_setpv(results[0], "CLIENT"); break;
			case EXECUTE_WHERE_SERVER: sv_setpv(results[0], "SERVER"); break;
		}
		return 1;

	    case CONFTYPE_SEND_AMREPORT_ON:
		results[0] = sv_newmortal();
		switch val_t__send_amreport(val) {
			case SEND_AMREPORT_ALL: sv_setpv(results[0], "ALL"); break;
			case SEND_AMREPORT_STRANGE: sv_setpv(results[0], "STRANGE"); break;
			case SEND_AMREPORT_ERROR: sv_setpv(results[0], "ERROR"); break;
			case SEND_AMREPORT_NEVER: sv_setpv(results[0], "NEVER"); break;
		}
		return 1;

	    case CONFTYPE_DATA_PATH:
		results[0] = sv_newmortal();
		switch val_t__data_path(val) {
			case DATA_PATH_AMANDA: sv_setpv(results[0], "AMANDA"); break;
			case DATA_PATH_DIRECTTCP: sv_setpv(results[0], "DIRECTTCP"); break;
		}
		return 1;

	    case CONFTYPE_PART_CACHE_TYPE:
		results[0] = sv_newmortal();
		switch val_t__part_cache_type(val) {
			case PART_CACHE_TYPE_NONE: sv_setpv(results[0], "NONE"); break;
			case PART_CACHE_TYPE_MEMORY: sv_setpv(results[0], "MEMORY"); break;
			case PART_CACHE_TYPE_DISK: sv_setpv(results[0], "DISK"); break;
		}
		return 1;

	    case CONFTYPE_INT:
		results[0] = sv_2mortal(amglue_newSVi64(val_t__int(val)));
		return 1;

           case CONFTYPE_ESTIMATELIST: {
		AV *elist = newAV();
		estimatelist_t el;
		for (el=val_t__estimatelist(val); el != NULL; el = el->next) {
		    int estimate = GPOINTER_TO_INT(el->data);
		    switch (estimate) {
			case ES_CALCSIZE: av_push(elist, newSVpv("CALCSIZE", 0)); break;
			case ES_CLIENT: av_push(elist, newSVpv("CLIENT", 0)); break;
			case ES_SERVER: av_push(elist, newSVpv("SERVER", 0)); break;
		    }
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

	    case CONFTYPE_IDENTLIST:
	    case CONFTYPE_STR_LIST: {
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

	    case CONFTYPE_DUMP_SELECTION: {
		AV *av;
		dump_selection_list_t dsl;
		av = newAV();
		for (dsl = val_t__dump_selection(val); dsl != NULL; dsl = dsl->next) {
		    dump_selection_t *ds = dsl->data;
		    HV *hv;

		    hv = (HV *)sv_2mortal((SV *)newHV());

		    if (ds->tag) {
			hv_store(hv, "tag", strlen("tag"), newSVpv(ds->tag, 0), 0);
		    }
		    switch (ds->tag_type) {
			case TAG_NAME: hv_store(hv, "tag_type", strlen("tag_type"), newSVpv("TAG",0), 0); break;
			case TAG_ALL: hv_store(hv, "tag_type", strlen("tag_type"), newSVpv("ALL",0), 0); break;
			case TAG_OTHER: hv_store(hv, "tag_type", strlen("tag_type"), newSVpv("OTHER",0), 0); break;
		    }
		    switch (ds->level) {
			case LEVEL_ALL : hv_store(hv, "level", strlen("level"), newSVpv("ALL",0), 0); break;
			case LEVEL_FULL: hv_store(hv, "level", strlen("level"), newSVpv("FULL",0), 0); break;
			case LEVEL_INCR: hv_store(hv, "level", strlen("level"), newSVpv("INCR",0), 0); break;
		    }
		    av_push(av, newRV_inc((SV*)hv));
		}

		results[0] = sv_2mortal(newRV_noinc((SV *)av));
		return 1;
	    }

	    case CONFTYPE_VAULT_LIST: {
		AV *av;
		vault_list_t vl;
		av = newAV();
		for (vl = val_t__vault_list(val); vl != NULL; vl = vl->next) {
		    vault_el_t *v = vl->data;
		    HV *hv;

		    hv = (HV *)sv_2mortal((SV *)newHV());

		    hv_store(hv, "storage", strlen("storage"), newSVpv(v->storage, 0), 0);
		    hv_store(hv, "days", strlen("days"), newSViv(v->days), 0);
		    av_push(av, newRV_inc((SV*)hv));
		}

		results[0] = sv_2mortal(newRV_noinc((SV *)av));
		return 1;
	    }

	    case CONFTYPE_AUTOLABEL: {
		autolabel_t *autolabel = val_t__autolabel(val);
		HV *hv;

		/* now build the hash */
		hv = (HV *)sv_2mortal((SV *)newHV());
		hv_store(hv, "template", 8,
			(autolabel->template)? newSVpv(autolabel->template, 0) : newSV(0), 0);
		hv_store(hv, "other_config", 12,
			newSVpv((autolabel->autolabel & AL_OTHER_CONFIG)? "YES" : "NO", 0), 0);
		hv_store(hv, "non_amanda", 10,
			newSVpv((autolabel->autolabel & AL_NON_AMANDA)? "YES" : "NO", 0), 0);
		hv_store(hv, "volume_error", 12,
			newSVpv((autolabel->autolabel & AL_VOLUME_ERROR)? "YES" : "NO", 0), 0);
		hv_store(hv, "empty", 5,
			newSVpv((autolabel->autolabel & AL_EMPTY)? "YES" : "NO", 0), 0);

		results[0] = sv_2mortal(newRV((SV *)hv));
		return 1;
	    }

	    case CONFTYPE_LABELSTR: {
		labelstr_s *labelstr = val_t__labelstr(val);
		HV *hv;

		/* now build the hash */
		hv = (HV *)sv_2mortal((SV *)newHV());
		hv_store(hv, "template", 8,
			(labelstr->template)? newSVpv(labelstr->template, 0) : newSV(0),
			0);
		hv_store(hv, "match_autolabel", 15,
			newSVpv((labelstr->match_autolabel)? "YES" : "NO", 0), 0);
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
    EXTEND(SP, nresults);

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

    EXTEND(SP, g_slist_length(it));
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

    int count = 0;
    while (it && *it) {
	count++;
	it++;
    }
    EXTEND(SP, count);

    it = $1;
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
char *confparm_key_to_name(int token);
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
    getconf_list confparm_key_to_name
);

tapetype_t *lookup_tapetype(char *identifier);
val_t *tapetype_getconf(tapetype_t *ttyp, tapetype_key key);
char *tapetype_name(tapetype_t *ttyp);
char *tapetype_key_to_name(int token);
gboolean tapetype_seen(tapetype_t *ttyp, tapetype_key key);
amglue_export_tag(getconf,
    lookup_tapetype tapetype_getconf tapetype_name
    tapetype_key_to_name tapetype_seen
);

dumptype_t *lookup_dumptype(char *identifier);
val_t *dumptype_getconf(dumptype_t *dtyp, dumptype_key key);
char *dumptype_name(dumptype_t *dtyp);
char *dumptype_key_to_name(int token);
gboolean dumptype_seen(dumptype_t *dtyp, dumptype_key key);
amglue_export_tag(getconf,
    lookup_dumptype dumptype_getconf dumptype_name
    dumptype_key_to_name dumptype_seen
);

interface_t *lookup_interface(char *identifier);
val_t *interface_getconf(interface_t *iface, interface_key key);
char *interface_name(interface_t *iface);
char *interface_key_to_name(int token);
gboolean interface_seen(interface_t *iface, interface_key key);
amglue_export_tag(getconf,
    lookup_interface interface_getconf interface_name
    interface_key_to_name interface_seen
);

holdingdisk_t *lookup_holdingdisk(char *identifier);
GSList *getconf_holdingdisks(void);
val_t *holdingdisk_getconf(holdingdisk_t *hdisk, holdingdisk_key key);
char *holdingdisk_name(holdingdisk_t *hdisk);
char *holdingdisk_key_to_name(int token);
gboolean holdingdisk_seen(holdingdisk_t *hdisk, holdingdisk_key key);
amglue_export_tag(getconf,
    lookup_holdingdisk holdingdisk_getconf holdingdisk_name
    getconf_holdingdisks
    holdingdisk_key_to_name holdingdisk_seen
);

application_t *lookup_application(char *identifier);
val_t *application_getconf(application_t *app, application_key key);
char *application_name(application_t *app);
char *application_key_to_name(int token);
gboolean application_seen(application_t *app, application_key key);
amglue_export_tag(getconf,
    lookup_application application_getconf application_name
    application_key_to_name application_seen
);

pp_script_t *lookup_pp_script(char *identifier);
val_t *pp_script_getconf(pp_script_t *pps, pp_script_key key);
char *pp_script_name(pp_script_t *pps);
char *pp_script_key_to_name(int token);
gboolean pp_script_seen(pp_script_t *app, pp_script_key key);
amglue_export_tag(getconf,
    lookup_pp_script pp_script_getconf pp_script_name
    pp_script_key_to_name pp_script_seen
);

device_config_t *lookup_device_config(char *identifier);
val_t *device_config_getconf(device_config_t *pps, device_config_key key);
char *device_config_name(device_config_t *pps);
char *device_config_key_to_name(int token);
gboolean device_config_seen(device_config_t *app, device_config_key key);
amglue_export_tag(getconf,
    lookup_device_config device_config_getconf device_config_name
    device_config_key_to_name device_config_seen
);

%typemap(out) char ** {
    gchar **iter;

    if ($1) {
        /* Count the DeviceProperties */
        EXTEND(SP, g_strv_length($1)); /* make room for return values */

        /* Note that we set $result several times. the nature of
         * SWIG's wrapping is such that incrementing argvi points
         * $result to the next location in perl's argument stack.
         */

        for (iter = $1; *iter; iter++) {
            $result = sv_2mortal(newSVpv(*iter, 0));
            g_free(*iter);
            argvi++;
        }
        g_free($1);
    }
}
%newobject get_changer_list;
char **get_changer_list();
changer_config_t *lookup_changer_config(char *identifier);
val_t *changer_config_getconf(changer_config_t *pps, changer_config_key key);
char *changer_config_name(changer_config_t *pps);
char *changer_config_key_to_name(int token);
gboolean changer_config_seen(changer_config_t *app, changer_config_key key);
amglue_export_tag(getconf,
    lookup_changer_config changer_config_getconf changer_config_name
    changer_config_key_to_name changer_config_seen
);

interactivity_t *lookup_interactivity(char *identifier);
val_t *interactivity_getconf(interactivity_t *app, interactivity_key key);
char *interactivity_name(interactivity_t *app);
char *interactivity_key_to_name(int token);
gboolean interactivity_seen(interactivity_t *app, interactivity_key key);
amglue_export_tag(getconf,
    lookup_interactivity interactivity_getconf interactivity_name
    interactivity_key_to_name interactivity_seen
);

taperscan_t *lookup_taperscan(char *identifier);
val_t *taperscan_getconf(taperscan_t *app, taperscan_key key);
char *taperscan_name(taperscan_t *app);
char *taperscan_key_to_name(int token);
gboolean taperscan_seen(taperscan_t *app, taperscan_key key);
amglue_export_tag(getconf,
    lookup_taperscan taperscan_getconf taperscan_name
    taperscan_key_to_name taperscan_seen
);

policy_s *lookup_policy(char *identifier);
val_t *policy_getconf(policy_s *app, policy_key key);
char *policy_name(policy_s *app);
char *policy_key_to_name(int token);
gboolean policy_seen(policy_s *app, policy_key key);
amglue_export_tag(getconf,
    lookup_policy policy_getconf policy_name
    policy_key_to_name policy_seen
);

%typemap(out) char ** {
    gchar **iter;

    if ($1) {
        /* Count the DeviceProperties */
        EXTEND(SP, g_strv_length($1)); /* make room for return values */

        /* Note that we set $result several times. the nature of
         * SWIG's wrapping is such that incrementing argvi points
         * $result to the next location in perl's argument stack.
         */

        for (iter = $1; *iter; iter++) {
            $result = sv_2mortal(newSVpv(*iter, 0));
            g_free(*iter);
            argvi++;
        }
        g_free($1);
    }
}
%newobject get_storage_list;
char **get_storage_list();
storage_t *lookup_storage(char *identifier);
val_t *storage_getconf(storage_t *app, storage_key key);
char *storage_name(storage_t *app);
char *storage_key_to_name(int token);
gboolean storage_seen(storage_t *app, storage_key key);
amglue_export_tag(getconf,
    lookup_storage storage_getconf storage_name
    storage_key_to_name storage_seen
);

catalog_t *lookup_catalog(char *identifier);
val_t *catalog_getconf(catalog_t *app, catalog_key key);
char *catalog_name(catalog_t *app);
gboolean catalog_seen(catalog_t *app, catalog_key key);
amglue_export_tag(getconf,
    lookup_catalog catalog_getconf catalog_name
    catalog_seen catalog_seen
);

%perlcode %{
# only those keys with a value of '1' should be shown; the
# others are deprecated
our %subsection_names = (
    "application" => 1,
    "application-tool" => 0,
    "catalog" => 1,
    "changer" => 1,
    "device" => 1,
    "dumptype" => 1,
    "holdingdisk" => 1,
    "interactivity" => 1,
    "interface" => 1,
    "policy" => 1,
    "script" => 1,
    "script-tool" => 0,
    "storage" => 1,
    "tapetype" => 1,
    "taperscan" => 1,
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
amglue_add_constant(CONFIG_INIT_GLOBAL, config_init_flags);
amglue_add_constant(CONFIG_OVERRDIDE_NO_ERROR, config_init_flags);
amglue_copy_to_tag(config_init_flags, init);

cfgerr_level_t config_init(config_init_flags flags,
		     char *arg_config_name);
cfgerr_level_t config_init_with_global(config_init_flags flags,
		     char *arg_config_name);
void config_uninit(void);
char **get_config_options(int first);
char *get_config_name(void);
char *get_config_dir(void);
char *get_config_filename(void);
char *get_running_on(void);

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

    EXTEND(SP, g_slist_length(it));
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
    config_init config_init_with_global config_uninit get_config_options
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

%typemap (out) val_t * {
    SV *results[3], **iter;
    int nresults;

    /* save the stack, since val_t_to_sv may invoke arbitrary Perl code */
    SP += argvi; PUTBACK;
    nresults = val_t_to_print($1, results);
    SPAGAIN; SP -= argvi;
    EXTEND(SP, nresults);

    /* add val_t_to_sv's results to the stack */
    for (iter = results; nresults; iter++, nresults--) {
	$result = *iter;
	argvi++;
    }
}

val_t *getconf_human(confparm_key key);
val_t *dumptype_getconf_human(dumptype_t *typ, dumptype_key key);
val_t *tapetype_getconf_human(tapetype_t *typ, tapetype_key key);
val_t *application_getconf_human(application_t *typ, application_key key);
val_t *device_config_getconf_human(device_config_t *typ, device_config_key key);
val_t *changer_config_getconf_human(changer_config_t *typ, changer_config_key key);
val_t *storage_getconf_human(storage_t *typ, storage_key key);
val_t *pp_script_getconf_human(pp_script_t *typ, pp_script_key key);
val_t *holdingdisk_getconf_human(holdingdisk_t *typ, holdingdisk_key key);
val_t *interface_getconf_human(interface_t *typ, interface_key key);
val_t *interactivity_getconf_human(interactivity_t *typ, interactivity_key key);
val_t *taperscan_getconf_human(taperscan_t *typ, taperscan_key key);
val_t *policy_getconf_human(policy_s *typ, policy_key key);
amglue_export_tag(getconf,
	getconf_human
	dumptype_getconf_human tapetype_getconf_human application_getconf_human
	device_config_getconf_human changer_config_getconf_human storage_getconf_human
	pp_script_getconf_human holdingdisk_getconf_human interface_getconf_human
	interactivity_getconf_human taperscan_getconf_human policy_getconf_human);

%perlcode %{
package Amanda::Config::Message;
use strict;
use warnings;

use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 1500000) {
	return "config warning: $self->{'cfgerror'}";
    } elsif ($self->{'code'} == 1500001) {
	return "config error: $self->{'cfgerror'}";
    } elsif ($self->{'code'} == 1500002) {
	return "No config specified";
    } elsif ($self->{'code'} == 1500003) {
	return "config name";
    } elsif ($self->{'code'} == 1500004) {
	return "no config";
    } elsif ($self->{'code'} == 1500005) {
	return "INVALID ERROR CODE '1500005': " . Data::Dumper::Dumper($self);
	return "no such '$self->{'config'}' config";
    } elsif ($self->{'code'} == 1500006) {
	return "Can't open config directory '$self->{'dir'}': $self->{'errnostr'}";
    } elsif ($self->{'code'} == 1500007) {
	return "Not existant parameters";
    } elsif ($self->{'code'} == 1500008) {
	return "Parameters values";
    } elsif ($self->{'code'} == 1500009) {
	return "No fields specified";
    } elsif ($self->{'code'} == 1500010) {
	return "Storage '$self->{'storage'}' not found";
    } elsif ($self->{'code'} == 1500011) {
	return "Not existant parameters in storage '$self->{'storage'}'";
    } elsif ($self->{'code'} == 1500012) {
	return "Parameters values for storage '$self->{'storage'}'";
    } elsif ($self->{'code'} == 1500013) {
	return "No storage defined";
    } elsif ($self->{'code'} == 1500014) {
	return "Defined storage";
    } elsif ($self->{'code'} == 1500015) {
	return "No label specified";
    } elsif ($self->{'code'} == 1500016) {
	return "Config error";
    } elsif ($self->{'code'} == 1500017) {
	return "Changer '$self->{'changer'}' not found";
    } elsif ($self->{'code'} == 1500018) {
	return "Not existant parameters in changer '$self->{'changer'}'";
    } elsif ($self->{'code'} == 1500019) {
	return "Parameters values for changer '$self->{'changer'}'";
    } elsif ($self->{'code'} == 1500020) {
	return "No changer defined";
    } elsif ($self->{'code'} == 1500021) {
	return "Defined changer";
    } elsif ($self->{'code'} == 1500022) {
	return "Dumptype list";
    } elsif ($self->{'code'} == 1500023) {
	return "Tapetype list";
    } elsif ($self->{'code'} == 1500024) {
	return "Application list";
    } elsif ($self->{'code'} == 1500025) {
	return "Device list";
    } elsif ($self->{'code'} == 1500026) {
	return "Changer list";
    } elsif ($self->{'code'} == 1500027) {
	return "Storage list";
    } elsif ($self->{'code'} == 1500028) {
	return "Script list";
    } elsif ($self->{'code'} == 1500029) {
	return "Holdingdisk list";
    } elsif ($self->{'code'} == 1500030) {
	return "Interface list";
    } elsif ($self->{'code'} == 1500031) {
	return "Interactivity list";
    } elsif ($self->{'code'} == 1500032) {
	return "Taperscan list";
    } elsif ($self->{'code'} == 1500033) {
	return "Policy list";
    } elsif ($self->{'code'} == 1500034) {
	return "No '$self->{'dumptype'}' dumptype";
    } elsif ($self->{'code'} == 1500035) {
	return "No '$self->{'tapetype'}' tapetype";
    } elsif ($self->{'code'} == 1500036) {
	return "Storage '$self->{'storage'}' parameters values";
    } elsif ($self->{'code'} == 1500037) {
	return "Dumptype '$self->{'dumptype'}' parameters values";
    } elsif ($self->{'code'} == 1500038) {
	return "Tapetype '$self->{'tapetype'}' parameters values";
    } elsif ($self->{'code'} == 1500039) {
	return "Application '$self->{'application'}' parameters values";
    } elsif ($self->{'code'} == 1500040) {
	return "Device '$self->{'device'}' parameters values";
    } elsif ($self->{'code'} == 1500041) {
	return "Changers '$self->{'changer'}' parameters values";
    } elsif ($self->{'code'} == 1500042) {
	return "Script '$self->{'script'}' parameters values";
    } elsif ($self->{'code'} == 1500043) {
	return "Holding disk '$self->{'holdingdisk'}' parameters values";
    } elsif ($self->{'code'} == 1500044) {
	return "Interface '$self->{'interface'}' parameters values";
    } elsif ($self->{'code'} == 1500045) {
	return "Interactivity '$self->{'interactivity'}' parameters values";
    } elsif ($self->{'code'} == 1500046) {
	return "Taperscan '$self->{'taperscan'}' parameters values";
    } elsif ($self->{'code'} == 1500047) {
	return "Policy '$self->{'policy'}' parameters values";
    } elsif ($self->{'code'} == 1500048) {
	return "No '$self->{'storage'}' storage";
    } elsif ($self->{'code'} == 1500049) {
	return "No '$self->{'application'}' application";
    } elsif ($self->{'code'} == 1500050) {
	return "No '$self->{'device'}' device";
    } elsif ($self->{'code'} == 1500051) {
	return "No '$self->{'changer'}' changer";
    } elsif ($self->{'code'} == 1500052) {
	return "No '$self->{'script'}' script";
    } elsif ($self->{'code'} == 1500053) {
	return "No '$self->{'holdingdisk'}' holdingdisk";
    } elsif ($self->{'code'} == 1500054) {
	return "No '$self->{'interface'}' interface";
    } elsif ($self->{'code'} == 1500055) {
	return "No '$self->{'interactivity'}' interactivity";
    } elsif ($self->{'code'} == 1500056) {
	return "No '$self->{'taperscan'}' taperscan";
    } elsif ($self->{'code'} == 1500057) {
	return "No '$self->{'policy'}' policy";
    } elsif ($self->{'code'} == 1500058) {
	return "invalid '$self->{'field'}' field specified";
    } elsif ($self->{'code'} == 1550000) {
	return "The version";
    } elsif ($self->{'code'} == 1560000) {
	return "Can't set FAILURE is not configure with --with-failure-code";
    }
}

%}

