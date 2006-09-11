
/*
 * Copyright (c) 1998 T.Hepper
 */
#ifndef SCSIDEFS_H
#define SCSIDEFS_H

#ifndef WORDS_BIGENDIAN
#define LITTLE_ENDIAN_BITFIELDS
#endif

typedef enum { Input, Output } Direction_T; 
typedef unsigned char CDB_T[12];

#ifdef _AIX
typedef unsigned int PackedBit;
#define AIX_USE_GSC 1
#else
typedef unsigned char PackedBit;
#endif

#define INDEX_CHANGER 0
#define INDEX_TAPE 1
#define INDEX_TAPECTL 2

#define CHG_MAXDEV 32		/* Maximum number of devices handled by pDev */
				/* Must be large to hold the result of ScanBus */

#define TAPETYPE 4
#define IMPORT 3
#define STORAGE 2
#define CHANGER 1

#define TAG_SIZE 36
#define MAXTRIES 100  /* How many tries until SCSI_TestUnitReady should return an ok */
/*
 * Sense Key definitions
*/
#define SENSE_NULL 0
#define SENSE_RECOVERED_ERROR 1
#define NOT_READY 2
#define SENSE_NOT_READY 2
#define SENSE_MEDIUM_ERROR 3
#define SENSE_HARDWARE_ERROR 4
#define HARDWARE_ERROR 4
#define ILLEGAL_REQUEST 5
#define SENSE_ILLEGAL_REQUEST 5
#define UNIT_ATTENTION 6
#define SENSE_UNIT_ATTENTION 6
#define SENSE_DATA_PROTECT 7
#define SENSE_BLANK_CHECK 8
#define SENSE_VENDOR_SPECIFIC 0x9
#define SENSE_ABORTED_COMMAND 0xb
#define SENSE_VOLUME_OVERFLOW 0xd
#define SENSE_CHG_ELEMENT_STATUS 0xe

#define MAX_RETRIES 100

#define INQUIRY_SIZE SIZEOF(SCSIInquiry_T)

/*
 * Return values from the OS dependent part
 * of the SCSI interface
 *
 * The underlaying functions must decide what to do
 */
#define SCSI_ERROR -1
#define SCSI_OK 0
#define SCSI_SENSE 1
#define SCSI_BUSY 2
#define SCSI_CHECK 3


/*
 *  SCSI Commands
*/
#define SC_COM_TEST_UNIT_READY 0
#define SC_COM_REWIND 0x1
#define SC_COM_REQUEST_SENSE 0x3
#define SC_COM_IES 0x7
#define SC_COM_INQUIRY 0x12
#define SC_COM_MODE_SELECT 0x15
#define SC_COM_ERASE 0x19
#define SC_COM_MODE_SENSE 0x1A
#define SC_COM_UNLOAD 0x1B
#define SC_COM_LOCATE 0x2B
#define SC_COM_LOG_SELECT 0x4C
#define SC_COM_LOG_SENSE 0x4d
#define SC_MOVE_MEDIUM 0xa5
#define SC_COM_RES 0xb8
/*
 * Define for LookupDevice
 */
#define LOOKUP_NAME 1
#define LOOKUP_FD 2
#define LOOKUP_TYPE 3
#define LOOKUP_CONFIG 4
/* 
 * Define for the return codes from SenseHandler
 */
#define SENSE_ABORT -1
#define SENSE_IGNORE 0
#define SENSE_RETRY 2
#define SENSE_IES 3
#define SENSE_TAPE_NOT_ONLINE 4
#define SENSE_TAPE_NOT_LOADED 5
#define SENSE_NO 6
#define SENSE_TAPE_NOT_UNLOADED 7
#define SENSE_CHM_FULL 8
/*
 * Defines for the type field in the inquiry command
 */
#define TYPE_DISK 0
#define TYPE_TAPE 1
#define TYPE_PRINTER 2
#define TYPE_PROCESSOR 3
#define TYPE_WORM 4
#define TYPE_CDROM 5
#define TYPE_SCANNER 6
#define TYPE_OPTICAL 7
#define TYPE_CHANGER 8
#define TYPE_COMM 9

/* Defines for Tape_Status */
#define TAPE_ONLINE 1        /* Tape is loaded */
#define TAPE_BOT 2           /* Tape is at begin of tape */
#define TAPE_EOT 4           /* Tape is at end of tape */
#define TAPE_WR_PROT 8       /* Tape is write protected */
#define TAPE_NOT_LOADED 16   /* Tape is not loaded */

/* Defines for the function Tape_Ioctl */
#define IOCTL_EJECT 0

/* Defines for exit status */
#define WARNING 1
#define FATAL	2

/* macros for building scsi msb array parameter lists */
#ifndef B
#define B(s,i) ((unsigned char)(((s) >> (i)) & 0xff))
#endif
#ifndef B1
#define B1(s)  ((unsigned char)((s) & 0xff))
#endif
#define B2(s)                       B((s),8),   B1(s)
#define B3(s)            B((s),16), B((s),8),   B1(s)
#define B4(s) B((s),24), B((s),16), B((s),8),   B1(s)

/* macros for converting scsi msb array to binary */
#define V1(s)           (s)[0]
#define V2(s)         (((s)[0] << 8) | (s)[1])
#define V3(s)       (((((s)[0] << 8) | (s)[1]) << 8) | (s)[2])
#define V4(s)     (((((((s)[0] << 8) | (s)[1]) << 8) | (s)[2]) << 8) | (s)[3])
#define V5(s)   (((((((((s)[0] << 8) | (s)[1]) << 8) | (s)[2]) << 8) | (s)[3]) << 8) | (s)[4])
#define V6(s) (((((((((((s)[0] << 8) | (s)[1]) << 8) | (s)[2]) << 8) | (s)[3]) << 8) | (s)[4]) << 8) | (s)[5])

/* macros for converting binary into scsi msb array */
#define MSB1(s,v)                                                (s)[0]=B1(v)
#define MSB2(s,v)                                 (s)[0]=B(v,8), (s)[1]=B1(v)
#define MSB3(s,v)                 (s)[0]=B(v,16), (s)[1]=B(v,8), (s)[2]=B1(v)
#define MSB4(s,v) (s)[0]=B(v,24), (s)[1]=B(v,16), (s)[2]=B(v,8), (s)[3]=B1(v)

#define LABEL_DB_VERSION 2

#define DEBUG_INFO 9
#define DEBUG_ERROR 1
#define DEBUG_ALL 0

#define SECTION_ALL 0
#define SECTION_INFO 1
#define SECTION_SCSI 2
#define SECTION_MAP_BARCODE 3
#define SECTION_ELEMENT 4
#define SECTION_BARCODE 5
#define SECTION_TAPE 6
#define SECTION_MOVE 7
/*----------------------------------------------------------------------------*/
/* Some stuff for our own configurationfile */
typedef struct {  /* The information we can get for any drive (configuration) */
  int	drivenum;    /* Which drive to use in the library */
  int	start;       /* Which is the first slot we may use */
  int	end;         /* The last slot we are allowed to use */
  int	cleanslot;   /* Where the cleaningcartridge stays */
  char *scsitapedev; /* Where can we send raw SCSI commands to the tape */
  char *device;      /* Which device is associated to the drivenum */
  char *slotfile;    /* Where we should have our memory */   
  char *cleanfile;   /* Where we count how many cleanings we did */
  char *timefile;    /* Where we count the time the tape was used*/
  char *tapestatfile;/* Where can we place some drive stats */
  char *changerident;/* Config to use for changer control, ovverride result from inquiry */
  char *tapeident;   /* Same as above for the tape device */
}config_t; 

typedef struct {
  int number_of_configs; /* How many different configurations are used */
  int eject;             /* Do the drives need an eject-command */
  int autoinv;           /* Do automaticly an inventory if an tape is not in the db or not active in the db */
  int havebarcode;       /* Do we have an barcode reader installed */
  char *debuglevel;      /* How many debug info to print */
  unsigned char emubarcode;	/* Emulate the barcode feature,  used for keeping an inventory of the lib */
  time_t sleep;          /* How many seconds to wait for the drive to get ready */
  int cleanmax;          /* How many runs could be done with one cleaning tape */
  char *device;          /* Which device is our changer */
  char *labelfile;       /* Mapping from Barcode labels to volume labels */
  config_t *conf;
}changer_t;

typedef struct {
  char voltag[128];
  char barcode[TAG_SIZE];
  unsigned char valid;
} LabelV1_T;

typedef struct {
  char voltag[128];              /* Tape volume label */
  char barcode[TAG_SIZE];        /* Barcode of the tape */
  int slot;                      /* in which slot is the tape */
  int from;                      /* from where it comes, needed to move a tape
				  * back to the right slot from the drive
				  */

  unsigned int LoadCount;	/* How many times has the tape been loaded */
  unsigned int RecovError;	/* How many recovered errors */
  unsigned int UnrecovError;	/* How man unrecoverd errors */
  unsigned char valid;		/* Is this tape in the current magazin */
} LabelV2_T;

typedef enum {BARCODE_PUT, BARCODE_VOL, BARCODE_BARCODE, BARCODE_DUMP, RESET_VALID, FIND_SLOT, UPDATE_SLOT } MBCAction_T;
typedef struct {
  LabelV2_T data;
  MBCAction_T action;
} MBC_T;


/* ======================================================= */
/* RequestSense_T */
/* ======================================================= */
typedef struct  
{
#ifdef LITTLE_ENDIAN_BITFIELDS 
    PackedBit     ErrorCode:7;                    /* Byte 0 Bits 0-6 */
    PackedBit     Valid:1;                              /* Byte 0 Bit 7    */
#else 
    PackedBit     Valid:1;                              /* Byte 0 Bit 7    */
    PackedBit     ErrorCode:7;                    /* Byte 0 Bits 0-6 */
#endif
    unsigned char SegmentNumber;                  /* Byte 1 */ 
#ifdef LITTLE_ENDIAN_BITFIELDS        
    PackedBit     SenseKey:4;                     /* Byte 2 Bits 0-3 */
    PackedBit     :1;                             /* Byte 2 Bit 4    */
    PackedBit     RILI:1;                                /* Byte 2 Bit 5    */
    PackedBit     REOM:1;                                /* Byte 2 Bit 6    */
    PackedBit     Filemark:1;                           /* Byte 2 Bit 7    */
#else 
    PackedBit     Filemark:1;                           /* Byte 2 Bit 7    */
    PackedBit     REOM:1;                                /* Byte 2 Bit 6    */
    PackedBit     RILI:1;                                /* Byte 2 Bit 5    */
    PackedBit     :1;                             /* Byte 2 Bit 4    */ 
    PackedBit     SenseKey:4;                     /* Byte 2 Bits 0-3 */
#endif
    unsigned char Information[4];                 /* Bytes 3-6       */ 
    unsigned char AdditionalSenseLength;          /* Byte 7          */   
    unsigned char CommandSpecificInformation[4];  /* Bytes 8-11      */ 
    unsigned char AdditionalSenseCode;            /* Byte 12         */
    unsigned char AdditionalSenseCodeQualifier;   /* Byte 13         */ 
    unsigned char Byte14;                          /* Byte 14         */ 
    unsigned char Byte15;                           /* Byte 15         */ 
    
} RequestSense_T;     

/* ======================================================= */
/* ExtendedRequestSense_T */
/* ======================================================= */
typedef struct  
{
#ifdef LITTLE_ENDIAN_BITFIELDS 
    PackedBit     ErrorCode:7;                    /* Byte 0 Bits 0-6 */
    PackedBit     Valid:1;                        /* Byte 0 Bit 7    */
#else 
    PackedBit     Valid:1;                        /* Byte 0 Bit 7    */
    PackedBit     ErrorCode:7;                    /* Byte 0 Bits 0-6 */
#endif
    unsigned char SegmentNumber;                  /* Byte 1 */ 
#ifdef LITTLE_ENDIAN_BITFIELDS        
    PackedBit     SenseKey:4;                     /* Byte 2 Bits 0-3 */
    PackedBit     :1;                             /* Byte 2 Bit 4    */
    PackedBit     RILI:1;                         /* Byte 2 Bit 5    */
    PackedBit     REOM:1;                         /* Byte 2 Bit 6    */
    PackedBit     Filemark:1;                     /* Byte 2 Bit 7    */
#else 
    PackedBit     Filemark:1;                     /* Byte 2 Bit 7    */
    PackedBit     REOM:1;                         /* Byte 2 Bit 6    */
    PackedBit     RILI:1;                         /* Byte 2 Bit 5    */
    PackedBit     :1;                             /* Byte 2 Bit 4    */ 
    PackedBit     SenseKey:4;                     /* Byte 2 Bits 0-3 */
#endif
    unsigned char Information[4];                 /* Bytes 3-6       */ 
    unsigned char AdditionalSenseLength;          /* Byte 7          */   
    unsigned char LogParameterPageCode;           /* Bytes 8         */ 
    unsigned char LogParameterCode;               /* Bytes 9         */ 
    unsigned char Byte10;                         /* Bytes 10        */ 
    unsigned char UnderrunOverrunCounter;         /* Bytes 11        */ 
    unsigned char AdditionalSenseCode;            /* Byte 12         */
    unsigned char AdditionalSenseCodeQualifier;   /* Byte 13         */ 
    unsigned char Byte14;                         /* Byte 14         */ 
    unsigned char Byte15;                         /* Byte 15         */ 
    unsigned char ReadWriteDataErrorCounter[3];   /* Byte 16-18      */ 
#ifdef LITTLE_ENDIAN_BITFIELDS        
    PackedBit     LBOT:1;                         /* Byte 19 Bits 0 */
    PackedBit     TNP:1;                          /* Byte 19 Bits 1 */
    PackedBit     TME:1;                          /* Byte 19 Bits 2 */
    PackedBit     ECO:1;                          /* Byte 19 Bits 3 */
    PackedBit     ME:1;                           /* Byte 19 Bits 4 */
    PackedBit     FPE:1;                          /* Byte 19 Bits 5 */
    PackedBit     BPE:1;                          /* Byte 19 Bits 6 */
    PackedBit     PF:1;                           /* Byte 19 Bits 7 */
#else 
    PackedBit     PF:1;                           /* Byte 19 Bits 7 */
    PackedBit     BPE:1;                          /* Byte 19 Bits 6 */
    PackedBit     FPE:1;                          /* Byte 19 Bits 5 */
    PackedBit     ME:1;                           /* Byte 19 Bits 4 */
    PackedBit     ECO:1;                          /* Byte 19 Bits 3 */
    PackedBit     TME:1;                          /* Byte 19 Bits 2 */
    PackedBit     TNP:1;                          /* Byte 19 Bits 1 */
    PackedBit     LBOT:1;                         /* Byte 19 Bits 0 */
#endif
#ifdef LITTLE_ENDIAN_BITFIELDS        
    PackedBit     FE:1;                           /* Byte 20 Bits 0 */
    PackedBit     SSE:1;                          /* Byte 20 Bits 1 */
    PackedBit     WEI:1;                          /* Byte 20 Bits 2 */
    PackedBit     URE:1;                          /* Byte 20 Bits 3 */
    PackedBit     FMKE:1;                         /* Byte 20 Bits 4 */
    PackedBit     WP:1;                           /* Byte 20 Bits 5 */
    PackedBit     TMD:1;                          /* Byte 20 Bits 6 */
    PackedBit     :1;                             /* Byte 20 Bits 7 */
#else 
    PackedBit     :1;                             /* Byte 20 Bits 7 */
    PackedBit     TMD:1;                          /* Byte 20 Bits 6 */
    PackedBit     WP:1;                           /* Byte 20 Bits 5 */
    PackedBit     FMKE:1;                         /* Byte 20 Bits 4 */
    PackedBit     URE:1;                          /* Byte 20 Bits 3 */
    PackedBit     WEI:1;                          /* Byte 20 Bits 2 */
    PackedBit     SSE:1;                          /* Byte 20 Bits 1 */
    PackedBit     FE:1;                           /* Byte 20 Bits 0 */
#endif
#ifdef LITTLE_ENDIAN_BITFIELDS        
    PackedBit     WSEO:1;                         /* Byte 21 Bits 0 */
    PackedBit     WSEB:1;                         /* Byte 21 Bits 1 */
    PackedBit     PEOT:1;                         /* Byte 21 Bits 2 */
    PackedBit     CLN:1;                          /* Byte 21 Bits 3 */
    PackedBit     CLND:1;                         /* Byte 21 Bits 4 */
    PackedBit     RRR:1;                          /* Byte 21 Bits 5 */
    PackedBit     UCLN:1;                         /* Byte 21 Bits 6 */
    PackedBit     :1;                             /* Byte 21 Bits 7 */
#else 
    PackedBit     :1;                             /* Byte 21 Bits 7 */
    PackedBit     UCLN:1;                         /* Byte 21 Bits 6 */
    PackedBit     RRR:1;                          /* Byte 21 Bits 5 */
    PackedBit     CLND:1;                         /* Byte 21 Bits 4 */
    PackedBit     CLN:1;                          /* Byte 21 Bits 3 */
    PackedBit     PEOT:1;                         /* Byte 21 Bits 2 */
    PackedBit     WSEB:1;                         /* Byte 21 Bits 1 */
    PackedBit     WSEO:1;                         /* Byte 21 Bits 0 */
#endif
    unsigned char Byte21;                          /* Byte 22         */ 
    unsigned char RemainingTape[3];                /* Byte 23-25      */ 
    unsigned char TrackingRetryCounter;            /* Byte 26         */ 
    unsigned char ReadWriteRetryCounter;           /* Byte 27         */ 
    unsigned char FaultSymptomCode;                /* Byte 28         */ 
    
} ExtendedRequestSense_T;     

/* ======================================================= */
/*  ReadElementStatus_T */
/* ======================================================= */
typedef struct 
{
    unsigned char cmd;                           /* Byte 1 */
#ifdef LITTLE_ENDIAN_BITFIELDS        
    PackedBit     type : 4;
    PackedBit     voltag :1;
    PackedBit     lun :3;
#else
    PackedBit     lun :3;
    PackedBit     voltag :1;
    PackedBit     type : 4;
#endif
    unsigned char start[2];                    /* Byte 3-4 */
    unsigned char number[2];                   /* Byte 5-6 */
    unsigned char byte4;                       /* Byte 7 */
    unsigned char length[4];                   /* Byte 8-11 */
    unsigned char byte78[2];                   /* Byte 12-13 */
} ReadElementStatus_T;

/* ======================================================= */
/* ElementStatusPage_T */
/* ======================================================= */
typedef struct 
{
    unsigned char type;     /* Byte 1 = Element Type Code*/
#ifdef LITTLE_ENDIAN_BITFIELDS        
    PackedBit     bitres  : 6;
    PackedBit     avoltag : 1;
    PackedBit     pvoltag : 1;
#else
    PackedBit     pvoltag : 1;
    PackedBit     avoltag : 1;
    PackedBit     bitres  : 6;
#endif
    unsigned char length[2];    /* Byte 2-3  = Element Descriptor Length */
    unsigned char byte4;        /* Byte 4 */
    unsigned char count[3];     /* Byte 5-7 = Byte Count of Descriptor Available */
} ElementStatusPage_T;


/* ======================================================= */
/* ElementStatusData_T */
/* ======================================================= */
typedef struct 
{
    unsigned char first[2];    /* Byte 1-2 = First Element Adress Reported */
    unsigned char number[2];   /* Byte 3-4 = Number of Elements Available */
    unsigned char byte5;      /* Reserved */
    unsigned char count[3];     /* Byte 6-8 = Byte Count of Report Available */
} ElementStatusData_T;

/* ======================================================= */
/* MediumTransportElementDescriptor_T */
/* ======================================================= */
typedef struct 
{
    unsigned char address[2];   /* Byte 1-2 = Element Address */
#ifdef LITTLE_ENDIAN_BITFIELDS        
    PackedBit     full   : 1;
    PackedBit     rsvd   : 1;
    PackedBit     except : 1;
    PackedBit     res    : 5;
#else
    PackedBit     res    : 5;
    PackedBit     except : 1;
    PackedBit     rsvd   : 1;
    PackedBit     full   : 1;
#endif
    unsigned char byte4;        /* Byte 4      */
    unsigned char asc;          /* Byte 5 ASC  */
    unsigned char ascq;         /* Byte 6 ASCQ */
    unsigned char byte79[3];    /* Byte 7-9    */
#ifdef LITTLE_ENDIAN_BITFIELDS        
    PackedBit     byte10res : 6;
    PackedBit     invert : 1;
    PackedBit     svalid : 1;
#else
    PackedBit     svalid : 1;
    PackedBit     invert : 1;
    PackedBit     byte10res : 6;
#endif
    unsigned char source[2];
  unsigned char pvoltag[36];
  unsigned char res4[4];
} MediumTransportElementDescriptor_T;

/* ======================================================= */
/* ImportExportElementDescriptor_T */
/* ======================================================= */
typedef struct 
{
  unsigned char address[2];   /* Byte 1 = Element Address */
#ifdef LITTLE_ENDIAN_BITFIELDS        
  PackedBit     full   : 1;
  PackedBit     impexp : 1;
  PackedBit     except : 1;
  PackedBit     access : 1;
  PackedBit     exenab : 1;
  PackedBit     inenab : 1;
  PackedBit     res    : 2;
#else
  PackedBit     res    : 2;
  PackedBit     inenab : 1;
  PackedBit     exenab : 1;
  PackedBit     access : 1;
  PackedBit     except : 1;
  PackedBit     rsvd   : 1;
  PackedBit     full   : 1;
#endif
    unsigned char byte4;
    unsigned char asc;
    unsigned char ascq;
    unsigned char byte79[3];
#ifdef LITTLE_ENDIAN_BITFIELDS        
    PackedBit     byte10res : 6;
    PackedBit     invert : 1;
    PackedBit     svalid : 1;
#else
    PackedBit     svalid : 1;
    PackedBit     invert : 1;
    PackedBit     byte10res : 6;
#endif
    unsigned char source[2];
  unsigned char pvoltag[36];
  unsigned char res4[4];
  unsigned char mediadomain[1];
  unsigned char mediatype[1];
  unsigned char res5[2];
} ImportExportElementDescriptor_T;

/* ======================================================= */
/* StorageElementDescriptor_T */
/* ======================================================= */
typedef struct 
{
    unsigned char address[2];
#ifdef LITTLE_ENDIAN_BITFIELDS        
    PackedBit     full   : 1;
    PackedBit     rsvd   : 1;
    PackedBit     except : 1;
    PackedBit     access : 1;
    PackedBit     res    : 4;
#else
    PackedBit     res    : 4;
    PackedBit     access : 1;
    PackedBit     except : 1;
    PackedBit     rsvd   : 1;
    PackedBit     full   : 1;
#endif
    unsigned char res1;
    unsigned char asc;
    unsigned char ascq;
    unsigned char res2[3];
#ifdef LITTLE_ENDIAN_BITFIELDS        
    PackedBit     res3   : 6;
    PackedBit     invert : 1;
    PackedBit     svalid : 1;
#else
    PackedBit     svalid : 1;
    PackedBit     invert : 1;
    PackedBit     res3   : 6;
#endif
    unsigned char source[2];
  unsigned char pvoltag[36];
  unsigned char res4[4];
  unsigned char mediadomain[1];
  unsigned char mediatype[1];
  unsigned char res5[2];
} StorageElementDescriptor_T;

/* ======================================================= */
/* DataTransferElementDescriptor_T */
/* ======================================================= */
typedef struct 
{
    unsigned char address[2];
#ifdef LITTLE_ENDIAN_BITFIELDS        
    PackedBit     full    : 1;
    PackedBit     rsvd    : 1;
    PackedBit     except  : 1;
    PackedBit     access  : 1;
    PackedBit     res     : 4;
#else
    PackedBit     res     : 4;
    PackedBit     access  : 1;
    PackedBit     except  : 1;
    PackedBit     rsvd    : 1;
    PackedBit     full    : 1;
#endif
    unsigned char res1;
    unsigned char asc;
    unsigned char ascq;
#ifdef LITTLE_ENDIAN_BITFIELDS        
    PackedBit     lun     : 3;
    PackedBit     rsvd1   : 1;
    PackedBit     luvalid : 1;
    PackedBit     idvalid : 1;
    PackedBit     rsvd2   : 1;
    PackedBit     notbus  : 1;
#else
    PackedBit     notbus  : 1;
    PackedBit     rsvd2   : 1;
    PackedBit     idvalid : 1;
    PackedBit     luvalid : 1;
    PackedBit     rsvd1   : 1;
    PackedBit     lun     : 3;
#endif
    unsigned char scsi;
    unsigned char res2;
#ifdef LITTLE_ENDIAN_BITFIELDS        
    PackedBit     res3    : 6;
    PackedBit     invert  : 1;
    PackedBit     svalid  : 1;
#else
    PackedBit     svalid  : 1;
    PackedBit     invert  : 1;
    PackedBit     res3    : 6;
#endif
    unsigned char source[2];
  unsigned char pvoltag[36];
    unsigned char res4[42];
} DataTransferElementDescriptor_T;

/* ======================================================= */
/* SCSIInquiry_T */
/* ======================================================= */
typedef struct
{
#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit     type : 5;
    PackedBit     qualifier : 3;

    PackedBit     type_modifier : 7;
    PackedBit     removable : 1;

    PackedBit     ansi_version : 3;
    PackedBit     ecma_version : 3;
    PackedBit     iso_version : 2;

    PackedBit     data_format : 4;
    PackedBit     res3_54 : 2;
    PackedBit     termiop : 1;
    PackedBit     aenc : 1;
#else
    PackedBit     qualifier : 3;
    PackedBit     type : 5;
  
    PackedBit     removable : 1;
    PackedBit     type_modifier : 7;
  
    PackedBit     iso_version : 2;
    PackedBit     ecma_version : 3;
    PackedBit     ansi_version : 3;
  
    PackedBit     aenc : 1;
    PackedBit     termiop : 1;
    PackedBit     res3_54 : 2;
    PackedBit     data_format : 4;
#endif
  
    unsigned char add_len;
  
    unsigned char  res2;
    unsigned char res3;
  
#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit     softreset : 1;
    PackedBit     cmdque : 1;
    PackedBit     res7_2 : 1;
    PackedBit     linked  : 1;
    PackedBit     sync : 1;
    PackedBit     wbus16 : 1;
    PackedBit     wbus32 : 1;
    PackedBit     reladr : 1;
#else
    PackedBit     reladr : 1;
    PackedBit     wbus32 : 1;
    PackedBit     wbus16 : 1;
    PackedBit     sync : 1;
    PackedBit     linked  : 1;
    PackedBit     res7_2 : 1;
    PackedBit     cmdque : 1;
    PackedBit     softreset : 1;
#endif
    char vendor_info[8];
    char prod_ident[16];
    char prod_version[4];
    char vendor_specific[20];
} SCSIInquiry_T;

/* ======================================================= */
/* ModeSenseHeader_T */
/* ======================================================= */
typedef struct
{
    unsigned char DataLength;
    unsigned char MediumType;
#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit Speed:4;
    PackedBit BufferedMode:3;
    PackedBit WP:1;
#else
    PackedBit WP:1;
    PackedBit BufferedMode:3;
    PackedBit Speed:4;
#endif
    unsigned char BlockDescLength;
} ModeSenseHeader_T;
/* ======================================================= */
/* ModeBlockDescriptor_T */
/* ======================================================= */
typedef struct 
{
    unsigned char DensityCode;
    unsigned char NumberOfBlocks[3];
    unsigned char Reserved;
    unsigned char BlockLength[3];
} ModeBlockDescriptor_T;
/* ======================================================= */
/* LogSenseHeader_T */
/* ======================================================= */
typedef struct 
{
#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit PageCode:6;
    PackedBit Reserved:2;
#else
    PackedBit Reserved:2;
    PackedBit PageCode:6;
#endif
    unsigned char Reserved1;
    unsigned char PageLength[2];
} LogSenseHeader_T ;
/* ======================================================= */
/* LogParameters_T */
/* ======================================================= */
typedef struct
{
    unsigned char ParameterCode[2];
#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit LP:1;
    PackedBit RSCD:1;
    PackedBit TMC:1;
    PackedBit ETC:1;
    PackedBit TSD:1;
    PackedBit DS:1;
    PackedBit DU:1;
#else
    PackedBit DU:1;
    PackedBit DS:1;
    PackedBit TSD:1;
    PackedBit ETC:1;
    PackedBit TMC:1;
    PackedBit RSCD:1;
    PackedBit LP:1;
#endif
    char ParameterLength;
} LogParameter_T;
/*
 * Pages returned by the MODE_SENSE command
 */
typedef struct {
    unsigned char SenseDataLength;
    char res[3];
} ParameterListHeader_T;
/* ======================================================= */
/* ReadWriteErrorRecoveryPage_T */
/* ======================================================= */
typedef struct 
{
#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit PageCode : 6;
    PackedBit res      : 1;
    PackedBit PS       : 1;
#else
    PackedBit PS       : 1;
    PackedBit res      : 1;
    PackedBit PageCode : 6;
#endif
    unsigned char ParameterListLength;
#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit dcr  :1;  /* Disable ECC Correction */
    PackedBit dte  :1;  /* Disable Transfer on Error */
    PackedBit per  :1;  /* Enable Post  Error reporting */
    PackedBit eer  :1;  /* Enable early recovery */
    PackedBit res1 :1;
    PackedBit tb   :1;  /* Transfer block (when not fully recovered) */
    PackedBit res2 :1;
    PackedBit res3 :1;
#else
    PackedBit res3 :1;
    PackedBit res2 :1;
    PackedBit tb   :1;
    PackedBit res1 :1;
    PackedBit eer  :1;
    PackedBit per  :1;
    PackedBit dte  :1;
    PackedBit dcr  :1;
#endif
    unsigned char ReadRetryCount;
    unsigned char res4[4];
    unsigned char WriteRetryCount;
    unsigned char res5[3];
} ReadWriteErrorRecoveryPage_T; 
/* ======================================================= */
/* EDisconnectReconnectPage_T */
/* ======================================================= */
typedef struct 
{
#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit PageCode : 6;
    PackedBit RSVD     : 1;
    PackedBit PS       : 1;
#else
    PackedBit PS       : 1;
    PackedBit RSVD     : 1;
    PackedBit PageCode : 6;
#endif

    unsigned char BufferFullRatio;
    unsigned char BufferEmptyRatio;
    unsigned char BusInactivityLimit[2];
    unsigned char DisconnectTimeLimit[2];
    unsigned char ConnectTimeLimit[2];
    unsigned char MaximumBurstSize[2];

#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit DTDC :2;
    PackedBit res  :6;
#else
    PackedBit res  :6;
    PackedBit DTDC :2;
#endif
    unsigned char res1[3];
} DisconnectReconnectPage_T;

/* ======================================================= */
/* EAAPage_T */
/* ======================================================= */
typedef struct 
{
#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit PageCode : 6;
    PackedBit RSVD     : 1;
    PackedBit PS       : 1;
#else
    PackedBit PS       : 1;
    PackedBit RSVD     : 1;
    PackedBit PageCode : 6;
#endif
    unsigned char ParameterListLength;
    unsigned char MediumTransportElementAddress[2];
    unsigned char NoMediumTransportElements[2];
    unsigned char FirstStorageElementAddress[2];
    unsigned char NoStorageElements[2];
    unsigned char FirstImportExportElementAddress[2];
    unsigned char NoImportExportElements[2];
    unsigned char FirstDataTransferElementAddress[2];
    unsigned char NoDataTransferElements[2];
    unsigned char res[2];
} EAAPage_T;    
/* ======================================================= */
/* TransPortGeometryDescriptorPage_T */
/* ======================================================= */
typedef struct {
#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit PageCode : 6;
    PackedBit RSVD     : 1;
    PackedBit PS       : 1;
#else
    PackedBit PS       : 1;
    PackedBit RSVD     : 1;
    PackedBit PageCode : 6;
#endif
    unsigned char ParameterListLength;
#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit Rotate    : 1;
    PackedBit res       : 7;
#else
    PackedBit res       : 7;
    PackedBit Rotate    : 1;
#endif
    unsigned char MemberNumber;
} TransportGeometryDescriptorPage_T;  
/* ======================================================= */
/* DeviceCapabilitiesPage_T */
/* ======================================================= */
typedef struct
{
#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit PageCode : 6;
    PackedBit RSVD     : 1;
    PackedBit PS       : 1;
#else
    PackedBit PS       : 1;
    PackedBit RSVD     : 1;
    PackedBit PageCode : 6;
#endif
    unsigned char ParameterLength;
#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit MT        : 1;
    PackedBit ST        : 1;
    PackedBit IE        : 1;
    PackedBit DT        : 1;
    PackedBit res1      : 4;
#else
    PackedBit res1      : 4;
    PackedBit DT        : 1;
    PackedBit IE        : 1;
    PackedBit ST        : 1;
    PackedBit MT        : 1;
#endif
    unsigned char res;
#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit MT2MT     : 1;
    PackedBit MT2ST     : 1;
    PackedBit MT2IE     : 1;
    PackedBit MT2DT     : 1;
    PackedBit res2      : 4;
#else
    PackedBit res2      : 4;
    PackedBit MT2DT     : 1;
    PackedBit MT2IE     : 1;
    PackedBit MT2ST     : 1;
    PackedBit MT2MT     : 1;
#endif

#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit ST2MT     : 1;
    PackedBit ST2ST     : 1;
    PackedBit ST2IE     : 1;
    PackedBit ST2DT     : 1;
    PackedBit res3      : 4;
#else
    PackedBit res3      : 4;
    PackedBit ST2DT     : 1;
    PackedBit ST2IE     : 1;
    PackedBit ST2ST     : 1;
    PackedBit ST2MT     : 1;
#endif

#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit IE2MT     : 1;
    PackedBit IE2ST     : 1;
    PackedBit IE2IE     : 1;
    PackedBit IE2DT     : 1;
    PackedBit res4      : 4;
#else
    PackedBit res4      : 4;
    PackedBit IE2DT     : 1;
    PackedBit IE2IE     : 1;
    PackedBit IE2ST     : 1;
    PackedBit IE2MT     : 1;
#endif

#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit DT2MT     : 1;
    PackedBit DT2ST     : 1;
    PackedBit DT2IE     : 1;
    PackedBit DT2DT     : 1;
    PackedBit res5      : 4;
#else
    PackedBit res5      : 4;
    PackedBit DT2DT     : 1;
    PackedBit DT2IE     : 1;
    PackedBit DT2ST     : 1;
    PackedBit DT2MT     : 1;
#endif
    unsigned char res0819[12];
} DeviceCapabilitiesPage_T;  
/* ======================================================= */
/* ModePageEXB10hLCD_T */
/* ======================================================= */
typedef struct ModePageEXB10hLCD
{
  unsigned char PageCode;
  unsigned char ParameterListLength;

#ifdef LITTLE_ENDIAN_BITFIELDS
  PackedBit WriteLine4 : 1;
  PackedBit WriteLine3 : 1;
  PackedBit WriteLine2 : 1;
  PackedBit WriteLine1 : 1;
  PackedBit res        : 4;
#else
  PackedBit res        : 4;
  PackedBit WriteLine1 : 1;
  PackedBit WriteLine2 : 1;
  PackedBit WriteLine3 : 1;
  PackedBit WriteLine4 : 1;
#endif
  unsigned char reserved;
  unsigned char line1[20];
  unsigned char line2[20];
  unsigned char line3[20];
  unsigned char line4[20];
} ModePageEXB10hLCD_T;
/* ======================================================= */
/* ModePageEXBBaudRatePage_T */
/* ======================================================= */
typedef struct ModePageEXBBaudRatePage
{
  unsigned char PageCode;
  unsigned char ParameterListLength;
  unsigned char BaudRate[2];
} ModePageEXBBaudRatePage_T;
/* ======================================================= */
/* ModePageEXB120VendorUnique_T */
/* ======================================================= */
typedef struct ModePageEXB120VendorUnique
{
#ifdef  LITTLE_ENDIAN_BITFIELDS
    PackedBit PageCode : 6;
    PackedBit RSVD0    : 1;
    PackedBit PS       : 1;
#else
    PackedBit PS       : 1;
    PackedBit RSVD0    : 1;
    PackedBit PageCode : 6;
#endif
    unsigned char ParameterListLength;
#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit MDC  : 2;
    PackedBit NRDC : 1;
    PackedBit RSVD : 1;
    PackedBit NBL  : 1;
    PackedBit PRTY : 1;
    PackedBit UINT : 1;
    PackedBit AINT : 1;
#else
    PackedBit AINT : 1;
    PackedBit UINT : 1;
    PackedBit PRTY : 1;
    PackedBit NBL  : 1;
    PackedBit RSVD : 1;
    PackedBit NRDC : 1;
    PackedBit MDC  : 2;
#endif
    unsigned char MaxParityRetries;
    unsigned char DisplayMessage[60];
} ModePageEXB120VendorUnique_T;
/* ======================================================= */
/* ModePageTreeFrogVendorUnique_T */
/* ======================================================= */
typedef struct ModePageTreeFrogVendorUnique
{
#ifdef  LITTLE_ENDIAN_BITFIELDS
    PackedBit PageCode : 6;
    PackedBit res0     : 1;
    PackedBit PS       : 1;
#else
    PackedBit PS       : 1;
    PackedBit res0     : 1;
    PackedBit PageCode : 6;
#endif
    unsigned char ParameterListLength;
#ifdef LITTLE_ENDIAN_BITFIELDS
    PackedBit EBARCO  : 1;
    PackedBit CHKSUM  : 1;
    PackedBit res2    : 6;
#else
    PackedBit res2    : 6;
    PackedBit CHKSUM  : 1;
    PackedBit EBARCO  : 1;
#endif
    unsigned char res3;
    unsigned char res4;
    unsigned char res5;
    unsigned char res6;
    unsigned char res7;
    unsigned char res8;
    unsigned char res9;
} ModePageTreeFrogVendorUnique_T;
/* ======================================================= */
/* ElementInfo_T */
/* ======================================================= */
typedef struct ElementInfo
{
    int type;       /* CHANGER - 1, STORAGE - 2, TAPE - 4 */
    int address;    /* Adress of this Element */
    int from;       /* From where did it come */
    char status;    /* F -> Full, E -> Empty */
    char VolTag[TAG_SIZE+1]; /* Label Info if Barcode reader exsist */
    unsigned char ASC;  /* Additional Sense Code from read element status */
    unsigned char ASCQ; /* */
    unsigned char scsi; /* if DTE, which scsi address */

  PackedBit svalid : 1;
  PackedBit invert : 1;
  PackedBit full   : 1;
  PackedBit impexp : 1;
  PackedBit except : 1;
  PackedBit access : 1;
  PackedBit inenab : 1;
  PackedBit exenab : 1;

} ElementInfo_T;



typedef struct {
    char *ident;                  /* Name of the device from inquiry */
    char *type;                   /* Device Type, tape|robot */
    int (*function_move)(int, int, int);
    int (*function_status)(int, int);
    int (*function_reset_status)(int);
    int (*function_free)(void);
    int (*function_eject)(char *, int);
    int (*function_clean)(char *);
    int (*function_rewind)(int);
    int (*function_barcode)(int);
    int (*function_search)(void);
    int (*function_error)(int, unsigned char, unsigned char, unsigned char, unsigned char, RequestSense_T *);
} ChangerCMD_T ;

typedef struct {
    unsigned char command;        /* The SCSI command byte */
    int length;                   /* How long */
    char *name;                   /* Name of the command */
} SC_COM_T;

typedef struct OpenFiles {
    int fd;                       /* The filedescriptor */
#ifdef HAVE_CAM_LIKE_SCSI
    struct cam_device *curdev;
#endif
  unsigned char avail;          /* Is this device available */
  unsigned char devopen;        /* Is the device open */
  unsigned char inqdone;        /* Did we try to get device infos, was an open sucessfull */
  unsigned char SCSI;           /* Can we send SCSI commands */
  int flags;                    /* Can be used for some flags ... */
  char *dev;                    /* The device which is used */
  char *type;			/* Type of device, tape/changer */
  char *ConfigName;             /* The name in the config */
  char ident[17];               /* The identifier from the inquiry command */
  ChangerCMD_T *functions;      /* Pointer to the function array for this device */
  SCSIInquiry_T *inquiry;       /* The result from the Inquiry */
} OpenFiles_T;

typedef struct LogPageDecode {
    int LogPage;
    char *ident;
    void (*decode)(LogParameter_T *, size_t);
} LogPageDecode_T;

typedef struct {
   char *ident;        /* Ident as returned from the inquiry */
   char *vendor;       /* Vendor as returned from the inquiry */
   unsigned char type; /* removable .... */
   unsigned char sense;/* Sense key as returned from the device */
   unsigned char asc;  /* ASC as set in the sense struct */
   unsigned char ascq; /* ASCQ as set in the sense struct */
   int  ret;           /* What we think that we should return on this conditon */
   char text[80];      /* A short text describing this condition */
} SenseType_T;                                                                                    

/* ======================================================= */
/* Function-Declaration */
/* ======================================================= */
int SCSI_OpenDevice(int);
int OpenDevice(int, char *DeviceName, char *ConfigName, char *ident);

int SCSI_CloseDevice(int DeviceFD); 
int CloseDevice(char *, int); 
int Tape_Eject(int);
int Tape_Status(int);
void DumpSense(void);
int Sense2Action(char *ident,
			unsigned char type,
			unsigned char ignsense,
			unsigned char sense,
			unsigned char asc,
			unsigned char ascq,
			char **text);

int SCSI_ExecuteCommand(int DeviceFD,
                        Direction_T Direction,
                        CDB_T CDB,
                        size_t CDB_Length,
                        void *DataBuffer,
                        size_t DataBufferLength,
                        RequestSense_T *RequestSense,
                        size_t RequestSenseLength);

int Tape_Ioctl(int DeviceFD, int command);
void ChangerStatus(char * option,
			char * labelfile,
			int HasBarCode,
			char *changer_file,
			char *changer_dev,
			char *tape_device);

int SCSI_Inquiry(int, SCSIInquiry_T *, size_t);
int PrintInquiry(SCSIInquiry_T *);
int DecodeSCSI(CDB_T CDB, char *string);

int RequestSense(int fd, ExtendedRequestSense_T *s, int ClearErrorCounters);
int DecodeSense(RequestSense_T *sense, char *pstring, FILE *out);
int DecodeExtSense(ExtendedRequestSense_T *sense, char *pstring, FILE *out);

void ChgExit(char *, char *, int);

void ChangerReplay(char *option);
void ChangerStatus(char *option, char *labelfile, int HasBarCode, char *changer_file, char *changer_dev, char *tape_device);
int BarCode(int fd);
int MapBarCode(char *labelfile, MBC_T *);

int Tape_Ready(int fd, time_t wait_time);

void Inventory(char *labelfile, int drive, int eject, int start, int stop, int clean);
void ChangerDriverVersion(void);
void PrintConf(void);
int LogSense(int fd);
int ScanBus(int print);
void DebugPrint(int level, int section, char * fmt, ...);
int DecodeSense(RequestSense_T *sense, char *pstring, FILE *out);
void SCSI_OS_Version(void);
int get_clean_state(char *tapedev);
int find_empty(int fd, int start, int count);
int get_slot_count(int fd);
int get_drive_count(int fd);
int GetCurrentSlot(int fd, int drive);
void DumpDev(OpenFiles_T *p, char *device);
int isempty(int fd, int slot);

#endif	/* !SCSIDEFS_H */
/*
 * Local variables:
 * indent-tabs-mode: nil
 * c-file-style: gnu
 * End:
 */
