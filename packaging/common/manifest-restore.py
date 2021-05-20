#!/opt/zmanda/amanda/python/bin/python3.6
import os
from os.path import realpath
import stat
import sys
import rpmfile
import pathlib
import ctypes
import subprocess
import argparse

from pwd import getpwnam
from grp import getgrnam

"""
    XX "filestates": 1029,
    ? "fileuids": 1031,
    ? "filegids": 1032,
    ? "filerdevs": 1033,
    XX "filemtimes": 1034,   # use only outside of python!
    ? "filedigests": x....

    * "filenames": 1027,
    * "filesizes": 1028,
    * "filemodes": 1030,
    * "filelinktos": 1036,
    * "fileflags": 1037,
       "root": 1038,
    * "fileusername": 1039,
    * "filegroupname": 1040,

From RPM includes ...
       1          - MD5                                    "MD5"
       2          - SHA-1                                  "SHA1"
       3          - RIPE-MD/160                            "RIPEMD160"
       4          - Reserved for double-width SHA (experimental)
       5          - MD2                                    "MD2"
       6          - Reserved for TIGER/192                 "TIGER192"
       7          - Reserved for HAVAL (5 pass, 160-bit)    "HAVAL-5-160"
       8          - SHA-256                                "SHA256"
       9          - SHA-384                                "SHA384"
       10         - SHA-512                                "SHA512"
       11         - SHA-224                                "SHA224"
       100 to 110 - Private/Experimental algorithm.
"""

RPMFILE_CONFIG     = 2**0    # from %config
RPMFILE_DOC        = 2**1    # from %doc
RPMFILE_ICON       = 2**2    # from %donotuse.
RPMFILE_MISSINGOK  = 2**3    # from %config(missingok)
RPMFILE_NOREPLACE  = 2**4    # from %config(noreplace)
RPMFILE_SPECFILE   = 2**5    # @todo (unnecessary) marks 1st file in srpm.
RPMFILE_GHOST      = 2**6    # from %ghost
RPMFILE_LICENSE    = 2**7    # from %license
RPMFILE_README     = 2**8    # from %readme
RPMFILE_PUBKEY     = 2**11   # from %pubkey

opt_canOutput = False
opt_clrSymlinks = False
opt_confSave = False
opt_stubOnly = False
manifName = ''

def handle_argv_opts():
    global opt_canOutput
    global opt_clrSymlinks
    global opt_stubOnly
    global opt_confSave
    global manifName

    if len(sys.argv) < 2:
        print("Usage: [ --clr-symlinks ] [ --stub-only ] /opt/zmanda/amanda/pkg-manifests/<manifest-file>",file=sys.stderr)
        sys.exit(-1)

    p = argparse.ArgumentParser()
    p.add_argument('--clr-symlinks',action='store_true')
    p.add_argument('--stub-only',action='store_true')
    p.add_argument('--conf-save',action='store_true')
    p.add_argument('args',nargs='+')
    p = p.parse_args(sys.argv[1:])

    # allow for pipe outputs as needed...
    opt_canOutput = sys.stdin.isatty() or sys.stdout.isatty()
    opt_clrSymlinks = p.clr_symlinks
    opt_stubOnly    = p.stub_only
    opt_confSave    = p.conf_save
    manifName       = p.args[0]

class ManifestStub:

    def __init__(self,fstr):
        self._manifpath = pathlib.PurePath(realpath(fstr))

        if str(self._manifpath.parent) != '/opt/zmanda/amanda/pkg-manifests':
            raise SystemExit('must read file in /opt/zmanda/amanda/pkg-manifests only:' + str(self._manifpath.parent))

        self._rpm = rpmfile.open(self._manifpath)

        if not self._rpm:
            raise SystemExit('could not open rpm: ' + self._manifpath)

        self._name = self._rpm.headers.get('name').decode()

        self._files = self._rpm.headers.get('basenames')
        self._dirs = self._rpm.headers.get('dirindexes')
        self._dirindex = self._rpm.headers.get('dirnames')
        self._flags = self._rpm.headers.get('fileflags')

        self._modes = self._rpm.headers.get('filemodes') # ( modes[n] & 0o170000 ),( modes[n] & 0o7777 )
        self._sizes = self._rpm.headers.get('filesizes')
        self._slinks = self._rpm.headers.get('filelinktos')
        self._owners = self._rpm.headers.get('fileusername')
        self._groups = self._rpm.headers.get('filegroupname')
        self._mtimes = self._rpm.headers.get('filemtimes')
        self._digests = self._rpm.headers.get('filedigests')
        self._digests = ( self._digests if self._digests else self._rpm.headers.get('filemd5s') )

        self.nfiles = len(self._files)

        # if len(self._flags) != self.nfiles: raise SystemExit('flags array malformed: ' + name)
        # if len(self._modes) != self.nfiles: raise SystemExit('modes array malformed: ' + name)
        # if len(self._sizes) != self.nfiles: raise SystemExit('sizes array malformed: ' + name)
        # if len(self._owners) != self.nfiles: raise SystemExit('owners array malformed: ' + name)
        # if len(self._groups) != self.nfiles: raise SystemExit('groups array malformed: ' + name)
        # if len(self._mtimes) != self.nfiles: raise SystemExit('mtimes array malformed: ' + name)
        # if len(self._digests) != self.nfiles: raise SystemExit('digests array malformed: ' + name)

        self._digestsSuffix = {1: ".md5sums", 8:".sha256sums"}.get(self._rpm.headers.get('filedigestalgo'),".digests")

        self._ownerMap = {b'root': 0}
        self._groupMap = {b'root': 0}

        for i in set(self._owners):
            try:
                ent = getpwnam(i.decode())
                self._ownerMap[i] = ent[2]
            except:
                self._ownerMap[i] = 0

        for i in set(self._groups):
            try:
                ent = getgrnam(i.decode())
                self._groupMap[i] = ent[2]
            except:
                self._groupMap[i] = 0



class ManifestFile:
    def __init__(self,manif,n,dirfd=None):
        self._manif = manif
        self._n = n
        self._dirToClose = None
        self._dirfd = None
        try:
            if ( dirfd >= 0 ):
                self._dirfd = dirfd
            else:
                self._dirfd = os.open(self.fdir_bstr(), os.O_RDONLY)
                self._dirToClose = self._dirfd
        except:
            self._dirfd = None

    def __del__(self):
        if self._dirToClose != None:
            os.close(self._dirToClose)

    def my_fpath(self):
        return self.fname() if self._dirfd != None else self.fpath()
    def my_fpath_bstr(self):
        return self.fname_bstr() if self._dirfd != None else self.fpath_bstr()

    def ispresent(self):
        return os.access(self.fpath_bstr(),os.F_OK,dir_fd=self._dirfd)
    def issym(self):
        return stat.S_IFMT(self._manif._modes[self._n] & 0xffff) == stat.S_IFLNK
    def isreg(self):
        return stat.S_ISREG(self._manif._modes[self._n] & 0xffff)
    def isconfig(self):
        return self._manif._flags[self._n] & RPMFILE_CONFIG
    def isinstalled(self):
        return (self._manif._flags[self._n] & (RPMFILE_GHOST|RPMFILE_MISSINGOK)) == 0
    def fname(self):
        return  self._manif._files[self._n].decode()
    def fname_bstr(self):
        return  self._manif._files[self._n]
    def fdir(self):
        return  self._manif._dirindex[self._manif._dirs[self._n]].decode()
    def fdir_bstr(self):
        return  self._manif._dirindex[self._manif._dirs[self._n]]
    def fpath(self):
        return self.fdir() + self.fname()
    def fpath_bstr(self):
        return self.fdir_bstr() + self.fname_bstr()
    def fdir_idx(self):
        return self._manif._dirs[self._n]
    def fmode(self):
        return stat.S_IMODE(self._manif._modes[self._n] & 0xffff)
    def ftype(self):
        return stat.S_IFMT(self._manif._modes[self._n] & 0xffff)
    def lpath(self):
        return self._manif._slinks[self._n].decode()
    def lpath_bstr(self):
        return self._manif._slinks[self._n]
    def uid(self):
        return self._manif._ownerMap[self._manif._owners[self._n]]
    def gid(self):
        return self._manif._groupMap[self._manif._groups[self._n]]
    def verify_string(self,st):
        # * S file Size differs
        # * M Mode differs (includes permissions and file type)
        # X 5 digest (formerly MD5 sum) differs
        # X D Device major/minor number mismatch
        # * L readLink(2) path mismatch
        # * U User ownership differs
        # * G Group ownership differs
        # X T mTime differs
        # X P caPabilities differ
        return "{size}{mode}{digests}.{link}{uid}{gid}{mtime}.".format(
              size=   ( 'S' if ( self.isreg() \
                                 and self.isinstalled() \
                                 and self._manif._sizes[self._n] != st.st_size ) else '.' ),
              mode=   ( 'M' if ( not self.issym() and self.isinstalled() and self.fmode() != stat.S_IMODE(st.st_mode) ) else '.' ),
              digests=( '?' if ( self.isreg() \
                                 and self.isinstalled() \
                                 and self._manif._sizes[self._n] == st.st_size ) else '.' ),
              link=   ( 'L' if ( self.issym() \
                                 and self.isinstalled() \
                                 and self.get_lstat_match() \
                                 and self.lpath() != os.readlink(self.fpath()) ) else '.' ),
              uid=    ( 'U' if ( self.uid() != st.st_uid ) else '.' ),
              gid=    ( 'G' if ( self.gid() != st.st_gid ) else '.' ),
              mtime=  ( 'T' if ( self.isreg() and self.isinstalled() \
                                 and self._manif._sizes[self._n] == st.st_size \
                                 and self._manif._mtimes[self._n] != st.st_mtime \
                                 and not self.fname().endswith('.log')) else '.' ),
           )
    def get_lstat(self):
        return os.lstat(self.my_fpath_bstr(), dir_fd=self._dirfd)
    def get_lstat_match(self):
        if self.issym() and os.path.islink(self.fpath()):
            return True
        if not os.path.exists(self.fpath()):
            return False
        t = stat.S_IFMT(self.get_lstat().st_mode & 0xffff)
        return t == self.ftype()

    def do_chown(self):
        os.chown(self.my_fpath_bstr(), self.uid(), self.gid(), dir_fd=self._dirfd, follow_symlinks=False)
    def do_sha256_check(self):
        if not ( self.isreg() and self.isinstalled() ):
            return True
        return not subprocess.run(["/usr/bin/sha256sum","--quiet","-c","-"], check=False, \
                                   input=(self._manif._digests[self._n] + b" " + self.fpath_bstr())).returncode
    def do_unlink(self):
        try:
            os.unlink( self.my_fpath_bstr(), dir_fd=self._dirfd)
        except:
            pass

    def do_symlink(self):
        self.do_unlink()
        os.symlink( self.lpath_bstr(), self.my_fpath_bstr(), dir_fd=self._dirfd)

    def do_chmod(self):
        os.chmod( self.my_fpath_bstr(), self.fmode(), dir_fd=self._dirfd)  # already ignores symlinks

    def do_utime(self,atime):
        os.utime( self.my_fpath_bstr(), ( atime, self._manif._mtimes[self._n] ), dir_fd=self._dirfd)  # already ignores symlinks


handle_argv_opts()

manif = ManifestStub(manifName)

# quick traversal to clear any symlinks
if opt_clrSymlinks:
    for n in range(0,manif.nfiles):
        f = ManifestFile(manif,n)
        try:
            if not f.get_lstat_match():
                if not os.path.exists(f.fpath()):
                    continue
                os.rename(f.fpath(), "{}.{}.cfgsave".format(f.fpath(),os.getpid()))
                continue
            if not f.issym():
                continue
            f.do_unlink()
        except Exception as e:
            print("ERROR: failed to unlink: {} with {}".format(f.fpath(),str(e)),file=sys.stderr)
            raise e

# protect the config files that are out of place
for n in range(0,manif.nfiles):
    f = ManifestFile(manif,n)
    if not f.isconfig():
        continue
    try:
        nm = f.fpath()
        oldcnf = os.path.isfile(nm)
        savecnf = os.path.isfile(nm + ".cfgsave")
        if opt_confSave:
            if not oldcnf:
                print("NOTE: no previous conf file {}".format(nm),file=sys.stderr)
                continue
            if savecnf:
               os.unlink(nm + ".cfgsave")
            os.rename(nm, nm + ".cfgsave")
            print("NOTE: preserved file as {}".format(nm + ".cfgsave"),file=sys.stderr)
            continue
        if savecnf:
            if oldcnf:
               os.unlink(nm)
            os.rename(nm + ".cfgsave", nm)
            print("NOTE: restored file from {}".format(nm + ".cfgsave"),file=sys.stderr)
            continue
    except:
        pass

#
#
# NOTE: for debian stubs only...
#
#
try:
    name = manif._name.replace('_','-')                   # make safe-for-debian
    proc = subprocess.run(["dpkg-query","-c",name], check=False, stderr=subprocess.PIPE, stdout=subprocess.PIPE);
    l = str(proc.stdout.decode()).partition("\n")[0]
    i = l.rfind(".") + 1
    l = l[0:i] + "list"   #file list

    lastline='/'
    line=''
    with open(l) as flist:
        lastline='/'
        # detect a single file only .. built from directories
        for line in flist:
            line = line.strip()

            # must keep extending from last line
            if not os.path.samefile(os.path.dirname(line), lastline):
                raise Exception('no stub install found')
            lastline = line

    # not a correct stub install?
    if lastline != line or line != str(manif._manifpath):
        raise Exception('no stub install found')

    # append all files into the /var/lib/dpkg/info database
    stublist = open(l,'a')  # writes to nothing if not a debian stub
    stubcfgs = open(l.replace('.list','.conffiles'),'w')  # write to nothing if not a debian stub
    stubdigests = open(l.replace('.list',manif._digestsSuffix),'a')  # write to nothing if not a debian stub
    for n in range(0,manif.nfiles):
        f = ManifestFile(manif,n)
        nm = f.fpath()

        print(nm,file=stublist)

        if f.isconfig():
            print(nm,file=stubcfgs)

        ##### OOPS ####### DO NOT back up old config files ...
        # Because the unpack is used on install and bitrock-install.
        # It will do it ACTUALLY correctly (apparently with the conffiles list)

        print("{0} {1}".format(manif._digests[n].decode(),nm[1:]),file=stubdigests) \
           if (len(manif._digests[n]) > 0) else None
except:
    pass    # writing config files is not needed


# just skip the rest ...
if opt_stubOnly or opt_confSave:
    sys.exit(0)

vfyarray = [''] * manif.nfiles
nmarray = [''] * manif.nfiles
#
#
# NOTE: if files really exist ...
#
#
curdir = -1
dirfd = None
f = None
for n in range(0,manif.nfiles):
    # init
    st = os.stat_result
    f = ManifestFile(manif,n,dirfd)
    if curdir != f.fdir_idx():
        try:
            # TODO: check inode of old vs. new dirs
            if dirfd != None and dirfd >= 0:
                os.close(dirfd)
            f = ManifestFile(manif,n)
            dirfd = f._dirfd
            f._dirToClose = None
            curdir = f.fdir_idx()
            curdirNm = f.fdir()
        except:
            print("WARNING: {0:<9} {1}".format("d-error",curdirNm),file=sys.stderr) \
                if f.isinstalled() and opt_canOutput else 0
            dirfd = None
            curdir = -1
            continue

    nm = curdirNm + f.fname()

    try:
        if f.isreg() and not f.isinstalled() and not f.ispresent():
            with open(nm,'xb') as created:
               pass
            f.do_chown()
            f.do_chmod()
            continue
        st = f.get_lstat()
    except FileNotFoundError:
        if os.geteuid() == 0 and f.issym():
            f.do_symlink()
            print("INSTALL: {:<9} {} -> {}".format("new-sym",nm, f.lpath()),file=sys.stderr) \
                if f.isinstalled() and opt_canOutput else 0
        elif os.geteuid() == 0 and f.ftype() == stat.S_IFDIR:
            os.makedirs(f.fpath(), mode=f.fmode(), exist_ok=True)
            f.do_chown()
        else:
            # missing with no recourse..
            print("WARNING: {0:<9} {1}".format("missing",nm),file=sys.stderr) \
                if f.isinstalled() and opt_canOutput else 0
        continue

    except IOError as e:
        if os.geteuid() == 0 and f.issym():
            f.do_symlink()
            print("WARNING: {:<9} {} -> {}".format("io-sym",nm, f.lpath()),file=sys.stderr) \
                if f.isinstalled() and opt_canOutput else 0
        else:
            print("WARNING: {0:<9} {1}".format("error",nm),file=sys.stderr) \
                if f.isinstalled() and opt_canOutput else 0
        continue

    except:
        raise

    vfy = f.verify_string(st)

#
# perform real fixes now
#    order: chown, symlink-create, chmod, utime-set
#
    try:
        if os.geteuid() == 0:
            if ( vfy.find('U') > 0 or vfy.find('G') > 0 ):
                f.do_chown()

            if vfy.find('L') > 0:
                f.do_symlink()
                print("INSTALL: {:<9} {} -> {}".format("fix-sym",curdirNm + f.fname(), f.lpath()),file=sys.stderr) \
                    if f.isinstalled() and opt_canOutput else 0

            # MUST do *AFTER* others to preserve sticky bits..
            if vfy.find('M') > 0:
                f.do_chmod()

            if vfy.find('T') > 0:
                f.do_utime(st.st_atime)

        #print("{0} {1}".format(vfy,nm)) if opt_canOutput else 0
    except:
        print("WARNING: {0} {1}".format("failed",nm),file=sys.stderr) if opt_canOutput else 0

    nmarray[n] = nm
    vfyarray[n] = vfy


if not opt_canOutput:
    sys.exit(0)

for n in range(0,manif.nfiles):
    vfy = vfyarray[n]
    nm = nmarray[n]
    if vfy == '..?......':
        continue
    if vfy == '.........':
        continue

    print("{vfy} {cfg} {nm}".format(
          vfy=vfy,
          cfg=( 'c' if f.isconfig() else 'g' if not f.isinstalled() else ' '),
          nm=nm
        ))
