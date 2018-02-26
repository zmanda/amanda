-- ==================================================== --
--	dbbackup.sql					--
--							--
--	This script is run as user "sys"		--
--							--
--	creates user "backup" identified externally	--
--	creates role "backup_role" with permisssions	--
--	grants role "backup_role" to user "backup"	--
--							--
-- ==================================================== --
connect internal

create role backup_role;
grant create session to backup_role;
grant alter system to backup_role;
grant alter database to backup_role;
grant manage tablespace to backup_role;
grant select on dba_tablespaces to backup_role;
grant select on dba_data_files to backup_role;
grant select on v_$log to backup_role;

create user backup identified externally;
grant backup_role to backup;
