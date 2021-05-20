#!/bin/bash

python_mod_name=$1
log_names=$2
pkg_prefix=${1:0:3}

/bin/ln -sf ../sites-available/${python_mod_name}.conf \
         @@BITROCK_INSTALLDIR@@/nginx/etc/nginx/sites-enabled/${python_mod_name}
/bin/touch @@AMANDA_LOGDIR@@/aee/${log_name}-error.log
/bin/chown @@AMANDAUSER@@:@@AMANDAGROUP@@ @@AMANDA_LOGDIR@@/$pkg_prefix/${log_name}-error.log
/bin/touch @@AMANDA_LOGDIR@@/aee/${log_name}.log
/bin/chown @@AMANDAUSER@@:@@AMANDAGROUP@@ @@AMANDA_LOGDIR@@/$pkg_prefix/${log_name}.log
/bin/touch @@AMANDA_LOGDIR@@/aee/${python_mod_name}.log
/bin/chown @@AMANDAUSER@@:@@AMANDAGROUP@@ @@AMANDA_LOGDIR@@/$pkg_prefix/${python_mod_name}.log

# open perms
find /opt/zmanda/amanda -name \*.pyc | 
   tar -cf %{AMANDAHOMEDIR}/.python-orig.${pkg_prefix}.tar -T -

find %{INSTALL_PYTHON_PKGS}/%{python_mod_name} -type d | xargs chown %{amanda_user}:%{amanda_group}
find %{INSTALL_PYTHON_PKGS}/%{python_mod_name} -type d | xargs chmod u+w

# leave the bytecode as is ..
export PYTHONDONTWRITEBYTECODE=dont

/bin/su - @@AMANDAUSER@@ -c '@@BITROCK_INSTALLDIR@@/bin/ae-service-manage makemigrations --merge'
/bin/su - @@AMANDAUSER@@ -c '@@BITROCK_INSTALLDIR@@/bin/ae-service-manage makemigrations'
/bin/su - @@AMANDAUSER@@ -c '@@BITROCK_INSTALLDIR@@/bin/ae-service-manage migrate --run-syncdb'

tar -xf %{AMANDAHOMEDIR}/.python-orig.${pkg_prefix}.tar -C /
rm -f %{AMANDAHOMEDIR}/.python-orig.${pkg_prefix}.tar

find %{INSTALL_PYTHON_PKGS}/%{python_mod_name} -type d | xargs chown root:root
find %{INSTALL_PYTHON_PKGS}/%{python_mod_name} -type d | xargs chmod u-w
