#!/bin/bash

pkgs=(@@RPM_PACKAGE_LIST@@)

rpm -V ${pkgs[@]%.rpm} | grep -vi ' [a-z] /' || true

true
