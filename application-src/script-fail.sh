#!@SHELL@

if [ "$1" = "support" ]; then
  echo "CONFIG YES"
  echo "HOST YES"
  echo "DISK YES"
  echo "MESSAGE-LINE YES"
  echo "EXECUTE-WHERE YES"
  echo "TIMESTAMP YES"
  exit 0
else
  echo "stderr error: " $1 >&2
  exit 1
fi
