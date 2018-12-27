#!/bin/bash

ODBCINST=/etc/odbcinst.ini
ODBC=/etc/odbc.ini
TDS=/etc/freetds/freetds.conf
DNSNAME=semehrdns
SVRNAME=semehrsvr

if [[ ! -f "$TDS" ]]; then
    echo "$TDS not found!"
    exit -1
fi

if [[ ! -f "$ODBC" ]]; then
    echo "$ODBC not found!"
    exit -1
fi

if [[ ! -f "$ODBCINST" ]]; then
    echo "$ODBCINST not found!"
    exit -1
fi

if cat "$TDS" | grep -q "$SVRNAME" ; then
	echo "server name already set, ignoring..."
else
	echo "" >> "$TDS"
	echo "setting server name in tds..."
	echo "[$SVRNAME]" >> "$TDS"
	echo "host = {host}" >> "$TDS"
	echo "port = {port}" >> "$TDS"
	echo "tds version = 7.0" >> "$TDS"
	echo "" >> "$TDS"
fi


if cat "$ODBCINST" | grep -q \\[FreeTDS\\] ; then
	echo 'FreeTDS exists, ignoring...'
else
	echo "setting FreeTDS..."
	echo "" >> "$ODBCINST"
	echo "[FreeTDS]" >> "$ODBCINST"
	echo "Description = TDS driver (Sybase/MS SQL)" >> "$ODBCINST"
	echo "Driver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so" >> "$ODBCINST"
	echo "Setup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so" >> "$ODBCINST"
	echo "CPTimeout =" >> "$ODBCINST"
	echo "CPReuse =" >> "$ODBCINST"
	echo "FileUsage = 1" >> "$ODBCINST"
	echo "" >> "$ODBCINST"
fi

if cat "$ODBC" | grep -q "$DNSNAME" ; then
	echo "dns name already set, ignoring..."
else
	echo "setting dns..."
	echo "" >> "$ODBC"
	echo "[$DNSNAME]" >> "$ODBC"
	echo "Driver = FreeTDS" >> "$ODBC"
	echo "Description = SemEHR ODBC connection via FreeTDS" >> "$ODBC"
	echo "Trace = No" >> "$ODBC"
	echo "Servername = $SVRNAME" >> "$ODBC"
	echo "Database={database}" >> "$ODBC"
	echo "" >> "$ODBC"
fi

echo "odbc setup done"
