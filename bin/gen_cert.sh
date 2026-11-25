#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

# https://docs.openssl.org/3.0/man1/openssl-req/
# https://docs.openssl.org/3.0/man1/openssl-x509/
#
# Requires OpenSSL 3.0 or later for `-copy_extensions`.
#
# The server certificate has to carry every name the clients dial in its
# Subject Alternative Name: TLS clients ignore the CN once a SAN is present,
# and redis-cli verifies the server identity by default since Redis 8.12.

readonly NAME='redis-rb'
readonly EXPIRATION_DAYS='109500'
readonly SBJ_BASE='/C=US/ST=Georgia/L=Atlanta/O=redis-rb/OU=redis-cluster-client'
readonly CA_SBJ="${SBJ_BASE}/CN=redis-rb-ca"
readonly CERT_SBJ="${SBJ_BASE}/CN=127.0.0.1"
readonly CERT_DIR="$(dirname "$(realpath "$0")")/../test/ssl_certs"
readonly WORK_DIR=$(mktemp -d)
readonly SAN_DNS=(localhost node1 node2 node3 node4 node5 node6 node7 node8 node9)
readonly SAN_IP=(127.0.0.1 ::1)

san_txt=''
for v in "${SAN_DNS[@]}"; do san_txt+="DNS:${v},"; done
for v in "${SAN_IP[@]}"; do san_txt+="IP:${v},"; done
readonly SAN_TXT="${san_txt%,}"

trap 'rm -rf "${WORK_DIR}"' EXIT
cd "${WORK_DIR}"
mkdir -p "${CERT_DIR}"
rm -f "${CERT_DIR}"/*

openssl genpkey\
  -algorithm RSA\
  -pkeyopt rsa_keygen_bits:2048\
  -out "./${NAME}-ca.key"

openssl req\
  -new\
  -x509\
  -days "${EXPIRATION_DAYS}"\
  -key "./${NAME}-ca.key"\
  -sha256\
  -out "${CERT_DIR}/${NAME}-ca.crt"\
  -subj "${CA_SBJ}"

openssl req\
  -newkey rsa:2048\
  -keyout "${CERT_DIR}/${NAME}-cert.key"\
  -nodes\
  -out "./${NAME}-cert.req"\
  -subj "${CERT_SBJ}"\
  -addext "subjectAltName = ${SAN_TXT}"

openssl x509\
  -req\
  -in "./${NAME}-cert.req"\
  -days "${EXPIRATION_DAYS}"\
  -sha256\
  -CA "${CERT_DIR}/${NAME}-ca.crt"\
  -CAkey "./${NAME}-ca.key"\
  -set_serial "0x$(openssl rand -hex 16)"\
  -copy_extensions copy\
  -out "${CERT_DIR}/${NAME}-cert.crt"

openssl verify\
  -CAfile "${CERT_DIR}/${NAME}-ca.crt"\
  "${CERT_DIR}/${NAME}-cert.crt"

chmod 644 "${CERT_DIR}"/*
