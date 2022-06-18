#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

# https://www.openssl.org/docs/man1.1.1/man1/req.html

readonly NAME='redis-rb'
readonly EXPIRATION_DAYS='109500'
readonly SBJ='/C=US/ST=Georgia/L=Atlanta/O=redis-rb/OU=redis-cluster-client/CN=127.0.0.1'
readonly SAN1='localhost'
readonly SAN2='endpoint.example.com'
readonly CERT_DIR="$(dirname $(realpath $0))/../test/ssl_certs"
readonly WORK_DIR=$(mktemp -d)

cd ${WORK_DIR}
mkdir -p ${CERT_DIR} ./demoCA ./demoCA/certs ./demoCA/crl ./demoCA/newcerts ./demoCA/private
touch ./demoCA/index.txt
rm -f ${CERT_DIR}/*

openssl genpkey\
  -algorithm RSA\
  -pkeyopt rsa_keygen_bits:2048\
  -out ./${NAME}-ca.key

openssl req\
  -new\
  -x509\
  -days ${EXPIRATION_DAYS}\
  -key ./${NAME}-ca.key\
  -sha256\
  -out ${CERT_DIR}/${NAME}-ca.crt\
  -subj "${SBJ}"\
  -addext "subjectAltName = DNS:${SAN1},DNS:${SAN2}"

openssl x509\
  -in ${CERT_DIR}/${NAME}-ca.crt\
  -noout\
  -next_serial\
  -out ./demoCA/serial

openssl req\
  -newkey rsa:2048\
  -keyout ${CERT_DIR}/${NAME}-cert.key\
  -nodes\
  -out ./${NAME}-cert.req\
  -subj "${SBJ}"\
  -addext "subjectAltName = DNS:${SAN1},DNS:${SAN2}"

openssl ca\
  -days ${EXPIRATION_DAYS}\
  -cert ${CERT_DIR}/${NAME}-ca.crt\
  -keyfile ./${NAME}-ca.key\
  -out ${CERT_DIR}/${NAME}-cert.crt\
  -infiles ./${NAME}-cert.req

chmod 644 -R ${CERT_DIR}/*
