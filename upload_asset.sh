#!/bin/bash

GITHUB_TOKEN=""

RELEASE_ID=`curl -H 'Cache-Control: no-cache' -s https://api.github.com/repos/Jrohy/mysqldump/releases/latest|grep id|awk 'NR==1{print $2}'|sed 's/,//'`

function uploadfile() {
  FILE=$1
  CTYPE=$(file -b --mime-type $FILE)

  sleep 1
  curl -H "Authorization: token ${GITHUB_TOKEN}" -H "Content-Type: ${CTYPE}" --data-binary @$FILE "https://uploads.github.com/repos/Jrohy/mysqldump/releases/${RELEASE_ID}/assets?name=$(basename $FILE)"
  sleep 1
}

function upload() {
  FILE=$1
  DGST=$1.dgst
  openssl dgst -md5 $FILE | sed 's/([^)]*)//g' >> $DGST
  openssl dgst -sha1 $FILE | sed 's/([^)]*)//g' >> $DGST
  openssl dgst -sha256 $FILE | sed 's/([^)]*)//g' >> $DGST
  openssl dgst -sha512 $FILE | sed 's/([^)]*)//g' >> $DGST
  uploadfile $FILE
  uploadfile $DGST
}

pushd `pwd`

go get github.com/mitchellh/gox

gox -output="result/mysqldump_{{.OS}}_{{.Arch}}" -ldflags="-s -w"

cd result

UPLOAD_ITEM=($(ls -l|awk '{print $9}'|xargs -r))

for ITEM in ${UPLOAD_ITEM[@]}
do
    upload $ITEM
done

popd

rm -rf result