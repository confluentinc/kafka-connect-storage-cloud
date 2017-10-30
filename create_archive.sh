#!/usr/bin/env bash
#
# Creates an archive suitable for distribution (standard layout for binaries,
# libraries, etc.).

set -e

if [ -z ${PACKAGE_TITLE} -o -z ${VERSION} -o -z ${DESTDIR} ]; then
    echo "PACKAGE_TITLE, VERSION, and DESTDIR environment variables must be set."
    exit 1
fi

BINPATH=${PREFIX}/bin
LIBPATH=${PREFIX}/share/java/${PACKAGE_TITLE}
DOCPATH=${PREFIX}/share/doc/${PACKAGE_TITLE}

INSTALL="install -D -m 644"
INSTALL_X="install -D -m 755"

rm -rf ${DESTDIR}${PREFIX}
mkdir -p ${DESTDIR}${PREFIX}
mkdir -p ${DESTDIR}${BINPATH}
mkdir -p ${DESTDIR}${LIBPATH}
mkdir -p ${DESTDIR}${SYSCONFDIR}

function copy_subpackage() {
    local SUBPACKAGE="$1"
    pushd "${SUBPACKAGE}/target/${SUBPACKAGE}-${VERSION}-package"
    find bin/ -type f | grep -v README[.]rpm | xargs -I XXX ${INSTALL_X} -o root -g root XXX ${DESTDIR}${PREFIX}/XXX
    find share/ -type f | grep -v README[.]rpm | xargs -I XXX ${INSTALL} -o root -g root XXX ${DESTDIR}${PREFIX}/XXX
    if [ -d etc/${PACKAGE_TITLE}/ ]; then
        pushd etc/${PACKAGE_TITLE}/
        find . -type f | grep -v README[.]rpm | xargs -I XXX ${INSTALL} -o root -g root XXX ${DESTDIR}${SYSCONFDIR}/XXX
        popd
    fi

    local major_version="$(echo ${VERSION} | cut -f 1 -d '.')"
    if [[ ${major_version} -ge 4 ]]; then
      pushd ${DESTDIR}${LIBPATH}
      ln -s ../kafka-connect-storage-common storage-common
      popd
    fi
    popd
}

case "${PACKAGE_TITLE}" in
  "kafka-connect-s3" | "kafka-connect-gcs" | "kafka-connect-azure")
    copy_subpackage ${PACKAGE_TITLE}
    ;;
  *)
    echo "Unexpected value for PACKAGE_TITLE environment variable found: ${PACKAGE_TITLE}. Expected one of
    kafka-connect-s3, kafka-connect-gcs, or kafka-connect-azure."
    exit 1
    ;;
esac
