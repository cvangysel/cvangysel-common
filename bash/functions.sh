set -e

check_installed() {
    command -v $1 >/dev/null 2>&1 || { echo >&2 "Required tool '$1' is not installed. Aborting."; exit 1; }
}

package_root() {
    git rev-parse --show-toplevel
}

#
# Array-related functions.
#

# From http://stackoverflow.com/questions/3685970/check-if-an-array-contains-a-value.
function contains() {
    local n=$#
    local value=${!n}
    for ((i=1;i < $#;i++)) {
        if [ "${!i}" == "${value}" ]; then
            echo "y"
            return 0
        fi
    }
    echo "n"
    return 1
}

#
# Value comparison.
#

check_eq() {
    check_not_empty "${1:-}" "first argument"
    check_not_empty "${2:-}" "second argument"

    if [[ "${1}" != "${2}" ]]; then
        echo "Value '${1}' is not equal to '${2}'."

        exit -1
    fi
}

#
# Integral.
#

check_int() {
    if [[ ! "$1" =~ ^-?[0-9]+?$ ]] ; then
        echo "String $1 is not a float."
        exit -1
    fi
}

int_compare() {
    if [[ "$1" < "$2" ]]; then
        echo "$1 is smaller than $2."
        exit -1
    fi
}

#
# Floating point.
#

check_float() {
    if [[ ! "$1" =~ ^-?[0-9]+([.][0-9]+)?(e-?[0-9]+)?$ ]] ; then
        echo "String $1 is not a float."
        exit -1
    fi
}

float_compare() {
    awk -v n1=$1 -v n2=$2 'BEGIN{ if (n1<n2) print 0; print 1}'
}

max() {
    check_float "${1:-}"
    check_float "${2:-}"
    echo "${1:-}" | awk "{if (\$0 > ${2:-}) {print} else {print ${2:-}}}"
}

#
# String validation.
#

check_not_empty() {
    if [[ -z "$1" ]]; then
        echo "Received empty string instead of $2."
        exit -1
    fi
}

check_valid_option() {
    if [[ $(contains "$@") != "y" ]]; then
        local n=$#
        local value=${!n}

        printf "Option '%s' is not valid (allowed:" "${value}"
        for ((i=1;i < $#;i++)) {
            printf " %s" "${!i}"
        }
        printf ").\n"

        exit -1
    fi
}

#
# File system.
#

check_file() {
    if [[ ! -f "$1" ]]; then
        echo "File $1 does not exist."
        exit -1
    fi
}

check_file_not_exists() {
    if [[ -f "$1" ]]; then
        echo "File $1 already exists."
        exit -1
    fi
}

check_directory() {
    if [[ ! -d "$1" ]]; then
        echo "Directory $1 does not exist."
        exit -1
    fi
}

check_directory_not_exists() {
    if [[ -d "$1" ]]; then
        echo "Directory $1 already exists."
        exit -1
    fi
}

directory_md5sum() {
    DIRECTORY="${1:-}"
    check_not_empty "${DIRECTORY}" "m5sum directory"
    check_directory "${DIRECTORY}"

    CURRENT_DIR=$(pwd)

    cd "${DIRECTORY}" && find . -type f -exec md5sum {} \; \
        | awk '{print $2 " " $1}' \
        | sort -k 1

    cd "${CURRENT_DIR}"
}