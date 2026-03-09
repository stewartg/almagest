#!/bin/bash

MIN_PYTHON_VERSION=""
PYTHON_VERSION=""
# Initialize variables to store the argument values
dev_flag=false
test_flag=false
build_flag=false
deploy_flag=false
all_flag=false
EDITABLE=""
VERSION_VAR=""
GREEN=$'\e[0;32m'
YELLOW=$'\e[0;33m'
RED=$'\e[0;31m'
ORANGE=$'\e[38;5;208m'
NC=$'\e[0m' # No Color

# Make sure the project root is set correctly whether this script is run from the
# scripts directory or not. Also set the directory for the virtual environment to
# be created in
setup_script_start() {
    # Change to the directory where the script is located
    cwd="$(pwd)"
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    # Determine the project root directory based on the script's location
    PROJECT_ROOT="$SCRIPT_DIR/.."
    VENV_DIR="$PROJECT_ROOT/venv"
    # Navigate to the project root directory
    cd "$PROJECT_ROOT" || return 1
    if get_min_python_version; then
        if verify_python_version; then
            create_virtual_environment
            activate_virtual_environment
            install_packages
        fi
    fi
    # Change back to the start directory
    cd "$cwd" || return 1
}

# Attempts to retrieve the minimum python version from the pyproject.toml file.
get_min_python_version() {
    if [ ! -f "pyproject.toml" ]; then
        echo "Error: Attempting to retrieve minimun python version failed because pyproject.toml not found."
        return 1
    fi

    # Extract the minimum Python version using grep and sed
    MIN_PYTHON_VERSION=$(grep -oP 'requires-python = ">=[\d.]+"' pyproject.toml | sed -e 's/requires-python = ">=//' -e 's/"//')

    if [ -z "$MIN_PYTHON_VERSION" ]; then
        echo "Error: Attempting to retrieve minimun python velarsion failed because the 'requires-python' key was not found in pyproject.toml."
        return 1
    fi
}

# Display usage information
usage() {
    echo "Usage: source $0 [-v|--version] [-d|--dev]"
    echo ""
    echo "Sets up the development environment using the specified Python version or the system default python version."
    echo " 'source' ing this script will ensure the virtual environment is activated properly."
    echo ""
    echo "Options:"
    echo "  [-h|--help]: Display this help message and exit. [default]"
    echo "  [-v|--version] 3.11: The version of the python virtual environment to install. If not provided,"
    echo "        the script will attempt to use system default python version."
    echo "  [-b|--build]: Install build dependencies. [default]"
    echo "  [-d|--dev]: Install development dependencies in editable mode."
    echo "  [-y|--deploy]: Install deployment dependencies."
    echo "  [-t|--test]: Install development dependencies for test (non-editable)."
    echo "  [-a|--all]: Install all dependencies (non-editable)."
    echo ""
}

if [[ -z "$BASH_SOURCE" || "$BASH_SOURCE" == "$0" ]]; then
    echo "Script should not be executed directly"
    usage
    exit 1
fi

# Check if minimum python3 is installed and exit if not. If it is, check if a version
# argument was supplied and verify its greater than or equal to the minimum version
# and that its installed. If a version argument is not supplied, get the latest installed python3 version.
verify_python_version() {
    if ! command -v python3 &> /dev/null; then
        echo "${RED}Python 3 could not be found. Please install at least Python $MIN_PYTHON_VERSION first.${NC}"
        return 1
    fi

    if [ -n "$VERSION_VAR" ]; then
        get_python_version "$VERSION_VAR" "$MIN_PYTHON_VERSION"
    else
        py_path_version=$(find /usr/bin -type f -name 'python3.[0-9]*' | sort -V | tail -1)
        if [ -z "$py_path_version" ]; then
            echo "${RED}No suitable Python version found in /usr/bin. Please install at least Python $MIN_PYTHON_VERSION first.${NC}"
            return 1
        else
            # Extract the version number from the path
            read py_version _ <<< $(echo "$py_path_version" | grep -oP '\d+\.\d+')
            get_python_version "$py_version" "$MIN_PYTHON_VERSION"
        fi
    fi
}

# Create a virtual environment if it doesn't exist as 'venv' in the VENV_DIR directory
create_virtual_environment() {
    if [ ! -d "$VENV_DIR" ]; then
        echo -e "${GREEN}Creating virtual environment using python $PYTHON_VERSION in $VENV_DIR...${NC}"
        python${PYTHON_VERSION:-3} -m venv "$VENV_DIR"
    else
        echo "${YELLOW}Virtual environment already exists in $VENV_DIR. Skipping creation.${NC}"
    fi
}

activate_virtual_environment() {
    echo "${YELLOW}Activating virtual environment...${NC}"
    source "$VENV_DIR/bin/activate"
}

# Install required packages using pip including all dev packages
install_packages() {
    echo "${GREEN}Installing required Python packages...${NC}"
    sh scripts/set_pip_configuration.sh $VENV_DIR
    
    # Prevents the WARNING: There was an error checking the latest version of pip.
    if [ -d ~/.cache/pip/selfcheck/ ]; then
        rm -r ~/.cache/pip/selfcheck/
    fi

    pip install --no-cache-dir --upgrade pip

    dependencies=()

    if [[ "${all_flag}" == "true" ]]; then
        dependencies+=( "build" "dev" "deploy")
    else
        if [[ "${dev_flag}" == "true"  || "${test_flag}" == "true" ]]; then
            dependencies+=( "dev" )
        fi
        if [[ "${build_flag}" == "true" || "${deploy_flag}" == "true" ]]; then
            dependencies+=( "build" )
        fi
        if [[ "${deploy_flag}" == "true" ]]; then
            dependencies+=( "deploy" )
        fi

        # If no specific flags are set, default to 'build'
        if [ ${#dependencies[@]} -eq 0 ]; then
            dependencies=( "build" )
        fi
    fi

    do_pip_install "${dependencies[@]}"

    # Print a success message
    echo "${ORANGE}Enclave python environment setup complete.${NC}"
}

do_pip_install() {
    dependencies=("$@")

    # Join dependencies into a single comma-separated list
    dependency_str=$(IFS=,; echo "${dependencies[*]}")

    if [[ -n $EDITABLE ]]; then
        echo "${GREEN}Installing ${dependency_str} dependencies in${NC} ${ORANGE}editable mode${NC}"
    else
        echo "${GREEN}Installing ${dependency_str} dependencies...${NC}"
    fi

    pip install ${EDITABLE} .[${dependency_str}]

    if $dev_flag || $all_flag; then
        # Install pre-commit hooks only once after all dependencies are installed
        pre-commit install
    fi
}

# Compare 2 versions in the format 1.23.4 and return 1 if the supplied 2nd version
# argument is the version in the first position of the sorted list.
compare_versions() {
    local version1=$1
    local version2=$2
    local res="0"
    if [[ $(echo -e "$version1\n$version2" | sort -V | head -n 1) == "$version2" ]]; then
        # "$version1 is greater than or equal to $version2"
        res="1"
    fi
    echo $res
}

# Compare python versions and then determine if those versions are installed. If the supplied
# first version is installed, set the PYTHON_VERSION variable equal to it.
get_python_version() {
    local version1=$1
    local version2=$2
    compare_status=$(compare_versions "$version1" "$version2")
    if [ $compare_status -eq 0 ]; then
        echo "Python $version1 does not meet minimum python version requirement. Please install at least Python $MIN_PYTHON_VERSION first."
        return 1
    else
        read py_version _ <<< $(python${version1} --version | awk '{print $2}' | cut -d '.' -f 1,2)
        if [ ! -z "$py_version" ]; then
            PYTHON_VERSION="$py_version"
        else
            echo "Python $version1 was not found."
            return 1
        fi
    fi
}

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -v|--version)
            VERSION_VAR=$2
            shift 1 # past argument=value
            ;;
        -d|--dev)
            dev_flag=true
            EDITABLE="-e"
            shift 1 # past argument=value
            ;;
        -t|--test)
            test_flag=true
            shift 1 # past argument=value
            ;;
        -b|--build)
            build_flag=true
            shift 1
            ;;
        -y|--deploy)
            deploy_flag=true
            shift 1 # past argument=value
            ;;
        -a|--all)
            all_flag=true
            EDITABLE="-e"
            shift 1 # past argument=value
            ;;
        -*|--*=) # unsupported flags
            echo "Error: Unsupported flag $1" >&2
            usage
            return 1
            ;;
        *) # preserve positional arguments
            PARAMS="$PARAMS $1"
            shift
            ;;
    esac
done

setup_script_start
