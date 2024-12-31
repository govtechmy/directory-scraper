#!/opt/homebrew/bin
# Setup file for ministry google apps scripts (GAS) directories

# Installing and setting up clasp
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
export NVM_DIR="$([ -z "${XDG_CONFIG_HOME-}" ] && printf %s "${HOME}/.nvm" || printf %s "${XDG_CONFIG_HOME}/nvm")"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"

nvm install npm
nvm install node
npm install -g @google/clasp

# Define cleanup handler, create temporary log directory
trap '[[ -n "$(jobs -p)" ]] && kill -- -$$; [[ -n "${logdir}" ]] && rm -rf "${logdir}"' EXIT
logdir=$(mktemp -d)

# Creating GAS directories
BASE_DIR="$(pwd)/sheet_scripts"
source "${BASE_DIR}/sheetconfig.sh" # Executes the contents of sheetconfig.sh (creates an array and assigns it to the variable SHEET_NAMES)
echo "Creating folders: ${SHEET_NAMES[@]}"

# Creating GAS directories and start specific clasp action for each project
max_jobs=25
poll_delay="0.1s"

declare -A pid_pro_map=() pid_log_map=()
for name in "${SHEET_NAMES[@]}"; do # Loops over array, creates directory with the array names and copies main.js into the new directories
    # Sleeps if the number of current processes exceedes the max_jobs value
    if (( ${max_jobs} > 0 )); then
        while jobs=$(jobs -r -p | wc -l) && (( ${jobs} >= ${max_jobs} )); do
            sleep "${poll_delay}" &> /dev/null
        done
    fi
    
    # Creates and moves into the directory
    mkdir -p "${BASE_DIR}/deployments/${name}" && cd "${BASE_DIR}/deployments/${name}"
    cp "${BASE_DIR}/ministry-template/main.js" "${BASE_DIR}/deployments/${name}/main.js"

    logfile=$(mktemp -p "${logdir}")
    # If project exists, skip the creation step and push the changes to remote
    if [ -e '.clasp.json' ]; then
        echo "Pushing code to sheet: Directory Gov - ${name}"
        clasp push -f && clasp push &> "${logfile}" &
    else
        # Runs clasp action and pipes the output to a temporary logfile
        (clasp create --type sheets --title "Directory Gov - ${name}") &> "${logfile}" &
        clasp push && clasp push # Double push to forcefully update remote

    fi
    # Deploy the GAS project
    clasp deploy --versionNumber $CODE_VERSION --description 'Initial version' &> "${logfile}" &

    # Takes the PID of the last program run in the shell and keeps them in mapps them to the sheet name and the logfile
    pid=$!; pid_pro_map[${pid}]="${name}"; pid_log_map[${pid}]="${logfile}"
    echo -e "Started clasp action for project '\e[1m${name}\e[0m' (pid ${pid})"

done

# Wait for background jobs to finish and report results
echo -e "\nWaiting for background jobs to finish...\n"
jobs_done=0; jobs_total=${#SHEET_NAMES[@]}
while true; do
    # Waits for PID to return exit code and assigns it to the variable `results`
    wait -n -p pid; result=$?
    # Checks if PID is empty, breaks if true (short-circuits)
    [[ -z "${pid}" ]] && break
    jobs_done=$((jobs_done + 1))

    # Echos out exit code of processes
    if (( ${result} == 0 )); then
        echo -e "Clasp action for project '\e[1m${pid_pro_map[${pid}]}\e[0m' (pid ${pid}) (${jobs_done}/${jobs_total}): \e[1;32mSUCCESS\e[0m"
    else
        echo -e "Clasp action for project '\e[1m${pid_pro_map[${pid}]}\e[0m' (pid ${pid}) (${jobs_done}/${jobs_total}): \e[1;31mFAILURE\e[0m"
        cat "${pid_log_map[${pid}]}"
    fi
done