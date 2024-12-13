### Installation
- Install `npm`
    - Windows (taken from (nodist repository)[https://github.com/nodists/nodist?tab=readme-ov-file]):
        1. Download the installer from the (release pages)[https://github.com/nodists/nodist/releases]
        2. Run the installer and follow the instructions

    - Linux, MacOS (taken from ()[])
        1. Run `curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash` or `wget -qO- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash` to install nvm
        2. Run ```
        export NVM_DIR="$([ -z "${XDG_CONFIG_HOME-}" ] && printf %s "${HOME}/.nvm" || printf %s "${XDG_CONFIG_HOME}/nvm")"
        [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh" # This loads nvm
        ```
### Runing (requires Bash 5.0+)
- Run setup with `sheet_scripts/gas_setup.sh`