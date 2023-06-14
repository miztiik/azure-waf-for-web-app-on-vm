#!/bin/bash
# set -ex
set -x
set -o pipefail

# version: 26May2023

##################################################
#############     SET GLOBALS     ################
##################################################

REPO_NAME="azure-web-server-vm"

GIT_REPO_URL="https://github.com/miztiik/$REPO_NAME.git"

APP_DIR="/var/$REPO_NAME"

LOG_FILE="/var/log/miztiik-automation-bootstrap-$(date +'%Y-%m-%d').log"

# https://learn.microsoft.com/en-us/azure/virtual-machines/linux/tutorial-automate-vm-deployment

instruction()
{
  echo "usage: ./build.sh package <stage> <region>"
  echo ""
  echo "/build.sh deploy <stage> <region> <pkg_dir>"
  echo ""
  echo "/build.sh test-<test_type> <stage>"
}

log_this() {
  # Calling this function like log_this "Begin installation" will result log like below
  # {"timestamp": "2023-04-15T10:22:23Z", "message": "Begin installation"}
    local timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    local message=$(printf '%s' "$*" | sed 's/"/\\"/g') # Escaping double quotes
    printf '{"timestamp": "%s", "message": "%s"}\n' "$timestamp" "$message"
}

assume_role() {
  if [ -n "$DEPLOYER_ROLE_ARN" ]; then
    echo "Assuming role $DEPLOYER_ROLE_ARN ..."
  fi
}

unassume_role() {
  unset TOKEN
}

function clone_git_repo(){
  log_this "Cloning Repo"
    # mkdir -p /var/
    cd /var
    git clone $GIT_REPO_URL
    cd /var/$REPO_NAME
}

function add_env_vars(){
    IMDS=`curl -s -H Metadata:true --noproxy "*" "http://169.254.169.254/metadata/instance?api-version=2021-02-01"`
    declare -g USER_DATA_SCRIPT=`curl -H Metadata:true --noproxy "*" "http://169.254.169.254/metadata/instance/compute/userData?api-version=2021-01-01&format=text" | base64 --decode`
}

function install_libs_on_ubuntu(){
  log_this "Begin Azure CLI Installation"
  # https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-linux?pivots=apt
  curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

  # Initiate az login
 
  az config set extension.use_dynamic_install=yes_without_prompt
  az login --identity

  log_this "Begin jq, git, python3-pip Installation"

  sudo apt-get -y install jq
  sudo apt-get -y install git
  sudo apt-get -y install python3-pip
  sudo apt-get -y install mysql-client

  # sudo apt-get -y python3 -m pip install --upgrade pip
  
  log_this "End of jq, git, python3-pip Installation"
}

function install_azure_python_sdk(){
  log_this "Begin Azure Python SDK Installation"
  
  python3 -m pip install --no-cache-dir --upgrade install azure-identity
  python3 -m pip install --no-cache-dir --upgrade install azure-storage-blob
  python3 -m pip install --no-cache-dir --upgrade install azure-storage-queue
  python3 -m pip install --no-cache-dir --upgrade install azure-appconfiguration-provider
  python3 -m pip install --no-cache-dir --upgrade install azure-cosmos
  python3 -m pip install --no-cache-dir --upgrade install mysql.connector
  
  log_this "End of Azure Python SDK Installation"
}

function install_libs(){
    # Prepare the server for python3
    sudo yum -y install git jq
    sudo yum -y install python3-pip
    sudo yum -y install python3 
}

function install_nginx(){
    sudo apt-get -y update
    sudo apt-get -y install nginx
    sudo systemctl start nginx
    sudo systemctl enable nginx


    # Get the server IP address and hostname
    IP_ADDRESS=$(hostname -I | awk '{print $1}')
    HOSTNAME=$(hostname)

# Create holla.html file
echo "holla" > /var/www/html/holla.html

# Create Nginx configuration file
cat << EOF > /etc/nginx/sites-available/miztiikconfig
server {
    listen 80;
    listen [::]:80;

    server_name your_domain.com;

    root /var/www/html;
    index holla.html;

    location / {
        try_files \$uri \$uri/ =404;
    }
}
EOF

    # Create symbolic link to enable the configuration
    sudo ln -s /etc/nginx/sites-available/miztiikconfig /etc/nginx/sites-enabled/

    # Create the index.html file with the desired content
    sudo sh -c "echo 'Hello World from miztiik-automation-vm <b>$HOSTNAME</b> <b>$IP_ADDRESS</b> on $(date) ' > /var/www/html/index.html"

    # Start the nginx service
    sudo service nginx restart
    sudo systemctl restart nginx
}

function install_nodejs(){
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
    . ~/.nvm/nvm.sh
    nvm install node
    node -e "console.log('Running Node.js ' + process.version)"
}

function check_execution(){
  HOST_FQDN=$(hostname)
  HOST_IP=$(hostname -I)
  CURR_PWD=$(pwd)
  CURR_USER=$(whoami)
  CURR_PATH=$(echo $PATH)
  log_this "Miztiik Customisation of Host: ${HOST_FQDN} - ${HOST_IP}"
  # log_this "hello" >>/var/log/miztiik.log
  log_this "Current User: ${CURR_USER}"
  log_this "Current PWD: ${CURR_PWD}"
  log_this "Current PATH: ${CURR_PATH}"
}

check_execution                 |   tee -a "${LOG_FILE}"
install_libs_on_ubuntu          |   tee -a "${LOG_FILE}"
# install_azure_python_sdk        |   tee -a "${LOG_FILE}"
# clone_git_repo                  |   tee -a "${LOG_FILE}"
install_nginx                  |   tee -a "${LOG_FILE}"


