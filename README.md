
# Installation

    pip install "git+https://github.com/zeehio/fuse4dbricks.git"

# Usage

Assuming you are the only user

    export DATABRICKS_HOST="https://adb-xxxx.azuredatabricks.net"
    export DATABRICKS_TOKEN="dapi...."
    sudo mkdir "/Volumes"
    fuse4dbricks "/Volumes"
