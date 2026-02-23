# fuse4dbricks

A filesystem in userspace for mounting the Unity Catalog from Databricks.

## Disclaimer

This is not an official databricks package. I, the author of this package, am not affiliated to Databricks. My capacity to support this package is very limited or none. I may review issues and pull requests but I won't commit to timelines or features.

## Features

The filesystem is read only.

This filesystem uses the [public databricks API](https://docs.databricks.com/api/azure/workspace/introduction) to retrieve files, directories and access permissions from the Unity Catalog.

To mitigate latency and improve **performance**, file metadata is cached in-memory. Data is cached
to a local cache directory (`--disk-cache-dir`) and partially to RAM as well. Options to control
the sizes of those caches are available.

**Credentials** are stored in RAM while the filesystem is mounted, and must be passed by writing a
personal access token to a virtual file:

    echo "dapi0000000-2" > /Volumes/.auth/personal_access_token

If fuse (`/etc/fuse.conf`) has `user_allow_other` activated, this driver supports the `--allow-other`,
option so **multiple users** can access it. In this case, the process should typically run from a system user,
(you may consider creating a fuse4dbricks user?) who should have exclusive access to `--disk-cache-dir`. Each user should provide its own personal access token as described. **Permissions are respected for each user**. The cache is shared among all users in this scenario.

When an access token is missing, revoked or expired, the unity catalog is not accessible anymore and only
a virtual `/Volumes/README.txt` file appears, with instructions on how to add the access token.

In the future other auth options may be integrated.

## Installation

You can install this package from pypi:

    pip install "fuse4dbricks"

Or the development version:

    pip install "git+https://github.com/zeehio/fuse4dbricks.git"

## Quickstart

Assuming you are the only user:

    sudo mkdir "/Volumes" # or any other directory, in your home, it's up to you
    fuse4dbricks --workspace "https://adb-xxxx.azuredatabricks.net" /Volumes

Open a new terminal:

    # Provide your databricks access token:
    echo "dapi0000000-2" > /Volumes/.auth/personal_access_token
    # Access your catalog files:
    ls /Volumes
    # Your catalogs will appear

## Multi user setup

- Create a virtual environment and install fuse4dbricks there:

      # Note that fuse4dbricks requires python>=3.11
      sudo mkdir /opt/fuse4dbricks
      sudo chmod 755 /opt/fuse4dbricks
      sudo python3.11 -m venv /opt/fuse4dbricks/venv
      source /opt/fuse4dbricks/venv/bin/activate
      python3 -m pip install fuse4dbricks
      deactivate

- Create a system user account

      sudo useradd --system --shell /usr/sbin/nologin fuse4dbricks

- Create the mount directory:

      sudo mkdir /Volumes
      sudo chown fuse4dbricks /Volumes
      sudo chmod 0700 /Volumes

- Create the cache directory:

      sudo mkdir /var/cache/fuse4dbricks
      sudo chmod 0700 /var/cache/fuse4dbricks
      sudo chown fuse4dbricks /var/cache/fuse4dbricks

- Create a starting script and make it executable:

    Please replace whatever you need here

      cat << EOF | sudo tee /opt/fuse4dbricks/fuse4dbricks_start.sh
      #!/bin/bash

      source /opt/fuse4dbricks/venv/bin/activate
      fuse4dbricks \
        --workspace "https://adb-xxxx.azuredatabricks.net" \
        --disk-cache-dir /var/cache/fuse4dbricks \
        --allow-other \
        --ram-cache-mb 512 \
        --disk-cache-gb 1024 \
        --disk-cache-max-days 30 \
        /Volumes
      EOF
      sudo chmod +x /opt/fuse4dbricks/fuse4dbricks_start.sh

- Create a systemd unit

      cat << EOF | sudo tee /etc/systemd/system/fuse4dbricks.service
      [Unit]
      Description=fuse4dbricks
      After=network.target

      [Service]
      Type=simple
      User=fuse4dbricks
      WorkingDirectory=/opt/fuse4dbricks
      ExecStart=/opt/fuse4dbricks/fuse4dbricks_start.sh
      Restart=on-failure
      RestartSec=5

      [Install]
      WantedBy=multi-user.target
      EOF


- Reload the daemon lists

      sudo systemctl daemon-reload

- Enable and start the service

      sudo systemctl enable fuse4dbricks
      sudo systemctl start fuse4dbricks
