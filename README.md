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

## Requirements

**This package depends on pyfuse3, that requires the `libfuse3-dev` or `fuse3-devel` system package
being installed. The package name depends on your specific linux distribution.**

- Debian / Ubuntu: `sudo apt install libfuse3-dev`
- RedHat / Fedora: `sudo dnf install fuse3-devel`
- SUSE: `sudo zypper install fuse3-devel`


## Quickstart

If you want to see databricks volumes as if you were on Databricks (in /Volumes) you will need admin
privileges to create /Volumes directory on your computer. Otherwise you can use $HOME/Volumes or whatever
directory you have access to.

    pip install "fuse4dbricks"
    mkdir "$HOME/Volumes" # or any other directory
    fuse4dbricks --workspace "https://adb-xxxx.azuredatabricks.net" $HOME/Volumes
    ls $HOME/Volumes
    # Your catalogs will appear

This uses databricks unified authentication (e.g. searches for `$HOME/.databrickscfg` in your home). If no unified auth file
is found, you will find a README file with instructions on how to provide a personal access token.

If you are the only user (e.g. on your laptop) and you have admin permissions, feel free to use `/Volumes` so paths are like on databricks:

    pip install "fuse4dbricks"
    sudo mkdir /Volumes"
	sudo chown $USER /Volumes
    fuse4dbricks --workspace "https://adb-xxxx.azuredatabricks.net" /Volumes
    ls /Volumes
    # Your catalogs will appear on /Volumes

## Multi user setup

If you are on a shared computer (e.g. an HPC), fuse4dbricks can run as a root service and use the access tokens from any user who
aims to access unity catalog volumes.

The databricks token used will depend on the user who asks for the file.

A cache is shared so if many users read the same files the access speed is faster. Users can't access other users files, because each
user has to provide its own access token.

- Create a virtual environment and install fuse4dbricks there:

      # Note that fuse4dbricks requires python>=3.11
	  sudo bash
      mkdir /opt/fuse4dbricks
      chmod 755 /opt/fuse4dbricks
      python3.11 -m venv /opt/fuse4dbricks/venv
      source /opt/fuse4dbricks/venv/bin/activate
      python3 -m pip install fuse4dbricks
      deactivate
	  exit # exit from sudo bash

- Create the mount directory:

      sudo mkdir /Volumes
      sudo chmod 0755 /Volumes

- Create the cache directory:

      sudo mkdir /var/cache/fuse4dbricks
      sudo chmod 0700 /var/cache/fuse4dbricks

- Enable `user_allow_other` support in `/etc/fuse.conf`

    Edit `/etc/fuse.conf` and uncomment the `user_allow_other` line.

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
      User=root
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
