# fuse4dbricks

A filesystem in userspace for mounting the Unity Catalog from Databricks.

## Disclaimer

This is not an official databricks package. I, the author of this package, am not affiliated to Databricks. My capacity to support this package is very limited or none. I may review issues and pull requests but I won't commit to timelines or features.

## Features

This filesystem uses the [public databricks API](https://docs.databricks.com/api/azure/workspace/introduction) to retrieve files, directories and access permissions from the Unity Catalog.

**Read and write** access is supported for Unity Catalog Volumes. Users with `WRITE_VOLUME` privilege can create, overwrite and delete files and directories. Files above 5 GB are uploaded via multipart upload handled transparently by the Databricks SDK. Pass `--read-only` to disable all writes to Unity Catalog (useful when mounting a shared volume that should not be modified).

To mitigate latency and improve **performance**, file metadata is cached in-memory. Data is cached
to a local cache directory (`--disk-cache-dir`) and partially to RAM as well. Options to control
the sizes of those caches are available.

**Credentials** are stored in RAM while the filesystem is mounted.

`DATABRICKS_TOKEN`, `DATABRICKS_CONFIG_PROFILE` and `DATABRICKS_CONFIG_FILE` are used to find the databricks configuration file and obtain a valid access token. Alternatively, credentials can be passed by writing a
personal access token to a virtual file:

    echo "dapi0000000-2" > /Volumes/.auth/personal_access_token


If fuse (`/etc/fuse.conf`) has `user_allow_other` activated, this driver supports the `--allow-other`,
option so **multiple users** can access it. In this case, the fuse4dbricks process should run from a root account, who should have exclusive access to `--disk-cache-dir`. fuse4dbricks will inspect the environment variables of the requesting process, as well as the requesting user, in order to find the access credentials. This may require reading the `.databrickscfg` file from the requesting user. The cache is shared among all users in this scenario.

When an access token is missing, revoked or expired, the unity catalog is not accessible anymore and only
a virtual `/Volumes/README.txt` file appears, with instructions on how to add the access token manually.

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

## Known limitations

These are intentional design trade-offs in the current implementation. We are open to
discussing them, so feel free to open an issue if any of them is a problem for your use case.

- **The attribute cache is global.** File and directory metadata (names, sizes and
  modification times) is cached in memory and shared across all users of a single mount.
  As a consequence, names, sizes and modification times of files may leak between users,
  even between users who do not have access to read them. **File contents never leak**:
  reading data always goes through a per-user permission check against Unity Catalog using
  that user's own token. The metadata cache is shared for performance reasons, to avoid
  repeating Unity Catalog API calls for objects that several users access.

- **Users are mapped to tokens 1-to-1.** fuse4dbricks associates a single access token with
  each user (uid). It is therefore not possible for two processes belonging to the same user
  to access different Unity Catalog principals at the same time (for example by setting a
  different `DATABRICKS_CONFIG_PROFILE` per process). The first token resolved for a user is
  reused for all of that user's processes.

- **File owner and group are meaningless.** The user and group reported for a file or
  directory (`st_uid`/`st_gid`, what `ls -l` shows in the owner and group columns) are simply
  whoever happened to populate the shared metadata cache for that entry first, so the same
  file may appear owned by different users at different times. These values are irrelevant:
  access is never decided from POSIX ownership, only from a per-user Unity Catalog permission
  check. Do not rely on the reported owner or group for anything.

- **Not-found results are briefly cached.** When a path is looked up and does not exist, that
  "not found" answer is cached per user for a short time (`--metadata-cache-ttl-negative-sec`,
  5 seconds by default) to avoid repeatedly asking Unity Catalog about missing paths. As a
  result, a file created (or that becomes visible to you) just after you looked for it may not
  appear until this short TTL expires. Unlike file metadata, these negative results are *not*
  shared between users.

- **Writes are object-store semantics, not POSIX semantics.** The Databricks Files API is a
  full-replace store: a file is not visible to other processes until the writing process closes
  it (`release`). Concurrent writers follow last-write-wins — there is no locking. Deleting a
  file while another process has it open will cause that process to receive an I/O error on its
  next network fetch. These are intentional trade-offs for performance on object storage.

- **Opening a large file O_RDWR without O_TRUNC downloads the entire file first.** Because
  writes are buffered locally and uploaded on close, the full existing content must be
  downloaded into a temporary file before the open returns. For very large files (multi-GB)
  this can be slow. Use `O_WRONLY` or `O_RDWR | O_TRUNC` when you intend to overwrite the
  file completely.
