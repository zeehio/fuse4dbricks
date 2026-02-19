# fuse4dbricks

A filesystem in userspace for mounting the Unity Catalog from Databricks.

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

# Installation

You can install this from pypi:

    pip install "fuse4dbricks"

Or the development version:

    pip install "git+https://github.com/zeehio/fuse4dbricks.git"

# Quickstart

Assuming you are the only user:

    sudo mkdir "/Volumes" # or any other directory, in your home, it's up to you
    fuse4dbricks --workspace "https://adb-xxxx.azuredatabricks.net" /Volumes

Open a new terminal:

    # Provide your databricks access token:
    echo "dapi0000000-2" > /Volumes/.auth/personal_access_token
    # Access your catalog files:
    ls /Volumes
    # Your catalogs will appear

