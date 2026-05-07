"""
Auth provider. Saves access tokens locally in RAM. Delegates to external provider if given
"""

import configparser
import errno
import logging
import os
from pathlib import Path
import pwd
import traceback
from typing import Optional

try:
    import pyfuse3
except ImportError:
    import fuse4dbricks.mock.pyfuse3 as pyfuse3  # type:ignore[no-redef]


logger = logging.getLogger(__name__)

class DatabricksUnifiedAuthProvider:
    """
    Uses Databricks Unified Authentication to authenticate the user.
    Each process may define its corresponding DATABRICKS_CONFIG_PROFILE or DATABRICKS_CONFIG_FILE,
    so we honor the requesting process environment variables by checking /proc/$pid/environ
    
    If fuse4dbricks runs by a regular user, without allow other, then fuse4dbricks will typically
    have access to the environment variables of the requesting processes and no additional permissions are needed.

    However, if fuse4dbricks runs with allow_other then it may require to run as root to access the
    tokens. This is because it will need to check the environment variables of other processes, which
    can be achieved by reading /proc/{pid}/environ, but this requires CAP_SYS_PTRACE capability. Then
    it will require reading the databricks config file, which may be in another user's home directory.
    Root permissions grant access to both environment variables and arbitrary files.

    Alternatively, this unified auth can be disabled and users just need to provide their own token manually.

    """
    def __init__(self):
        pass

    def _home_for_uid(self, uid: int, subuid_file: str = "/etc/subuid") -> str:
        """
        Return the home directory for a UID.

        - If `uid` is a normal system UID (present in /etc/passwd), returns that user's home.
        - If `uid` is a subordinate UID (falls within a range in /etc/subuid),
        returns the home directory of the *owning* user for that subuid range.

        Parameters
        ----------
        uid : int
            UID to resolve.
        subuid_file : str
            Path to /etc/subuid (can be overridden for testing).

        Returns
        -------
        str
            Home directory path.

        Raises
        ------
        KeyError
            If the UID cannot be resolved either as a real UID or a subuid.
        ValueError
            If uid is negative.
        """
        if uid < 0:
            raise ValueError("uid must be non-negative")

        # 1) Fast path: is it a real user uid?
        try:
            return pwd.getpwuid(uid).pw_dir
        except KeyError:
            pass  # Not a real UID, try subuid mapping

        # 2) Subuid path: check /etc/subuid ranges
        owner_username: Optional[str] = self._subuid_owner(uid, subuid_file=subuid_file)
        if owner_username is None:
            raise KeyError(f"UID {uid} not found in /etc/passwd and not in {subuid_file}")

        # 3) Return the owning user's home directory
        try:
            return pwd.getpwnam(owner_username).pw_dir
        except KeyError as e:
            raise KeyError(
                f"Subuid {uid} belongs to '{owner_username}' in {subuid_file}, "
                f"but that user is not present in /etc/passwd"
            ) from e


    def _subuid_owner(self, uid: int, subuid_file: str = "/etc/subuid") -> Optional[str]:
        """
        Return the username owning the subuid range that contains `uid`,
        or None if not found.

        /etc/subuid format (typical):
            username:start_uid:count
        """
        path = Path(subuid_file)
        if not path.exists():
            return None

        # Note: if ranges overlap (shouldn't), this returns the first match.
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                parts = line.split(":")
                if len(parts) != 3:
                    continue  # skip malformed lines

                username, start_s, count_s = parts
                try:
                    start = int(start_s)
                    count = int(count_s)
                except ValueError:
                    continue  # skip malformed numeric fields

                if count <= 0:
                    continue

                end = start + count - 1
                if start <= uid <= end:
                    return username
        return None

    def _get_env_for_pid(self, pid: int) -> Optional[dict[str, str]]:
        # read from /proc/{ctx.pid}/environ
        environ_file = f"/proc/{pid}/environ"
        if not os.path.exists(environ_file):
            logger.error("Environ file %s does not exist for pid %s", environ_file, pid)
            return None
        try:
            with open(environ_file, "r") as f:
                env_vars = f.read().split("\0")
                env_dict = dict(var.split("=", 1) for var in env_vars if "=" in var)
                return env_dict
        except Exception as exc:
            logger.error("Failed to read environ file %s for pid %s. Exception: %s", environ_file, pid, exc)
            return None

    def _get_profile_and_config_file_name(self, env: dict[str, str], uid: int) -> tuple[str, str] | tuple[None, None]:
        """ Gets the databricks profile and config file name.
        1. Check if env defines DATABRICKS_CONFIG_PROFILE and DATABRICKS_CONFIG_FILE
        2. If not, default to DATABRICKS_CONFIG_PROFILE=DEFAULT and DATABRICKS_CONFIG_FILE=~/.databrickscfg (in the user's home directory)
        """
        if env is not None:
            profile = env.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
            config_file = env.get("DATABRICKS_CONFIG_FILE")
        else:
            profile = "DEFAULT"
            config_file = None

        if config_file is None:
            # Default to ~/.databrickscfg in the user's home directory
            home_dir = self._home_for_uid(uid)
            config_file = os.path.join(home_dir, ".databrickscfg")
            if not Path(config_file).exists():
                config_file = None       
        if config_file is None:
            logger.error("No databricks config file found for uid %s. Checked environment variable DATABRICKS_CONFIG_FILE and default location", uid)
            return None, None
        return profile, config_file

    def _get_token_from_config(self, profile: str, config_file: str) -> Optional[str]:
        """ Reads the databricks token from the config file for the given profile. """
        config = configparser.ConfigParser()
        try:
            config.read(config_file)
        except Exception as exc:
            logger.error("Failed to read databricks config file at %s for profile %s. Exception: %s\n%s", config_file, profile, exc, traceback.format_exc())
            return None
        if profile not in config:
            logger.error("Profile '%s' not found in %s", profile, config_file)
            return None
        try:
            token = config[profile]["token"]
            return token
        except KeyError:
            logger.error(f"'token' not found in profile '{profile}' of {config_file}")
            return None

    async def get_access_token(self, ctx: pyfuse3.RequestContext) -> str:
        """ Gets the databricks token for the given request context by checking the environment variables of the requesting process and then the config file. """
        env = self._get_env_for_pid(ctx.pid)
        # The process defines an access token:
        if env is not None and "DATABRICKS_TOKEN" in env:
            return env["DATABRICKS_TOKEN"]
        # The process does not define a token, but defines a profile or config file:
        profile, config_file = self._get_profile_and_config_file_name(env, ctx.uid)
        if profile is None or config_file is None:
            logger.error("No databricks token found in environment variables for pid %s, and no profile or config file found for uid %s", ctx.pid, ctx.uid)
            return None
        return self._get_token_from_config(profile, config_file)

class AuthProvider:
    def __init__(self, unified_auth: bool = True):
        self._uid_to_access_token: dict[int, str] = {}
        "uid -> access_token"
        self._unified_auth = DatabricksUnifiedAuthProvider() if unified_auth else None


    def invalidate_access_token(self, ctx: pyfuse3.RequestContext):
        if ctx.uid in self._uid_to_access_token:
            del self._uid_to_access_token[ctx.uid]

    async def get_access_token(self, ctx: pyfuse3.RequestContext) -> str:
        """
        There are two possible ways to get a token: A local token stored in the class
        or a token given by an external provider (e.g. obtained via posit workbench).

        Local token has precedence over external provider token.

        For any request, if there is a valid local token that is used. Otherwise we try to get
        a token from the requesting process, if unified authentication is available.

        :param ctx: The Request context representing the user that we want to get the token for
        :raises pyfuse3.FUSEError: raises ENOACCES if no token is available
        :return: A token
        """
        # If the user has given a token, we use that token. Otherwise we check the external provider
        try:
            local_token = self._uid_to_access_token.get(ctx.uid)
            if local_token:
                return local_token
            if self._unified_auth is None:
                raise pyfuse3.FUSEError(errno.EACCES)
            token =  await self._unified_auth.get_access_token(ctx=ctx)
            if token is None:
                raise pyfuse3.FUSEError(errno.EACCES)
            self.set_access_token(ctx.uid, token)
            return token
        except KeyError as exc:
            raise pyfuse3.FUSEError(errno.EACCES) from exc

    def set_access_token(self, ctx_uid: int, access_token: str):
        self._uid_to_access_token[ctx_uid] = access_token
        return access_token
