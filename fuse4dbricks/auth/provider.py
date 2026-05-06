"""
Auth provider. Saves access tokens locally in RAM. Delegates to external provider if given
"""

import logging
import pwd
import sys

try:
    import pyfuse3
except ImportError:
    import fuse4dbricks.mock.pyfuse3 as pyfuse3  # type:ignore[no-redef]

import errno
import configparser
import os

logger = logging.getLogger(__name__)

class PositWorkbenchAuthProvider:
    def __init__(self):
        pass

    def _get_profile_and_config_file_name(self, ctx: pyfuse3.RequestContext) -> tuple[str, str] | tuple[None, None]:
        # read from /proc/{ctx.pid}/environ to get DATABRICKS_CONFIG_FILE and DATABRICKS_PROFILE
        environ_file = f"/proc/{ctx.pid}/environ"
        if not os.path.exists(environ_file):
            logger.error("Environ file %s does not exist for pid %s", environ_file, ctx.pid)
            return None, None
        try:
            with open(environ_file, "r") as f:
                env_vars = f.read().split("\0")
                env_dict = dict(var.split("=", 1) for var in env_vars if "=" in var)
        except Exception as exc:
            logger.error("Failed to read environ file %s for pid %s. Exception: %s", environ_file, ctx.pid, exc)
            return None, None
        profile = env_dict.get("DATABRICKS_PROFILE", "DEFAULT")
        config_file = env_dict.get("DATABRICKS_CONFIG_FILE")
        if config_file is None:
            logger.error("DATABRICKS_CONFIG_FILE not found in environ for pid %s", ctx.pid)
            return None, None
        return profile, config_file

    async def get_access_token(self, ctx: pyfuse3.RequestContext) -> str:
        """
        Get access token for a given user. This is just a placeholder implementation. In a real implementation, this could involve IPC with another process that manages authentication (e.g. Posit Workbench).

        :param ctx: The Request context representing the user that we want to get the token for
        :return: An access token
        """
        (databricks_profile, databricks_config_file) = self._get_profile_and_config_file_name(ctx)
        # Read databricks token from config file:
        config = configparser.ConfigParser()
        if not os.path.exists(databricks_config_file):
            return None
        try:
            config.read(databricks_config_file)
        except Exception as exc:
            logger.error("Failed to read databricks config file at %s for uid %s. Exception: %s\n%s", databricks_config_file, ctx.uid, exc, traceback.format_exc())
            return None
        if databricks_profile not in config:
            logger.error("Profile '%s' not found in %s for uid %s", databricks_profile, databricks_config_file, ctx.uid)
            return None
        try:
            token = config[databricks_profile]["token"]
        except KeyError:
            logger.error(f"'token' not found in profile '{databricks_profile}' of {databricks_config_file}")
            return None
        return token

class AuthProvider:
    def __init__(self, provider=None):
        self._uid_to_access_token: dict[int, str] = {}
        "uid -> access_token"
        self._external_provider = provider

    @property
    def has_external_provider(self):
        return self._external_provider is not None

    def invalidate_access_token(self, ctx: pyfuse3.RequestContext):
        if ctx.uid in self._uid_to_access_token:
            del self._uid_to_access_token[ctx.uid]

    async def get_access_token(self, ctx: pyfuse3.RequestContext) -> str:
        """
        There are two possible ways to get a token: A local token stored in the class
        or a token given by an external provider (e.g. obtained via posit workbench).

        Local token has precedence over external provider token.

        :param ctx: The Request context representing the user that we want to get the token for
        :raises pyfuse3.FUSEError: raises ENOACCES if no token is available
        :return: A token
        """
        # If the user has given a token, we use that token. Otherwise we check the external provider
        try:
            local_token = self._uid_to_access_token.get(ctx.uid)
            if local_token:
                return local_token
            if self._external_provider is None:
                raise pyfuse3.FUSEError(errno.EACCES)
            token =  await self._external_provider.get_access_token(ctx=ctx)
            if token is None:
                raise pyfuse3.FUSEError(errno.EACCES)
            self.set_access_token(ctx.uid, token)
            return token
        except KeyError as exc:
            raise pyfuse3.FUSEError(errno.EACCES) from exc

    def set_access_token(self, ctx_uid: int, access_token: str):
        self._uid_to_access_token[ctx_uid] = access_token
        return access_token
