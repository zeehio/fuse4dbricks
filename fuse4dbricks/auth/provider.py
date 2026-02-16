"""
Auth provider. Saves access tokens locally in RAM. Delegates to external provider if given
"""

import logging

try:
    import pyfuse3
except ImportError:
    import fuse4dbricks.mock.pyfuse3 as pyfuse3  # type:ignore[no-redef]

import errno

logger = logging.getLogger(__name__)

class AuthProvider:
    def __init__(self, provider =None):
        self._uid_to_access_token: dict[int, str] = {}
        "uid -> access_token"
        self._external_provider = provider

    @property
    def has_external_provider(self):
        return self._external_provider is not None

    async def get_access_token(self, ctx_uid: int, force_refresh: bool = False) -> str:
        """
        There are two possible ways to get a token: A local token stored in the class
        or a token given by an external provider (e.g. obtained via oauth).

        Local token has precedence over external provider token.

        :param ctx_uid: The uid representing the user that we want to get the token for
        :param force_refresh: Ignored if the user has provided a token manually.
        :raises pyfuse3.FUSEError: raises ENOACCES if no token is available
        :return: A token
        """
        # If the user has given a token, we use that token. Otherwise we check the external provider
        try:
            local_token = self._uid_to_access_token.get(ctx_uid)
            if local_token:
                return local_token
            if self._external_provider is None:
                raise pyfuse3.FUSEError(errno.EACCES)
            token =  await self._external_provider.get_access_token(ctx_uid=ctx_uid, force_refresh=force_refresh)
            if token is None:
                raise pyfuse3.FUSEError(errno.EACCES)
            return token
        except KeyError as exc:
            raise pyfuse3.FUSEError(errno.EACCES) from exc

    def set_access_token(self, ctx_uid: int, access_token: str):
        self._uid_to_access_token[ctx_uid] = access_token
        return access_token
