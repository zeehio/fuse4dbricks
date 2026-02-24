from dataclasses import dataclass


@dataclass
class UcError(Exception):
    message: str
    status_code: int | None = None
    uc_path: str | None = None
    url: str | None = None
    request_id: str | None = None

    def __str__(self) -> str:
        bits = [self.message]
        if self.status_code is not None:
            bits.append(f"status={self.status_code}")
        if self.uc_path:
            bits.append(f"uc_path={self.uc_path}")
        if self.request_id:
            bits.append(f"request_id={self.request_id}")
        return " ".join(bits)


class UcPermissionDenied(UcError):
    pass


class UcNotFound(UcError):
    pass


class UcConflict(UcError):
    pass


class UcRateLimited(UcError):
    retry_after_s: float | None = None


class UcUnavailable(UcError):
    pass


class UcBadRequest(UcError):
    pass