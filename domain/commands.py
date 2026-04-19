from dataclasses import dataclass
from typing import Optional

from .errors import DomainValidationError


@dataclass(frozen=True)
class CreateReservation:
    reservation_at: str
    party_size: int
    source: str = "manual"
    external_ref: Optional[str] = None
    guest_name: Optional[str] = None
    guest_phone: Optional[str] = None
    comment: Optional[str] = None
    actor: Optional[str] = None

    def __post_init__(self) -> None:
        if self.party_size <= 0:
            raise DomainValidationError("party_size must be greater than 0")
        if not self.reservation_at:
            raise DomainValidationError("reservation_at is required")


@dataclass(frozen=True)
class ConfirmReservation:
    reservation_id: int
    actor: Optional[str] = None

    def __post_init__(self) -> None:
        if self.reservation_id <= 0:
            raise DomainValidationError("reservation_id must be positive")


@dataclass(frozen=True)
class CancelReservation:
    reservation_id: int
    reason: Optional[str] = None
    actor: Optional[str] = None

    def __post_init__(self) -> None:
        if self.reservation_id <= 0:
            raise DomainValidationError("reservation_id must be positive")


@dataclass(frozen=True)
class AssignTable:
    reservation_id: int
    table_id: int
    assigned_by: Optional[str] = None

    def __post_init__(self) -> None:
        if self.reservation_id <= 0:
            raise DomainValidationError("reservation_id must be positive")
        if self.table_id <= 0:
            raise DomainValidationError("table_id must be positive")


@dataclass(frozen=True)
class ClearTable:
    reservation_id: int
    released_by: Optional[str] = None

    def __post_init__(self) -> None:
        if self.reservation_id <= 0:
            raise DomainValidationError("reservation_id must be positive")


@dataclass(frozen=True)
class SetDeposit:
    reservation_id: int
    amount: int
    comment: Optional[str] = None
    set_by: Optional[str] = None

    def __post_init__(self) -> None:
        if self.reservation_id <= 0:
            raise DomainValidationError("reservation_id must be positive")
        if self.amount < 0:
            raise DomainValidationError("amount must be >= 0")


@dataclass(frozen=True)
class ClearDeposit:
    reservation_id: int
    cleared_by: Optional[str] = None
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        if self.reservation_id <= 0:
            raise DomainValidationError("reservation_id must be positive")


@dataclass(frozen=True)
class RestrictTable:
    table_id: int
    starts_at: str
    ends_at: str
    reason: Optional[str] = None
    block_type: str = "manual"
    reservation_id: Optional[int] = None
    created_by: Optional[str] = None

    def __post_init__(self) -> None:
        if self.table_id <= 0:
            raise DomainValidationError("table_id must be positive")
        if not self.starts_at or not self.ends_at:
            raise DomainValidationError("starts_at and ends_at are required")


@dataclass(frozen=True)
class ClearRestriction:
    restriction_id: int
    cleared_by: Optional[str] = None

    def __post_init__(self) -> None:
        if self.restriction_id <= 0:
            raise DomainValidationError("restriction_id must be positive")

