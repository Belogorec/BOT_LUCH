from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from .errors import DomainValidationError


@dataclass(frozen=True)
class ReservationDTO:
    reservation_at: str
    party_size: int
    source: str = "manual"
    status: str = "waiting"
    id: Optional[int] = None
    external_ref: Optional[str] = None
    guest_name: Optional[str] = None
    guest_phone: Optional[str] = None
    comment: Optional[str] = None
    deposit_amount: Optional[int] = None
    deposit_comment: Optional[str] = None
    contact_id: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def __post_init__(self) -> None:
        if self.party_size <= 0:
            raise DomainValidationError("party_size must be greater than 0")
        if self.deposit_amount is not None and self.deposit_amount < 0:
            raise DomainValidationError("deposit_amount must be >= 0")
        if not self.reservation_at:
            raise DomainValidationError("reservation_at is required")


@dataclass(frozen=True)
class TableAssignmentDTO:
    reservation_id: int
    table_id: int
    assigned_at: str
    id: Optional[int] = None
    assigned_by: Optional[str] = None
    released_at: Optional[str] = None

    def __post_init__(self) -> None:
        if self.reservation_id <= 0:
            raise DomainValidationError("reservation_id must be positive")
        if self.table_id <= 0:
            raise DomainValidationError("table_id must be positive")
        if not self.assigned_at:
            raise DomainValidationError("assigned_at is required")


@dataclass(frozen=True)
class TableRestrictionDTO:
    table_id: int
    starts_at: str
    ends_at: str
    id: Optional[int] = None
    reason: Optional[str] = None
    block_type: str = "manual"
    reservation_id: Optional[int] = None
    created_by: Optional[str] = None
    created_at: Optional[str] = None

    def __post_init__(self) -> None:
        if self.table_id <= 0:
            raise DomainValidationError("table_id must be positive")
        if not self.starts_at or not self.ends_at:
            raise DomainValidationError("starts_at and ends_at are required")


@dataclass(frozen=True)
class ContactDTO:
    id: Optional[int] = None
    phone_e164: Optional[str] = None
    display_name: Optional[str] = None
    source: str = "manual"
    tags: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class InboundEventDTO:
    platform: str
    bot_scope: str
    event_type: str
    payload: Dict[str, Any]
    external_event_id: Optional[str] = None
    actor_external_id: Optional[str] = None
    actor_display_name: Optional[str] = None
    peer_external_id: Optional[str] = None
    reservation_id: Optional[int] = None

    def __post_init__(self) -> None:
        if not self.platform:
            raise DomainValidationError("platform is required")
        if not self.bot_scope:
            raise DomainValidationError("bot_scope is required")
        if not self.event_type:
            raise DomainValidationError("event_type is required")


@dataclass(frozen=True)
class OutboundMessageDTO:
    platform: str
    bot_scope: str
    message_type: str
    payload: Dict[str, Any]
    reservation_id: Optional[int] = None
    target_peer_id: Optional[int] = None
    target_external_id: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.platform:
            raise DomainValidationError("platform is required")
        if not self.bot_scope:
            raise DomainValidationError("bot_scope is required")
        if not self.message_type:
            raise DomainValidationError("message_type is required")

