"""Terminal activity database model.

Shares last-active timestamps across worker processes so any worker's idle
reaper sees traffic handled by the others before tearing a terminal down.
"""

from sqlalchemy import Column, Float, String, UniqueConstraint

from terminals.models.base import Base


class TerminalActivity(Base):
    __tablename__ = "terminal_activity"
    __table_args__ = (
        UniqueConstraint("user_id", "policy_id", name="uq_terminal_activity_user_policy"),
    )

    id = Column(String, primary_key=True)  # "{user_id}:{policy_id}"
    user_id = Column(String, nullable=False)
    policy_id = Column(String, nullable=False)
    last_active_at = Column(Float, nullable=False)  # unix wall-clock timestamp

    def __repr__(self) -> str:
        return (
            f"<TerminalActivity user_id={self.user_id!r} "
            f"policy_id={self.policy_id!r}>"
        )
