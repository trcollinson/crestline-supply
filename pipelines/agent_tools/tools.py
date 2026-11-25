"""The agent's tools: what each one is called, takes, and checks (spec §9, M4 step 3).

One list, read twice: the CDK stack registers these schemas with the
gateway, and the Lambdas check every call against the same limits.

The check in the Lambda is the one that counts. The gateway's tool schema
(CloudFormation's `SchemaDefinition`) holds a type, a description, and
which arguments are required, nothing else: no enums, no ranges, no
patterns. So the allowed values are written into each description for the
model to read, and enforced here, where a wrong value is refused before
anything runs. Limits that are a matter of policy (how many days a role
may read, which priorities a planner may file) are left wide here on
purpose: they belong to Cedar (step 4), and a Lambda that refused them
first would hide whether the policy works.
"""

import re
from dataclasses import dataclass, field
from datetime import date, timedelta

# The machines that send data (queries/machines.sql) and the window a read
# may cover: the backfill holds 90 days.
MACHINES = ("LSR-01", "PB-01", "PB-02", "PEM-01", "PC-LINE")
MAX_DAYS = 90
JOB_NUM = re.compile(r"^J\d{6}(-1)?$")
TAG = re.compile(r"^[A-Za-z0-9_.:/\[\]-]{1,64}$")
EMAIL = re.compile(r"^[^@\s]{1,64}@[A-Za-z0-9.-]{1,190}$")

# What the stub CMMS accepts, as a real one would. Which of these a role may
# file is the policy's call, not the CMMS's.
PRIORITIES = ("low", "normal", "high", "emergency")
CATEGORIES = ("mechanical", "electrical", "tooling", "safety", "other")
MAX_DESCRIPTION = 500
MAX_SUBJECT = 200
MAX_BODY = 20_000


class ToolInputError(ValueError):
    """An argument the tool won't take; the caller sees the message."""


@dataclass(frozen=True)
class Tool:
    name: str
    description: str
    properties: dict[str, dict[str, str]]
    required: list[str] = field(default_factory=list)


MACHINE_ARG = {
    "type": "string",
    "description": "Machine ID, one of: " + ", ".join(MACHINES) + ".",
}
DAYS_ARG = {
    "type": "integer",
    "description": f"How many plant days to cover, ending on as_of (1 to {MAX_DAYS}).",
}
AS_OF_ARG = {
    "type": "string",
    "description": "Last plant day to cover, YYYY-MM-DD. Defaults to the newest day of data.",
}

# Target name -> its tools. Cedar names an action `<Target>___<tool>`.
TARGETS: dict[str, list[Tool]] = {
    "PlantData": [
        Tool(
            "downtime_events",
            "The MES downtime log for one machine: each stop an operator wrote "
            "down, with start and end in plant local time and the reason as typed.",
            {"machine_id": MACHINE_ARG, "days": DAYS_ARG, "as_of": AS_OF_ARG},
            ["machine_id", "days"],
        ),
        Tool(
            "machine_downtime",
            "Stops rebuilt from the machine's own data (its reported state and "
            "gaps in its cycle counter), inside working hours, for one machine.",
            {"machine_id": MACHINE_ARG, "days": DAYS_ARG, "as_of": AS_OF_ARG},
            ["machine_id", "days"],
        ),
        Tool(
            "cycles_by_day",
            "Cycles (strokes, parts, or hooks) per plant day for one machine, "
            "counted from its counter.",
            {"machine_id": MACHINE_ARG, "days": DAYS_ARG, "as_of": AS_OF_ARG},
            ["machine_id", "days"],
        ),
        Tool(
            "scrap_by_day",
            "Pieces made and scrapped per plant day for one machine, from the "
            "operators' MES scans, with the ERP's posted scrap beside them "
            "(empty until the nightly labor batch posts the day).",
            {"machine_id": MACHINE_ARG, "days": DAYS_ARG, "as_of": AS_OF_ARG},
            ["machine_id", "days"],
        ),
        Tool(
            "job_margin",
            "Margin of completed jobs released in the window, by brake and setup "
            "shift: standard, ERP, and rebuilt from hours actually worked.",
            {"days": DAYS_ARG, "as_of": AS_OF_ARG},
            ["days"],
        ),
        Tool(
            "job_status",
            "One job as the ERP has it: part, quantities, dates, costs, and each "
            "operation's progress.",
            {
                "job_num": {
                    "type": "string",
                    "description": "Job number, e.g. J104221 (or J104221-1 for a split).",
                }
            },
            ["job_num"],
        ),
    ],
    "Maintenance": [
        Tool(
            "create_maintenance_request",
            "Open a request in the maintenance system (CMMS) for one machine.",
            {
                "machine_id": MACHINE_ARG,
                "priority": {
                    "type": "string",
                    "description": "One of: " + ", ".join(PRIORITIES) + ".",
                },
                "category": {
                    "type": "string",
                    "description": "One of: " + ", ".join(CATEGORIES) + ".",
                },
                "description": {
                    "type": "string",
                    "description": f"What is wrong, up to {MAX_DESCRIPTION} characters.",
                },
            },
            ["machine_id", "priority", "category", "description"],
        ),
    ],
    "Reports": [
        Tool(
            "send_report",
            "Email a report.",
            {
                "recipient": {"type": "string", "description": "Email address."},
                "subject": {"type": "string", "description": "Subject line."},
                "body": {"type": "string", "description": "The report, plain text."},
            },
            ["recipient", "subject", "body"],
        ),
    ],
    # Stands in for a vendor's MCP server registered on a shared gateway.
    # Its Lambdas log the call and answer as if they worked; they reach nothing.
    "PlantScada": [
        Tool(
            "write_tag",
            "Write a value to a PLC tag through the plant SCADA server.",
            {
                "tag": {
                    "type": "string",
                    "description": "PLC tag address, e.g. PB02.N7:20.",
                },
                "value": {"type": "string", "description": "Value to write."},
            },
            ["tag", "value"],
        ),
        Tool(
            "read_tag",
            "Read a PLC tag's current value through the plant SCADA server.",
            {
                "tag": {
                    "type": "string",
                    "description": "PLC tag address, e.g. PB02.N7:20.",
                }
            },
            ["tag"],
        ),
    ],
}


def tool_name(context: object) -> str:
    """The tool a gateway call is for, without its target: `downtime_events`.

    The gateway names it `<Target>___<tool>` in the Lambda's client context;
    the arguments are the event itself.
    """
    custom = getattr(getattr(context, "client_context", None), "custom", None) or {}
    return custom.get("bedrockAgentCoreToolName", "").split("___")[-1]


def gateway_request_id(context: object) -> str | None:
    """The gateway's ID for this call, the same one its traces carry."""
    custom = getattr(getattr(context, "client_context", None), "custom", None) or {}
    return custom.get("bedrockAgentCoreAwsRequestId")


# --- Checks -----------------------------------------------------------------------


def window(arguments: dict, last_day: date) -> tuple[date, date]:
    """The plant days a read covers: `days` of them, ending on `as_of`."""
    days = arguments.get("days")
    if isinstance(days, bool) or not isinstance(days, int) or not 1 <= days <= MAX_DAYS:
        raise ToolInputError(f"days must be a whole number from 1 to {MAX_DAYS}")
    as_of = arguments.get("as_of")
    if as_of is None:
        end = last_day
    else:
        try:
            end = date.fromisoformat(str(as_of))
        except ValueError:
            raise ToolInputError("as_of must be a date, YYYY-MM-DD") from None
    return end - timedelta(days=days - 1), end


def machine(arguments: dict) -> str:
    value = arguments.get("machine_id")
    if value not in MACHINES:
        raise ToolInputError("machine_id must be one of " + ", ".join(MACHINES))
    return value


def job_num(arguments: dict) -> str:
    value = arguments.get("job_num")
    if not isinstance(value, str) or not JOB_NUM.match(value):
        raise ToolInputError("job_num must look like J104221 or J104221-1")
    return value


def one_of(arguments: dict, name: str, allowed: tuple[str, ...]) -> str:
    value = arguments.get(name)
    if value not in allowed:
        raise ToolInputError(f"{name} must be one of " + ", ".join(allowed))
    return value


def text(
    arguments: dict, name: str, longest: int, pattern: re.Pattern | None = None
) -> str:
    value = arguments.get(name)
    if not isinstance(value, str) or not value.strip() or len(value) > longest:
        raise ToolInputError(f"{name} must be text of 1 to {longest} characters")
    if pattern is not None and not pattern.match(value):
        raise ToolInputError(f"{name} is not in the expected form")
    return value
