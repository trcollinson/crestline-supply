"""The deployed ERP API's secrets, in Parameter Store (M2 step 6).

The client secret and the token signing key are SecureString parameters.
CloudFormation can't create a SecureString (it would have to write the value
into the template), so this creates them, once, with random values, before
the first deploy of ErpApiStack; the stack only lets the function read them.
Standard-tier parameters cost nothing. Account and region come from the AWS
environment (`AWS_PROFILE`, `AWS_REGION`).
"""

import secrets

import boto3

# Setting -> parameter name (infra/stacks/erp_api.py names the same two).
PARAMETERS = {
    "client_secret": "/crestline/erp-api/client-secret",
    "signing_key": "/crestline/erp-api/signing-key",
}
TAGS = [
    {"Key": "project", "Value": "crestline"},
    {"Key": "layer", "Value": "erp-api"},
    {"Key": "milestone", "Value": "M2"},
]


def ensure(ssm=None, rotate: bool = False) -> dict[str, str]:
    """Create any parameter that is missing (or replace both, with `rotate`).

    Returns parameter name -> "created", "kept", or "rotated". Existing values
    are kept unless asked, so running it twice changes nothing.
    """
    ssm = ssm or boto3.client("ssm")
    existing = _existing(ssm)
    done = {}
    for name in PARAMETERS.values():
        value = secrets.token_urlsafe(32)
        if name not in existing:
            ssm.put_parameter(
                Name=name,
                Value=value,
                Type="SecureString",
                Description="Crestline ERP API (simulated); see crestline.erp_secrets",
                Tags=TAGS,
            )
            done[name] = "created"
        elif rotate:
            ssm.put_parameter(
                Name=name, Value=value, Type="SecureString", Overwrite=True
            )
            done[name] = "rotated"
        else:
            done[name] = "kept"
    return done


def read(ssm=None) -> dict[str, str]:
    """Setting -> value, for a client of the deployed API (the laptop test)."""
    ssm = ssm or boto3.client("ssm")
    return {
        setting: ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
        for setting, name in PARAMETERS.items()
    }


def delete(ssm=None) -> list[str]:
    """Remove the parameters; returns the names that existed."""
    ssm = ssm or boto3.client("ssm")
    names = sorted(_existing(ssm))
    if names:
        ssm.delete_parameters(Names=names)
    return names


def _existing(ssm) -> set[str]:
    found = ssm.get_parameters(Names=list(PARAMETERS.values()))["Parameters"]
    return {parameter["Name"] for parameter in found}
