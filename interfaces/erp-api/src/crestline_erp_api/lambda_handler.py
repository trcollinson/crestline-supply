"""The ERP API in AWS Lambda, behind a function URL (spec §9, M2 step 6).

Mangum turns the function URL's events into ASGI requests for the same
FastAPI app that runs locally. Everything is configured by the environment
the stack sets (`settings.py`), except the two secrets: the client secret and
the token signing key live in Parameter Store as SecureString parameters,
named by `CRESTLINE_ERP_CLIENT_SECRET_PARAMETER` and
`CRESTLINE_ERP_SIGNING_KEY_PARAMETER`, and are read once per cold start.

The app is built on the first request rather than at import, so importing
this module (in a test, say) touches no AWS.
"""

import os
from functools import cache

from mangum import Mangum

from .app import create_app
from .settings import Settings

# Setting -> the environment variable naming the parameter that holds it.
SECRET_PARAMETERS = {
    "client_secret": "CRESTLINE_ERP_CLIENT_SECRET_PARAMETER",
    "signing_key": "CRESTLINE_ERP_SIGNING_KEY_PARAMETER",
}


def read_parameters(names: dict[str, str], ssm=None) -> dict[str, str]:
    """Setting -> its SecureString parameter's value, decrypted."""
    if not names:
        return {}
    if ssm is None:
        import boto3  # in the Lambda runtime; only needed when there are secrets

        ssm = boto3.client("ssm")
    return {
        setting: ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
        for setting, name in names.items()
    }


@cache
def asgi_handler() -> Mangum:
    names = {
        setting: os.environ[variable]
        for setting, variable in SECRET_PARAMETERS.items()
        if variable in os.environ
    }
    settings = Settings.from_env(**read_parameters(names))
    return Mangum(create_app(settings), lifespan="off")


def handler(event: dict, context: object) -> dict:
    return asgi_handler()(event, context)
