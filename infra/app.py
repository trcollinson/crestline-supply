"""The Crestline CDK app: one app, one stack per layer (spec §7, §9).

Run it from this folder through the CDK CLI, e.g. `uv run cdk synth`. The
stacks are environment-agnostic: they deploy to whichever account and region
the AWS profile points at (`--profile` or `AWS_PROFILE`), so no account ID or
region is written into the code or the synthesized templates.
"""

import aws_cdk as cdk

from stacks.foundation import FoundationStack


def build_app(context: dict | None = None) -> cdk.App:
    """The app with every stack in it.

    The CLI hands the app the feature flags in `cdk.json` by itself; tests
    pass them as `context` so they check the same template that deploys.
    """
    app = cdk.App(context=context)

    # The cost report groups the bill by these three tags, so they go on twice.
    # As stack tags, CloudFormation copies them onto every resource it
    # creates, including the Lambda and role CDK adds to empty the buckets on
    # delete. Through `Tags.of()`, they are written into the template on each
    # resource we define, so `cdk synth` shows them.
    tags = {"project": "crestline", "layer": "foundation", "milestone": "M1"}
    foundation = FoundationStack(app, "CrestlineFoundation", tags=tags)
    for key, value in tags.items():
        cdk.Tags.of(foundation).add(key, value)
    return app


if __name__ == "__main__":
    build_app().synth()
