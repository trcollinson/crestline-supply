"""CuratedStack: the curated Iceberg tables, checked in the synthesized template."""

import json
import re
from pathlib import Path

import pytest
from aws_cdk import assertions

from app import build_app
from erp_extract.curated import COLUMNS, CURATED_DATABASE, TABLES, column_name
from erp_extract.entities import ENTITIES

INFRA_DIR = Path(__file__).resolve().parent.parent / "infra"
TAGS = {"project": "crestline", "layer": "curated", "milestone": "M2"}


@pytest.fixture(scope="module")
def template() -> assertions.Template:
    base = json.loads((INFRA_DIR / "cdk.json").read_text())["context"]
    app = build_app(base | {"aws:cdk:bundling-stacks": []})
    return assertions.Template.from_stack(app.node.find_child("CrestlineCurated"))


def iceberg_tables(template) -> dict[str, dict]:
    """Table name -> its Iceberg input."""
    return {
        resource["Properties"]["Name"]: resource["Properties"]["OpenTableFormatInput"][
            "IcebergInput"
        ]
        for resource in template.find_resources("AWS::Glue::Table").values()
    }


def test_every_entity_is_an_iceberg_table_in_the_curated_database(template):
    template.has_resource_properties(
        "AWS::Glue::Database", {"DatabaseInput": {"Name": CURATED_DATABASE}}
    )
    for resource in template.find_resources("AWS::Glue::Table").values():
        assert resource["Properties"]["DatabaseName"] == CURATED_DATABASE
    tables = iceberg_tables(template)
    assert set(tables) == set(TABLES.values())
    for table in tables.values():
        # Format version 2: row-level deletes, which MERGE writes.
        assert (table["MetadataOperation"], table["Version"]) == ("CREATE", "2")


def test_columns_are_typed_from_the_schema_and_keys_are_required(template):
    tables = iceberg_tables(template)
    for entity, table_name in TABLES.items():
        fields = tables[table_name]["IcebergTableInput"]["Schema"]["Fields"]
        keys = {column_name(k) for k in ENTITIES[entity][0]}
        assert [(f["Id"], f["Name"], f["Type"]) for f in fields] == [
            (number, column_name(field), kind)
            for number, (field, kind) in enumerate(COLUMNS[entity], start=1)
        ]
        assert {f["Name"] for f in fields if f["Required"]} == keys, entity


def test_the_tables_live_in_a_private_bucket_that_goes_with_the_stack(template):
    [(name, bucket)] = template.find_resources("AWS::S3::Bucket").items()
    props = bucket["Properties"]
    assert props["PublicAccessBlockConfiguration"]["BlockPublicPolicy"] is True
    assert "VersioningConfiguration" not in props  # Iceberg keeps its own history
    assert bucket["DeletionPolicy"] == "Delete"
    template.resource_count_is("Custom::S3AutoDeleteObjects", 1)
    for table in iceberg_tables(template).values():
        location = json.dumps(table["IcebergTableInput"]["Location"])
        assert name in location
    pairs = {t["Key"]: t["Value"] for t in props["Tags"]}
    assert TAGS.items() <= pairs.items()


def test_outputs_name_the_bucket_and_database(template):
    outputs = template.find_outputs("*")
    assert outputs["CuratedDatabaseName"]["Value"] == CURATED_DATABASE
    assert "CuratedBucketName" in outputs


def test_the_template_names_no_account_or_region(template):
    text = json.dumps(template.to_json())
    assert not re.search(r"\b\d{12}\b", text)
    assert not re.search(r"\b(?:us|eu|ap|ca|sa)-[a-z]+-\d\b", text)
