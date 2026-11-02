r"""Crestline's machine gateways (spec §4.3).

`sparkplug` encodes and decodes Sparkplug B payloads with the schema in
`proto/sparkplug_b.proto` (Eclipse Tahu, EPL-2.0, vendored unmodified). The
simulator uses it to write LSR-01's stream, the local pipeline to read it;
in AWS the IoT rule decodes the same schema from a descriptor set built from
the same file. The replay publisher (M3 step 7) joins this package later.

`sparkplug_b_pb2.py` is generated from the proto and committed, so nothing
needs protoc at run time. To regenerate it after changing the proto:

    uv run python -m grpc_tools.protoc -I interfaces/machines/proto \
        --python_out=interfaces/machines/src/crestline_machines \
        interfaces/machines/proto/sparkplug_b.proto

The generated file names the protobuf runtime it needs; this package's
dependency pin matches it.
"""
