"""Emit the deployable DataHub lineage and metadata contracts for platform assets."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DatasetLineageTypeClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
    GlobalTagsClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def ice_urn(table_name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:iceberg,vina_bim_shop.{table_name},PROD)"


def kafka_urn(topic_name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:kafka,{topic_name},PROD)"


def pinot_urn(table_name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:pinot,{table_name},PROD)"


def datajob_urn(dag_id: str, task_id: str = "") -> str:
    if task_id:
        return f"urn:li:dataJob:(urn:li:dataFlow:(airflow,{dag_id},vina-bim-shop-local),{task_id})"
    return f"urn:li:dataFlow:(airflow,{dag_id},vina-bim-shop-local)"


class DataHubLineageEmitter:
    """Send DataHub metadata proposals through the configured GMS endpoint.

    The emitter accepts dataset and job identifiers and lets DataHub transport failures
    propagate so evidence capture cannot report absent lineage as successful.
    """

    def __init__(self, gms_url: str = "http://datahub-gms:8080"):
        self._emitter = DataHubRestEmitter(gms_url)

    def emit_upstream_lineage(self, child_urn: str, parent_urns: list[str]) -> None:
        now = _now_ms()
        self._emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=child_urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=parent,
                            type=DatasetLineageTypeClass.TRANSFORMED,
                            auditStamp=AuditStampClass(
                                time=now,
                                actor="urn:li:corpuser:data_engineer",
                            ),
                        )
                        for parent in parent_urns
                    ]
                ),
            )
        )

    def emit_dataflow(self, *, entity_urn: str, name: str, description: str) -> None:
        self._emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=DataFlowInfoClass(
                    name=name,
                    description=description,
                    env="PROD",
                ),
            )
        )

    def emit_datajob(
        self,
        *,
        entity_urn: str,
        flow_urn: str,
        name: str,
        description: str,
        input_urns: list[str],
        output_urns: list[str],
    ) -> None:
        self._emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=DataJobInfoClass(
                    name=name,
                    description=description,
                    flowUrn=flow_urn,
                    type="BATCH_SCHEDULED",
                    env="PROD",
                ),
            )
        )
        self._emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=DataJobInputOutputClass(
                    inputDatasets=sorted(set(input_urns)),
                    outputDatasets=sorted(set(output_urns)),
                ),
            )
        )

    def emit_tag(self, entity_urn: str, tag_name: str) -> None:
        self.emit_tags(entity_urn, [tag_name])

    def emit_tags(self, entity_urn: str, tag_names: list[str]) -> None:
        self._emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=GlobalTagsClass(
                    tags=[
                        TagAssociationClass(tag=f"urn:li:tag:{tag_name}")
                        for tag_name in sorted(set(tag_names))
                    ]
                ),
            )
        )

    def emit_string_schema(
        self,
        *,
        entity_urn: str,
        platform_urn: str,
        schema_name: str,
        field_names: list[str],
    ) -> None:
        raw_schema = json.dumps({"type": "record", "name": schema_name, "fields": field_names}, sort_keys=True)
        self._emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=SchemaMetadataClass(
                    schemaName=schema_name,
                    platform=platform_urn,
                    version=0,
                    hash=hashlib.sha256(raw_schema.encode("utf-8")).hexdigest(),
                    platformSchema=OtherSchemaClass(rawSchema=raw_schema),
                    fields=[
                        SchemaFieldClass(
                            fieldPath=field_name,
                            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                            nativeDataType="string",
                            nullable=False,
                            description="Contract field emitted for the coursework pipeline output.",
                        )
                        for field_name in field_names
                    ],
                ),
            )
        )

    def emit_ownership(self, entity_urn: str, owner_urn: str, owner_type: str = "TECHNICAL_OWNER") -> None:
        self._emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner=owner_urn,
                            type=getattr(OwnershipTypeClass, owner_type),
                        )
                    ]
                ),
            )
        )

    def emit_assertion(
        self,
        assertion_urn: str,
        dataset_urn: str,
        assertion_type: str,
        success: bool,
        column: str = "",
        run_id: str | None = None,
        timestamp_ms: int | None = None,
    ) -> None:
        from datahub.metadata.schema_classes import (
            AssertionInfoClass,
            AssertionResultClass,
            AssertionResultTypeClass,
            AssertionRunEventClass,
            AssertionRunStatusClass,
            AssertionTypeClass,
            AssertionStdOperatorClass,
            DatasetAssertionInfoClass,
            DatasetAssertionScopeClass,
        )

        now = timestamp_ms if timestamp_ms is not None else _now_ms()
        resolved_run_id = run_id or f"gx_run_{now}"

        self._emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=assertion_urn,
                aspect=AssertionInfoClass(
                    type=AssertionTypeClass.DATASET,
                    datasetAssertion=DatasetAssertionInfoClass(
                        dataset=dataset_urn,
                        scope=DatasetAssertionScopeClass.DATASET_COLUMN if column else DatasetAssertionScopeClass.DATASET_ROWS,
                        fields=[f"urn:li:schemaField:({dataset_urn},{column})"] if column else [],
                        operator=AssertionStdOperatorClass._NATIVE_,
                        nativeType=assertion_type,
                    ),
                ),
            )
        )

        self._emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=assertion_urn,
                aspect=AssertionRunEventClass(
                    timestampMillis=now,
                    asserteeUrn=dataset_urn,
                    runId=resolved_run_id,
                    assertionUrn=assertion_urn,
                    status=AssertionRunStatusClass.COMPLETE,
                    result=AssertionResultClass(
                        type=AssertionResultTypeClass.SUCCESS if success else AssertionResultTypeClass.FAILURE,
                        nativeResults={"success": str(success)},
                    ),
                ),
            )
        )
