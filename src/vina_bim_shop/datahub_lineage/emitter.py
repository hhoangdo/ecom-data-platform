from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
    GlobalTagsClass,
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

    def emit_tag(self, entity_urn: str, tag_name: str) -> None:
        self._emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=GlobalTagsClass(
                    tags=[TagAssociationClass(tag=f"urn:li:tag:{tag_name}")]
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
                            type=OwnershipTypeClass[owner_type],
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

        now = _now_ms()
        run_id = f"gx_run_{now}"

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
                    runId=run_id,
                    assertionUrn=assertion_urn,
                    status=AssertionRunStatusClass.COMPLETE,
                    result=AssertionResultClass(
                        type=AssertionResultTypeClass.SUCCESS if success else AssertionResultTypeClass.FAILURE,
                        nativeResults={"success": str(success)},
                    ),
                ),
            )
        )
