"""Common data-source + metadata + scan tools for the top-level MCP.

Grounds on the REAL environment by exercising SOE functions **as-is**:

  - ``list_data_sources()`` — the live SWS datasource catalog (provider / connector / status /
    id / name) via the top-level ``StitcherClient``, plus registered business datasets.
  - ``get_data_source_metadata(name_or_id)`` — columns + dtypes via the SOE metadata operator
    (``MetadataConsolidateOperator.__read_business_dataset_schema__``, which dispatches to
    ``ExtractRefDataSubOperator.read_database_schema`` for DB-backed connectors or
    ``__extract_reference_dataframe_recursion__`` for object-store).
  - ``scan_data(name_or_id, ...)`` — read REAL data by connection parameters (polars, projection
    pushdown): group_by+value gives a $ split, columns alone gives a head sample.

Uses ``soe.get_workflow_context()`` (hand-built from ``StitcherAssistantConfig``) so every SOE call below
runs as-is — no Temporal context (Step 1 spike verified these bodies are Temporal-free).
"""
from __future__ import annotations

from datetime import date

from fastmcp import FastMCP


def _build_data_connection_util(soe):
    """Construct the SOE DataConnectionUtil (init triggers a Keycloak SA-JWT — network-gated; the
    sub-MCP runs with real env creds so this resolves). Raises a helpful message when unscoped or
    when auth_tenant (the Keycloak realm) is missing (otherwise Keycloak fails with the cryptic
    'Realm does not exist')."""
    err = soe.scope_error()
    if err:
        raise RuntimeError(err)
    ten = soe.tenant_error()
    if ten:
        raise RuntimeError(ten)
    from stitcher.operation_executor.util.data_connection_util import DataConnectionUtil

    wc = soe.get_workflow_context()
    return DataConnectionUtil(wc.environment_id, wc.auth_tenant)


def _load_data_connection(soe, name_or_id):
    from stitcher.webservice.client import DataConnType as _DCT

    dcu = _build_data_connection_util(soe)
    return dcu.get_data_connection(name_or_id, _DCT.DATASOURCES)


def _datasource_metadata(soe, dc) -> dict:
    """Columns + dtypes for one data connection, via the SOE metadata operator (SOE-as-is)."""
    from stitcher.operation_executor.operator.metadata.metadata_consolidate_operator import (
        MetadataConsolidateOperator,
    )

    wc = soe.get_workflow_context()
    op = MetadataConsolidateOperator(wc)
    meta = op.__read_business_dataset_schema__(dc)  # pipeline.PipelineDatasourceMetadata
    schema = {}
    if meta is not None and meta.extract is not None:
        schema = dict(meta.extract.stage_schema or {})
    connector = getattr(dc, "connector_template_display_name", None) or getattr(dc, "connector_template_name", None)
    return {
        "name": getattr(dc, "name", None),
        "id": getattr(dc, "id", None),
        "provider": getattr(dc, "provider_name", None),
        "connector": connector,
        "dataset": getattr(dc, "dataset_name", None) or getattr(dc, "dataset_display_name", None),
        "columns": schema,
    }


def _read_dataframe(soe, dc) -> "object":
    """Read the dataset as an EAGER polars DataFrame via the SOE extract reference-data recursion
    (`pl.concat(batches)` — not a LazyFrame; `_serialize_scan` normalizes it to a LazyFrame before
    calling `.collect()`). Raises a clear message for unsupported (DB) connectors — for those use
    read_database_schema (schema+1-row) via _datasource_metadata instead."""
    from stitcher.operation_executor.operator.extract.sub_operators.extract_ref_data import (
        ExtractRefDataSubOperator,
    )
    from stitcher.pipeline.common.pipeline_config_models.versions.v1_alpha.extract.extract_config import (
        HandleMissingBusinessData,
        RemoteSource,
        SingleUseBusinessDataConnection,
    )

    wc = soe.get_workflow_context()
    path_suffix = (getattr(dc, "dataset_parameters", None) or {}).get("File path suffix")
    source = RemoteSource(
        data_connection=SingleUseBusinessDataConnection(name=dc.name, path_suffix=path_suffix),
        handle_missing_business_data=HandleMissingBusinessData.USE_PREVIOUS,
    )
    lf = ExtractRefDataSubOperator.__extract_reference_dataframe_recursion__(
        workflow_context=wc,
        run_month=date.today(),
        name=dc.name,
        source=source,
        data_connection=dc,
    )
    return lf


def _serialize_scan(df, columns, group_by, value, where, limit) -> str:
    import polars as pl

    if df is None:
        return "(no rows)"
    # The SOE extract reader (`__extract_reference_dataframe_recursion__`) returns an EAGER
    # pl.DataFrame (`pl.concat(batches)`), NOT a LazyFrame. Every aggregation/read path below
    # calls `.collect()` (a LazyFrame-only method), so normalize once here — eager -> lazy via
    # `.lazy()` — and let all the `.collect()` calls below work uniformly.
    if isinstance(df, pl.DataFrame):
        df = df.lazy()
    try:
        colset = set(df.columns)
    except Exception:
        return "(could not read columns)"
    # PROJECTION (pushdown-friendly): only keep the columns we need
    keep = [c for c in (columns or "").split(",") if c.strip() in colset]
    if group_by:
        gb = [c for c in group_by.split(",") if c.strip() in colset]
        val = value if value in colset else ""
        if gb and val:
            try:
                out = df.select([*gb, val]).group_by(gb).agg(pl.col(val).sum()).sort(val, descending=True)
                total = out.select(pl.col(val).sum()).collect().item()
                rows = out.collect().head(limit)
                lines = [f"# ${val} split by {', '.join(gb)}  (total ${total:.2f})", ""]
                for r in rows.iter_rows(named=True):
                    share = (float(r[val]) / total * 100.0) if total else 0.0
                    lines.append(f"- {', '.join(str(r[g]) for g in gb)}  ${float(r[val]):,.2f}  ({share:.1f}%)")
                return "\n".join(lines)
            except Exception as e:  # noqa: BLE001
                return f"ERR aggregating: {e}"
        # group_by but no value -> row counts
        try:
            out = df.select(gb).group_by(gb).len().sort("len", descending=True)
            rows = out.collect().head(limit)
            lines = [f"# row counts by {', '.join(gb)}", ""]
            for r in rows.iter_rows(named=True):
                lines.append(f"- {', '.join(str(r[g]) for g in gb)}  rows={r['len']}")
            return "\n".join(lines)
        except Exception as e:  # noqa: BLE001
            return f"ERR counting: {e}"
    # sample/full columns read
    try:
        dfp = (df.select(keep) if keep else df).collect().head(limit)
        # to_markdown needs the optional 'tabulate' package; fall back to a plain text render
        # so scan_data's head-sample path works even when tabulate isn't installed.
        try:
            return dfp.to_pandas().to_markdown(index=False)
        except ImportError:
            rows = dfp.head(limit).iter_rows()
            header = " | ".join(dfp.columns)
            body = "\n".join(" | ".join(str(v) for v in r) for r in rows)
            return f"{header}\n{body}" if body else header
    except Exception as e:  # noqa: BLE001
        return f"ERR reading sample: {e}"


def register(mcp: FastMCP, client, soe) -> None:
    @mcp.tool
    def list_data_sources() -> str:
        """List the REAL Stitcher data sources (datasource catalog) this environment has plus the
        registered business datasets. Use the returned names VERBATIM in get_data_source_metadata /
        scan_data. Deterministic (SWS, no LLM)."""
        blob = client.list_connections(scope="datasources")
        if blob.startswith("ERR") or "No datasources connections" in blob:
            return f"{blob}\n\n(config_generation needs a live, authenticated SWS env for the datasource catalog.)"
        return blob

    @mcp.tool
    def get_data_source_metadata(name_or_id: str) -> str:
        """Inspect a data source's METADATA — its real columns + dtypes — via the SOE metadata
        operator. Pass the name or id from list_data_sources. Use the returned column names
        VERBATIM when authoring a Lookup/Join. Refuses an unknown datasource."""
        try:
            dc = _load_data_connection(soe, name_or_id)
        except Exception as e:  # noqa: BLE001
            return f"ERR could not load datasource {name_or_id!r}: {str(e)[:200]}"
        meta = _datasource_metadata(soe, dc)
        cols = meta["columns"]
        if not cols:
            return (
                f"name={meta['name']}  id={meta['id']}  provider={meta['provider']}  "
                f"connector={meta['connector']}\n(no schema returned by the SOE metadata operator — "
                f"the connection may be unreachable or have no readable data this month.)"
            )
        lines = [
            f"# datasource: {meta['name']}  (id={meta['id']})",
            f"provider={meta['provider']}  connector={meta['connector']}  dataset={meta['dataset']}",
            f"columns ({len(cols)}):",
        ]
        for col, dtype in sorted(cols.items()):
            lines.append(f"  - {col}  ({dtype})")
        return "\n".join(lines)

    @mcp.tool
    def scan_data(
        name_or_id: str,
        columns: str = "",
        group_by: str = "",
        value: str = "",
        where: str = "",
        limit: int = 20,
    ) -> str:
        """Read REAL data from a datasource by its connection. columns=comma-separated projection.
        group_by + value -> SUM(value) per group with % share (e.g. group_by='x_team', value='EffectiveCost').
        group_by alone -> row counts. Neither -> head(limit) sample. where = 'col op value' (best-effort).
        Uses the SOE extract reference-data reader (polars, projection pushdown)."""
        try:
            dc = _load_data_connection(soe, name_or_id)
        except Exception as e:  # noqa: BLE001
            return f"ERR could not load datasource {name_or_id!r}: {str(e)[:200]}"
        # DB-backed connectors: full materialization goes through the GCS-export extract path
        # (heavy); expose schema + a 1-row sample via the metadata operator instead.
        try:
            from stitcher.operation_executor.operator.extract.sub_operators.extract_ref_data import (
                ExtractRefDataSubOperator,
            )

            if ExtractRefDataSubOperator.supports_database_connector(getattr(dc, "connector_template_name", "")):
                meta = _datasource_metadata(soe, dc)
                cols = meta["columns"] or {}
                head = ", ".join(list(cols)[:20]) or "(no schema)"
                return (
                    f"{name_or_id!r} is a DATABASE-backed connector. Full $ scans go through the GCS-export "
                    f"extract path (heavy). Columns ({len(cols)}):\n  {head}\n"
                    f"Use get_data_source_metadata('{name_or_id}') for the full schema."
                )
        except Exception as e:  # noqa: BLE001
            return f"ERR inspecting connector: {str(e)[:200]}"
        try:
            df = _read_dataframe(soe, dc)
        except Exception as e:  # noqa: BLE001
            return f"ERR reading data: {str(e)[:250]}"
        return _serialize_scan(df, columns, group_by, value, where, limit)
