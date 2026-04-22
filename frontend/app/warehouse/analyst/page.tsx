import Link from "next/link";

import { WarehouseAnalystStudio } from "../../../components/warehouse-analyst-studio";
import {
  fetchWarehouseQuery,
  fetchWarehouseQueryMetadata,
  type WarehouseQueryRequest,
} from "../../../lib/api";

const DEFAULT_REQUEST: WarehouseQueryRequest = {
  dataset: "sector_day",
  dimensions: ["calendar_date", "sector_name"],
  measures: ["active_minutes", "max_composite_score", "contagion_minutes"],
  sort_field: "max_composite_score",
  sort_direction: "desc",
  limit: 60,
};

export default async function WarehouseAnalystPage() {
  const metadata = await fetchWarehouseQueryMetadata();
  const initialRequest = metadata?.presets?.[0]?.request ?? DEFAULT_REQUEST;
  const initialResult = await fetchWarehouseQuery(initialRequest);

  if (!metadata || !initialResult) {
    return (
      <section className="heroPanel">
        <div className="pageHeader">
          <div>
            <p className="pageKicker">Warehouse</p>
            <h2 className="pageTitle">Analyst studio unavailable</h2>
          </div>
        </div>
        <div className="statusNote critical">
          The analyst workbench could not load the warehouse metadata or the initial query result.
        </div>
      </section>
    );
  }

  return (
    <>
      <section className="heroPanel">
        <div className="pageHeader">
          <div>
            <p className="pageKicker">Warehouse</p>
            <h2 className="pageTitle">Analyst studio</h2>
          </div>
          <div className="pageMetaGroup">
            <span className="metaTag">{metadata.datasets.length} datasets</span>
            <span className="metaTag">{metadata.presets.length} analyst presets</span>
          </div>
        </div>
        <div className="statusNote">
          This workbench sits on top of the warehouse facts and materialized views, letting an analyst build grouped queries visually, inspect result sets, and export printable reports without leaving the product.
        </div>
        <div className="inlineMeta">
          <Link href="/warehouse" className="linkButton">
            Back to warehouse overview
          </Link>
        </div>
      </section>

      <WarehouseAnalystStudio metadata={metadata} initialRequest={initialRequest} initialResult={initialResult} />
    </>
  );
}
