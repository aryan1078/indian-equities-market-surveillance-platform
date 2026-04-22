"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const links = [
  { href: "/", label: "Overview" },
  { href: "/stocks", label: "Stocks" },
  { href: "/contagion", label: "Contagion" },
  { href: "/process", label: "Process" },
  { href: "/methodology", label: "Methodology" },
  { href: "/warehouse", label: "Warehouse" },
  { href: "/system", label: "System" },
  { href: "/replay", label: "Replay" },
];

export function TopNav() {
  const pathname = usePathname();

  return (
    <aside className="sidebar">
      <div className="brandPanel">
        <div className="brandMark">MS</div>
        <div className="brandCopy">
          <p className="brandEyebrow">NSE Surveillance</p>
          <h1 className="brandTitle">Operator Console</h1>
        </div>
      </div>

      <nav className="sidebarNav">
        {links.map((link) => {
          const active = pathname === link.href || (link.href !== "/" && pathname.startsWith(link.href));
          return (
            <Link key={link.href} href={link.href} className={`sidebarLink ${active ? "active" : ""}`}>
              <span>{link.label}</span>
            </Link>
          );
        })}
      </nav>

      <div className="sidebarMeta">
        <span className="metaChip" title="Kafka is the event bus carrying ticks, anomalies, replay messages, and decoupled consumers.">
          Kafka
        </span>
        <span className="metaChip" title="Cassandra stores append-heavy intraday tick and anomaly operational data.">
          Cassandra
        </span>
        <span className="metaChip" title="Redis holds hot state for recovery, live snapshots, and fast dashboard reads.">
          Redis
        </span>
        <span className="metaChip" title="PostgreSQL serves both the operational relational layer and the analytical warehouse.">
          PostgreSQL
        </span>
      </div>
    </aside>
  );
}
