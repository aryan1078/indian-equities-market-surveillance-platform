"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const links = [
  { href: "/", label: "Overview" },
  { href: "/stocks", label: "Stocks" },
  { href: "/contagion", label: "Contagion" },
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
        <span className="metaChip">Kafka</span>
        <span className="metaChip">Cassandra</span>
        <span className="metaChip">Redis</span>
        <span className="metaChip">PostgreSQL</span>
      </div>
    </aside>
  );
}
