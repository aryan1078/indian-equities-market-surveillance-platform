import type { Metadata } from "next";

import { AlertsBell } from "../components/alerts-bell";
import { TopNav } from "../components/nav";
import { StockSearch } from "../components/stock-search";
import "./globals.css";

export const metadata: Metadata = {
  title: "Market Surveillance Console",
  description: "Indian equities surveillance and contagion platform",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>
        <div className="appShell">
          <TopNav />
          <div className="workspaceShell">
            <header className="workspaceTopbar">
              <div className="workspaceHeading">
                <p className="workspaceEyebrow">Market Operations</p>
                <strong className="workspaceLabel">Indian equities surveillance console</strong>
              </div>
              <div className="workspaceActions">
                <StockSearch />
                <AlertsBell />
              </div>
            </header>
            <main className="workspaceMain">{children}</main>
          </div>
        </div>
      </body>
    </html>
  );
}
