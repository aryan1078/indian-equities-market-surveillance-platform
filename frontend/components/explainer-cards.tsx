import Link from "next/link";

type ExplainerItem = {
  title: string;
  description: string;
  value?: string;
  tone?: "default" | "accent" | "warning" | "critical";
};

type ExplainerCardsProps = {
  eyebrow: string;
  title: string;
  items: ExplainerItem[];
  meta?: string;
  footerHref?: string;
  footerLabel?: string;
};

export function ExplainerCards({
  eyebrow,
  title,
  items,
  meta,
  footerHref,
  footerLabel,
}: ExplainerCardsProps) {
  return (
    <section className="surface">
      <div className="panelHeader">
        <div>
          <p className="panelEyebrow">{eyebrow}</p>
          <h3 className="panelTitle">{title}</h3>
        </div>
        {meta ? <span className="panelMeta">{meta}</span> : null}
      </div>
      <div className="explainerGrid">
        {items.map((item) => (
          <div key={item.title} className={`explainerCard ${item.tone ?? "default"}`}>
            <div className="explainerTitleRow">
              <strong>{item.title}</strong>
              {item.value ? <span className="explainerValue">{item.value}</span> : null}
            </div>
            <div className="explainerText">{item.description}</div>
          </div>
        ))}
      </div>
      {footerHref && footerLabel ? (
        <div className="explainerFooter">
          <Link href={footerHref} className="linkButton">
            {footerLabel}
          </Link>
        </div>
      ) : null}
    </section>
  );
}
