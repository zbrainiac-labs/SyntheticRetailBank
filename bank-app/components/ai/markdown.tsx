import type { ReactNode } from "react";

/** Inline: **bold**, `code`, *italic*. */
function inline(text: string): ReactNode[] {
  const out: ReactNode[] = [];
  const re = /(\*\*[^*]+\*\*|`[^`]+`|\*[^*\s][^*]*\*)/g;
  let last = 0;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text))) {
    if (m.index > last) out.push(text.slice(last, m.index));
    const t = m[0];
    if (t.startsWith("**")) out.push(<strong key={m.index}>{t.slice(2, -2)}</strong>);
    else if (t.startsWith("`")) out.push(<code key={m.index} className="rounded bg-muted px-1 text-[0.9em]">{t.slice(1, -1)}</code>);
    else out.push(<em key={m.index}>{t.slice(1, -1)}</em>);
    last = m.index + t.length;
  }
  if (last < text.length) out.push(text.slice(last));
  return out;
}

/**
 * Minimal markdown (headings, bullet/numbered lists, pipe tables as text,
 * bold/italic/code), no dependencies. Plain text keeps its line breaks.
 */
export function Markdown({ text }: { text: string }) {
  const blocks: ReactNode[] = [];
  let list: { ordered: boolean; items: string[] } | null = null;
  const flush = () => {
    if (!list) return;
    const Tag = list.ordered ? "ol" : "ul";
    blocks.push(
      <Tag key={blocks.length} className={list.ordered ? "ml-5 list-decimal space-y-0.5" : "ml-5 list-disc space-y-0.5"}>
        {list.items.map((it, i) => <li key={i}>{inline(it)}</li>)}
      </Tag>,
    );
    list = null;
  };
  for (const line of text.split("\n")) {
    const h = /^(#{1,4}) +(\S.*)$/.exec(line);
    const b = /^ *[-*•] +(\S.*)$/.exec(line);
    const n = /^ *\d+[.)] +(\S.*)$/.exec(line);
    if (b || n) {
      const ordered = !!n;
      if (list && list.ordered !== ordered) flush();
      list ??= { ordered, items: [] };
      list.items.push((b ?? n)![1]);
      continue;
    }
    flush();
    if (h) {
      const cls = h[1].length <= 2 ? "text-base font-semibold" : "text-sm font-semibold";
      blocks.push(<p key={blocks.length} className={`${cls} mt-2`}>{inline(h[2])}</p>);
    } else if (line.trim() === "") {
      blocks.push(<div key={blocks.length} className="h-2" />);
    } else {
      blocks.push(<p key={blocks.length} className="whitespace-pre-wrap">{inline(line)}</p>);
    }
  }
  flush();
  return <div className="space-y-1 text-sm leading-relaxed">{blocks}</div>;
}
