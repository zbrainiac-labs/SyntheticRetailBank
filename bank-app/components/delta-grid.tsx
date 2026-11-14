"use client";

import { Table, TableBody, TableCell, TableFooter, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { columnTotals, type DeltaNode } from "@/lib/delta-tree";
import { formatChfM } from "@/lib/format";
import { cn } from "@/lib/utils";
import {
  createExpandedRowModel,
  flexRender,
  rowExpandingFeature,
  tableFeatures,
  useTable,
  type ColumnDef,
  type ExpandedState,
} from "@tanstack/react-table";
import { ChevronDown, ChevronRight } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";

// TanStack Table v9: features and row-model slots are registered explicitly.
const features = tableFeatures({
  rowExpandingFeature,
  expandedRowModel: createExpandedRowModel(),
});
type Col = ColumnDef<typeof features, DeltaNode>;

export interface DeltaColumn {
  key: string;
  label: string;
  /** Second header line, e.g. the date range of a bucket */
  sublabel?: string;
}

export interface DeltaCellClick {
  node: DeltaNode;
  column: string;
  value: number;
}

interface Props {
  nodes: DeltaNode[];
  columns: DeltaColumn[];
  /** Rows expanded on first render: 1 = Level I only, 2 = Level II (UC2 default) */
  defaultDepth?: number;
  /** |value| at or above this CHF amount is highlighted (comment threshold) */
  threshold?: number;
  onCellClick?: (c: DeltaCellClick) => void;
  /** Colour marker per Level I label */
  levelColors?: Record<string, string>;
  totalLabel?: string;
  /** Unfiltered tree for the control sum when `nodes` is reduced by the amount filter */
  totalNodes?: DeltaNode[];
  /** Row id highlighted on load (back navigation from the deal level) */
  focusId?: string;
  /** Column of the highlighted cell */
  focusColumn?: string;
}

/** Ancestors of a node id ("A > B > C" -> A and A > B), expanded so the focused row is visible */
function ancestorsOf(id: string | undefined): Record<string, boolean> {
  if (!id) return {};
  const parts = id.split(" > ");
  return Object.fromEntries(parts.slice(0, -1).map((_, i) => [parts.slice(0, i + 1).join(" > "), true]));
}

function expandedToDepth(nodes: DeltaNode[], depth: number): ExpandedState {
  const out: Record<string, boolean> = {};
  const walk = (ns: DeltaNode[]) =>
    ns.forEach((n) => {
      if (n.level < depth && n.children?.length) {
        out[n.id] = true;
        walk(n.children);
      }
    });
  walk(nodes);
  return out;
}

/** The parts of a TanStack row the cells use (avoids depending on v9's generic Row type). */
interface GridRow {
  original: DeltaNode;
  depth: number;
  getCanExpand: () => boolean;
  getIsExpanded: () => boolean;
  getToggleExpandedHandler: () => () => void;
}

function LabelCell({ row, levelColors }: Readonly<{ row: GridRow; levelColors: Record<string, string> }>) {
  const root = row.original.id.split(" > ")[0];
  const expanded = row.getIsExpanded();
  return (
    <div className="flex min-w-0 items-center gap-1.5" style={{ paddingLeft: `${row.depth * 16}px` }}>
      {row.getCanExpand() ? (
        <button
          type="button"
          onClick={row.getToggleExpandedHandler()}
          aria-label={expanded ? "Collapse" : "Expand"}
          className="rounded p-0.5 text-muted-foreground hover:bg-muted"
        >
          {expanded ? <ChevronDown className="size-4" /> : <ChevronRight className="size-4" />}
        </button>
      ) : (
        <span className="inline-block w-5" />
      )}
      {row.depth === 0 && levelColors[root] && (
        <span className="size-2.5 shrink-0 rounded-full" style={{ background: levelColors[root] }} />
      )}
      <span className={cn("truncate", row.depth === 0 && "font-semibold")}>{row.original.label}</span>
    </div>
  );
}

function ColumnHeader({ col }: Readonly<{ col: DeltaColumn }>) {
  return (
    <div className="text-right">
      <div>{col.label}</div>
      {col.sublabel && <div className="font-normal normal-case tracking-normal">{col.sublabel}</div>}
    </div>
  );
}

interface ValueCellProps {
  node: DeltaNode;
  column: string;
  threshold?: number;
  onCellClick?: (c: DeltaCellClick) => void;
  focused: boolean;
}

function ValueCell({ node, column, threshold, onCellClick, focused }: Readonly<ValueCellProps>) {
  const value = node.values[column] ?? 0;
  const hot = threshold !== undefined && Math.abs(value) >= threshold;
  const text = formatChfM(value);
  if (onCellClick && node.drillable && value !== 0) {
    return (
      <button
        type="button"
        onClick={() => onCellClick({ node, column, value })}
        data-focused={focused || undefined}
        className={cn(
          "w-full rounded px-1 text-right underline-offset-2 hover:underline",
          hot && "bg-warning/25",
          focused && "ring-2 ring-primary",
        )}
      >
        {text}
      </button>
    );
  }
  return <span className={cn("block px-1", hot && "rounded bg-warning/25", focused && "rounded ring-2 ring-primary")}>{text}</span>;
}

interface CellOptions {
  threshold?: number;
  onCellClick?: (c: DeltaCellClick) => void;
  levelColors: Record<string, string>;
  focusId?: string;
  focusColumn?: string;
}

/** TanStack column definitions: the hierarchy label column plus one value column per bucket/day. */
function buildColumnDefs(columns: DeltaColumn[], o: CellOptions): Col[] {
  const label: Col = {
    id: "label",
    header: "Position",
    cell: ({ row }) => <LabelCell row={row} levelColors={o.levelColors} />,
  };
  const values = columns.map<Col>((col) => ({
    id: col.key,
    header: () => <ColumnHeader col={col} />,
    cell: ({ row }) => (
      <ValueCell
        node={row.original}
        column={col.key}
        threshold={o.threshold}
        onCellClick={o.onCellClick}
        focused={row.original.id === o.focusId && col.key === o.focusColumn}
      />
    ),
  }));
  return [label, ...values];
}

/**
 * Expandable delta table for the UC2 pages: hierarchy rows (Level I-IV or
 * product group > cluster > product), one column per bucket or day, a
 * control-sum footer and clickable cells for drill-down.
 */
export function DeltaGrid({
  nodes,
  columns,
  defaultDepth = 2,
  threshold,
  onCellClick,
  levelColors = {},
  totalLabel = "Control sum",
  totalNodes,
  focusId,
  focusColumn,
}: Props) {
  const [expanded, setExpanded] = useState<ExpandedState>(() => ({
    ...(expandedToDepth(nodes, defaultDepth) as Record<string, boolean>),
    ...ancestorsOf(focusId),
  }));
  const totals = useMemo(() => columnTotals(totalNodes ?? nodes, columns.map((c) => c.key)), [totalNodes, nodes, columns]);
  const focusRef = useRef<HTMLTableRowElement | null>(null);
  useEffect(() => {
    focusRef.current?.scrollIntoView({ block: "center" });
  }, []);

  const columnDefs = useMemo(
    () => buildColumnDefs(columns, { threshold, onCellClick, levelColors, focusId, focusColumn }),
    [columns, threshold, onCellClick, levelColors, focusId, focusColumn],
  );

  const table = useTable({
    features,
    data: nodes,
    columns: columnDefs,
    state: { expanded },
    onExpandedChange: setExpanded,
    getRowId: (n) => n.id,
    getSubRows: (n) => n.children,
  });

  return (
    <Table bordered>
      <TableHeader>
        {table.getHeaderGroups().map((hg) => (
          <TableRow key={hg.id}>
            {hg.headers.map((h, i) => (
              <TableHead key={h.id} numeric={i > 0} className={cn(i === 0 && "sticky left-0 z-10 min-w-56 bg-muted")}>
                {flexRender(h.column.columnDef.header, h.getContext())}
              </TableHead>
            ))}
          </TableRow>
        ))}
      </TableHeader>
      <TableBody>
        {table.getRowModel().rows.map((row) => (
          <TableRow key={row.id} data-level={row.original.level} ref={row.id === focusId ? focusRef : undefined}>
            {row.getAllCells().map((cell, i) => (
              <TableCell
                key={cell.id}
                numeric={i > 0}
                className={cn("py-2", i === 0 && "sticky left-0 z-10 bg-card")}
              >
                {flexRender(cell.column.columnDef.cell, cell.getContext())}
              </TableCell>
            ))}
          </TableRow>
        ))}
      </TableBody>
      <TableFooter>
        <TableRow>
          <TableCell className="sticky left-0 z-10 bg-muted py-2">{totalLabel}</TableCell>
          {columns.map((c) => (
            <TableCell key={c.key} numeric className="py-2">
              {formatChfM(totals[c.key])}
            </TableCell>
          ))}
        </TableRow>
      </TableFooter>
    </Table>
  );
}
