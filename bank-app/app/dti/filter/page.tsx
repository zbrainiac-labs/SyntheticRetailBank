"use client";

import { FilterControls } from "@/components/dti/filter-bar";
import { PageHeader } from "@/components/page-header";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { useDtiMeta } from "@/lib/dti/client";
import { useGlobalFilters } from "@/lib/filters/context";
import { entityLabel, type FilterKey } from "@/lib/filters/model";

const ALL: FilterKey[] = ["entities", "referenceDate", "comparisonDate", "valueType", "presentation", "includePreliminary", "minAbsDelta"];

/** UC2 6.2 Filter Page: global defaults for all DtI pages. A change resets local page selections. */
export default function DtiFilterPage() {
  const { filters, setGlobal } = useGlobalFilters();
  const meta = useDtiMeta();
  const prelim = (meta.data?.preliminary ?? []).filter((p) => p.AS_OF_DATE === filters.referenceDate || p.AS_OF_DATE === filters.comparisonDate);

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow="DtI comparison"
        title="Filter"
        description="Defaults for every DtI page. Pages can override single filters locally; changing a default here resets those local selections."
      />
      <Card>
        <CardHeader>
          <CardTitle>Global filters</CardTitle>
          <CardDescription>Group = sum of the selected legal entities.</CardDescription>
        </CardHeader>
        <CardContent>
          <FilterControls values={filters} show={ALL} onChange={(k, v) => setGlobal({ [k]: v })} />
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Data status of the selected dates</CardTitle>
          <CardDescription>PRELIMINARY runs are not locked yet and are excluded unless “Include preliminary” is set.</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-2">
          {prelim.length === 0 ? (
            <Badge variant="success">All runs FINAL</Badge>
          ) : (
            prelim.map((p) => (
              <Badge key={`${p.AS_OF_DATE}-${p.ENTITY_ID}`} variant="warning">
                {entityLabel(p.ENTITY_ID)} · {p.AS_OF_DATE} · {p.DATA_STATUS}
              </Badge>
            ))
          )}
        </CardContent>
      </Card>
    </div>
  );
}
