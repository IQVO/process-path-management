/** Mirrors process-path-management's own dto.go processPathResponse
 *  shape exactly -- see that file's doc comment ("domain structs never
 *  cross this boundary"). This is the frontend's copy of that same
 *  boundary contract. */
export interface Eligibility {
  maxUnitsPerLine?: number | null;
  requiredProductAttributes?: string[];
  excludedProductAttributes?: string[];
  nonSortable?: boolean;
}

export interface ProcessPath {
  pathId: string;
  matchPrefix: string;
  direct: boolean;
  requiredCapabilities: string[];
  /** Omitted by the API when no destination role was declared (ADR 0006). */
  destinationLocationRole?: string;
  /** Go duration string, e.g. "2h0m0s" (ADR 0010). Required on define/revise. */
  cycleTimeP95: string;
  /** The fulfillment capability contract (ADR 0010); absent means the
   *  fully permissive zero value. */
  eligibility?: Eligibility;
  status: "ACTIVE" | "DEACTIVATED";
  createdAt: string;
  updatedAt: string;
}
