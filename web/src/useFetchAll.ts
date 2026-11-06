import { useEffect, useState } from "react";

/** Largest page the API serves (REST guideline 3.2: limit 1-500). */
const PAGE_LIMIT = 500;
/** A bound, so a producer that keeps returning a nextCursor cannot loop the screen forever. */
export const MAX_PAGES = 50;

/**
 * fetchAllItems reads a keyset-paginated collection ({items,nextCursor}) to
 * the end: it follows `nextCursor` until it is absent, so a collection that
 * outgrows one page is never silently truncated (REST guideline 5.2). A 200
 * that is not a page (a bare array, `items` missing or null) is REFUSED, not
 * read as an empty list; so is a walk longer than MAX_PAGES.
 */
export async function fetchAllItems<T>(url: string): Promise<T[]> {
  const out: T[] = [];
  let cursor: string | undefined;
  for (let page = 0; page < MAX_PAGES; page++) {
    const sep = url.includes("?") ? "&" : "?";
    let next = `${url}${sep}limit=${PAGE_LIMIT}`;
    if (cursor) next += `&cursor=${encodeURIComponent(cursor)}`;
    const res = await fetch(next);
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`.trim());
    const body: unknown = await res.json();
    const items: unknown = body !== null && typeof body === "object" && !Array.isArray(body) ? Reflect.get(body, "items") : undefined;
    if (!Array.isArray(items)) throw new Error(`GET ${url} answered 200 but not a {items,nextCursor} page`);
    out.push(...(items as T[]));
    const nc: unknown = Reflect.get(body as object, "nextCursor");
    if (typeof nc !== "string" || nc === "") return out;
    cursor = nc;
  }
  throw new Error(`GET ${url} still had a nextCursor after ${MAX_PAGES} pages; refusing to truncate`);
}

export interface FetchAllState<T> {
  data: T[] | null;
  loading: boolean;
  error: Error | null;
}

interface Settled<T> {
  key: string;
  data: T[] | null;
  error: Error | null;
}

/**
 * useFetchAll loads every page of a collection. A null url disables the
 * request; bumping `refreshKey` reloads it (collections reject undeclared
 * query parameters, so there is no `?_r=` cache-buster).
 */
export function useFetchAll<T>(url: string | null, refreshKey = 0): FetchAllState<T> {
  const key = `${refreshKey}|${url ?? ""}`;
  const [settled, setSettled] = useState<Settled<T> | null>(null);
  useEffect(() => {
    if (url === null) return;
    let cancelled = false;
    fetchAllItems<T>(url)
      .then((data) => {
        if (!cancelled) setSettled({ key, data, error: null });
      })
      .catch((e: unknown) => {
        if (!cancelled) setSettled({ key, data: null, error: e instanceof Error ? e : new Error(String(e)) });
      });
    return () => {
      cancelled = true;
    };
  }, [url, key]);
  if (url === null) return { data: null, loading: false, error: null };
  if (settled === null || settled.key !== key) return { data: null, loading: true, error: null };
  return { data: settled.data, loading: false, error: settled.error };
}
