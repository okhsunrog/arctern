import type { VdevNode } from '../client'

/// Aggregate health rollup for a vdev subtree. `unhealthy` means any
/// descendant (or the node itself) is in a non-ONLINE state OR has any
/// non-zero error counter. The counts are sums across the whole subtree
/// (not just the node) so a collapsed interior node still surfaces the
/// total damage.
export interface SubtreeHealth {
  unhealthy: boolean
  readErrors: number
  writeErrors: number
  checksumErrors: number
  /// Worst state in the subtree, by severity (FAULTED > UNAVAIL >
  /// REMOVED > DEGRADED > OFFLINE > ONLINE).
  worstState: string
}

const STATE_SEVERITY: Record<string, number> = {
  ONLINE: 0,
  OFFLINE: 1,
  DEGRADED: 2,
  REMOVED: 3,
  UNAVAIL: 4,
  FAULTED: 5,
}

function worse(a: string, b: string): string {
  return (STATE_SEVERITY[a] ?? 0) >= (STATE_SEVERITY[b] ?? 0) ? a : b
}

export function subtreeHealth(node: VdevNode): SubtreeHealth {
  const r = Number.parseInt(node.read_errors, 10) || 0
  const w = Number.parseInt(node.write_errors, 10) || 0
  const c = Number.parseInt(node.checksum_errors, 10) || 0
  let agg: SubtreeHealth = {
    unhealthy: node.state !== 'ONLINE' || r > 0 || w > 0 || c > 0,
    readErrors: r,
    writeErrors: w,
    checksumErrors: c,
    worstState: node.state,
  }
  for (const child of node.children ?? []) {
    const sub = subtreeHealth(child)
    agg = {
      unhealthy: agg.unhealthy || sub.unhealthy,
      readErrors: agg.readErrors + sub.readErrors,
      writeErrors: agg.writeErrors + sub.writeErrors,
      checksumErrors: agg.checksumErrors + sub.checksumErrors,
      worstState: worse(agg.worstState, sub.worstState),
    }
  }
  return agg
}

/// `/dev/disk/by-id/nvme-Samsung_…-part5` truncates the manufacturer
/// prefix for the inline display while keeping the unique tail.
export function displayName(name: string): string {
  const idx = name.lastIndexOf('/')
  if (idx >= 0 && name.length > 40) return '…' + name.slice(idx)
  if (name.length > 60) return name.slice(0, 28) + '…' + name.slice(-28)
  return name
}
