// Recursive vdev tree node. Defined locally rather than in the
// generated client because utoipa cannot emit a schema for a
// self-referential type — see VdevNode's doc in crates/api.

export interface VdevNode {
  name: string
  vdev_type: string
  state: string
  alloc_space: string
  total_space: string
  read_errors: string
  write_errors: string
  checksum_errors: string
  path?: string | null
  children: VdevNode[]
}
