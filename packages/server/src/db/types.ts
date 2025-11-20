import * as plc from '@did-plc/lib'
import { Generated, GeneratedAlways, Insertable, Selectable } from 'kysely'

export interface PlcDatabase {
  close(): Promise<void>
  healthCheck(): Promise<void>
  validateAndAddOp(
    did: string,
    proposed: plc.CompatibleOpOrTombstone,
    proposedDate: Date,
  ): Promise<void>
  opsForDid(did: string): Promise<plc.CompatibleOpOrTombstone[]>
  indexedOpsForDid(
    did: string,
    includeNull?: boolean,
  ): Promise<plc.IndexedOperation[]>
  lastOpForDid(did: string): Promise<plc.CompatibleOpOrTombstone | null>
  exportOps(count: number, after?: Date): Promise<plc.ExportedOp[]>
  removeInvalidOps(
    did: string,
    cid: string,
  ): Promise<plc.CompatibleOpOrTombstone[]>
}

export interface DidsTable {
  did: string
}

export interface OperationsTable {
  did: string
  operation: plc.CompatibleOpOrTombstone
  cid: string
  nullified: boolean
  createdAt: Generated<Date> // Note: we do not currently make use of the Generated feature, it could be removed in future
}

export interface AdminLogsTable {
  id: Generated<number>
  type: string
  data: Record<string, string>
  createdAt: Generated<Date>
}

export const PLC_SEQ_SEQUENCE = 'plc_seq_sequence'

export interface PlcSeq {
  id: GeneratedAlways<number>
  seq: number | null
  event: Record<string, unknown>
  invalidated: Generated<number>
  sequencedAt?: Date
}

export type PlcSeqInsert = Insertable<PlcSeq>
export type PlcSeqEntry = Selectable<PlcSeq>

export interface DatabaseSchema {
  dids: DidsTable
  operations: OperationsTable
  admin_logs: AdminLogsTable
  plc_seq: PlcSeq
}
