import Database from '../db'
import * as plc from '@did-plc/lib'
import { CID } from 'multiformats/cid'
import { PlcSeqInsert } from '../db/types'
import { sql } from 'kysely'

export type PlcOperationEvent = {
  did: string
  operation: plc.CompatibleOpOrTombstone
  cid: string // this is redundant info, but allows consumers to double-check
  createdAt: string
  // Note: "nullified" field is NOT here (it is always false for new events, and keeping it synced would be hard)
}

export type PlcEvent = PlcOperationEvent

export type EventType = 'indexed_op' // this may later be a union with more types

export type SeqEvt = {
  seq: number
  sequencedAt: Date
  type: EventType
  event: PlcEvent
}

export const formatSeqPlcOp = (
  did: string,
  operation: plc.CompatibleOpOrTombstone,
  cid: CID,
  createdAt: Date,
): PlcSeqInsert => {
  const event: PlcOperationEvent = {
    did,
    operation,
    cid: cid.toString(),
    createdAt: createdAt.toISOString(),
  }

  return {
    type: 'indexed_op',
    event,
    invalidated: 0,
  }
}

export const sequenceEvt = async (
  dbTxn: Database,
  evt: PlcSeqInsert,
): Promise<void> => {
  dbTxn.assertTransaction()
  await dbTxn.db.insertInto('plc_seq').values(evt).execute()
}
