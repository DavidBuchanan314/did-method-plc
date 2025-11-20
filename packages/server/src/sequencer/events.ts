import Database from '../db'
import * as plc from '@did-plc/lib'
import { CID } from 'multiformats/cid'
import { PlcSeqInsert } from '../db/types'
import { sql } from 'kysely'

export type PlcOperationEvent = {
  $type: 'indexed_op'
  did: string
  operation: plc.CompatibleOpOrTombstone
  cid: string // this is redundant info, but allows consumers to double-check
  createdAt: string
}

export type PlcEvent = PlcOperationEvent

export type SeqEvt = {
  seq: number
  sequencedAt: Date
  event: PlcEvent
}

export const formatSeqPlcOp = (
  did: string,
  operation: plc.CompatibleOpOrTombstone,
  cid: CID,
  createdAt: Date,
): PlcSeqInsert => {
  const event: PlcOperationEvent = {
    $type: 'indexed_op',
    did,
    operation,
    cid: cid.toString(),
    createdAt: createdAt.toISOString(),
  }

  return {
    event,
    invalidated: 0,
  }
}

export const sequenceEvt = async (
  dbTxn: Database,
  evt: PlcSeqInsert,
): Promise<void> => {
  dbTxn.assertTransaction()
  console.log(new Date(), 'sending new_plc_event')
  await dbTxn.notify('new_plc_event')

  await dbTxn.db.insertInto('plc_seq').values(evt).execute()
}
