import Database from '../db'
import * as plc from '@did-plc/lib'
import { CID } from 'multiformats/cid'
import { PlcSeqInsert } from '../db/types'
import { sql } from 'kysely'

export type PlcOperationEvent = {
  $type: 'plc_operation'
  did: string
  operation: plc.CompatibleOpOrTombstone
  cid: string
  nullified: boolean
  createdAt: string
}

export type PlcEvent = PlcOperationEvent

export type SeqEvt = {
  seq: number
  time: Date
  evt: PlcEvent
}

export const formatSeqPlcOp = (
  did: string,
  operation: plc.CompatibleOpOrTombstone,
  cid: CID,
  nullified: boolean,
  createdAt: Date,
): PlcSeqInsert => {
  const event: PlcOperationEvent = {
    $type: 'plc_operation',
    did,
    operation,
    cid: cid.toString(),
    nullified,
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
  await dbTxn.notify('new_plc_event')

  await dbTxn.db.insertInto('plc_seq').values(evt).execute()
}

export const invalidateEventsByType = async (
  db: Database,
  did: string,
  types: string[],
): Promise<void> => {
  if (types.length < 1) return

  let query = db.db
    .updateTable('plc_seq')
    .where(sql`event->>'did'`, '=', did)
    .where('invalidated', '=', 0)

  if (types.length === 1) {
    query = query.where(sql`event->>'$type'`, '=', types[0])
  } else if (types.length > 1) {
    query = query.where(sql`event->>'$type'`, 'in', types)
  }

  await query.set({ invalidated: 1 }).execute()
}
