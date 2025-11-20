import { Leader } from '../db/leader'
import Database from '../db'
import { sql } from 'kysely'
import { PLC_SEQ_SEQUENCE } from '../db/types'

const SEQUENCER_LEADER_ID = 1100

export class SequencerLeader {
  leader: Leader
  destroyed = false

  constructor(public db: Database) {
    this.leader = new Leader(SEQUENCER_LEADER_ID, db)
  }

  async run(): Promise<{ ran: boolean }> {
    return this.leader.run(async ({ signal }) => {
      if (!this.db.channels) {
        throw new Error('Database channels not initialized')
      }

      const { listener } = this.db.channels.new_plc_event

      listener.on('message', () => {
        if (!this.destroyed) {
          this.sequenceOutgoing()
        }
      })

      // Poll periodically as backup
      while (!signal.aborted) {
        await this.sequenceOutgoing()
        await wait(5000)
      }
    })
  }

  async sequenceOutgoing(): Promise<void> {
    // Assign seq numbers to all pending events in insertion order
    await this.db.db
      .updateTable('plc_seq')
      .from((qb) =>
        qb
          .selectFrom('plc_seq')
          .select([
            'id as update_id',
            sql<number>`nextval(${sql.literal(PLC_SEQ_SEQUENCE)})`.as(
              'update_seq',
            ),
          ])
          .where('seq', 'is', null)
          .orderBy('id', 'asc')
          .as('update'),
      )
      .set({ seq: sql`update_seq::bigint` })
      .whereRef('id', '=', 'update_id')
      .execute()

    // Notify consumers
    await this.db.notify('outgoing_plc_seq')
  }

  destroy(): void {
    this.destroyed = true
    this.leader.destroy()
  }
}

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
