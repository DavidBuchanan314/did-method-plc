import { EventEmitter } from 'events'
import Database from '../db'
import { PlcSeqEntry } from '../db/types'
import { PlcEvent, EventType, SeqEvt } from './events'
import { sql } from 'kysely'

export interface SequencerEmitter {
  on(event: 'events', listener: (evts: SeqEvt[]) => void): this
  off(event: 'events', listener: (evts: SeqEvt[]) => void): this
  emit(event: 'events', evts: SeqEvt[]): boolean
  removeAllListeners(event?: string): this
}

export class Sequencer
  extends (EventEmitter as new () => SequencerEmitter)
  implements SequencerEmitter
{
  polling = false
  lastSeen = 0
  destroyed = false
  pollInterval: NodeJS.Timeout | null = null

  constructor(public db: Database) {
    super()
  }

  async start(): Promise<void> {
    const curr = await this.curr()
    if (curr) {
      this.lastSeen = curr.seq ?? 0
    }

    // Poll for new seq events frequently
    this.pollInterval = setInterval(() => {
      if (!this.destroyed && !this.polling) {
        this.pollDb()
      }
    }, 50)
  }

  async curr(): Promise<PlcSeqEntry | null> {
    const result = await this.db.db
      .selectFrom('plc_seq')
      .selectAll()
      .where('seq', 'is not', null)
      .orderBy('seq', 'desc')
      .limit(1)
      .executeTakeFirst()
    return result ?? null
  }

  async next(cursor: number): Promise<PlcSeqEntry | null> {
    const result = await this.db.db
      .selectFrom('plc_seq')
      .selectAll()
      .where('seq', 'is not', null)
      .where('seq', '>', cursor)
      .limit(1)
      .orderBy('seq', 'asc')
      .executeTakeFirst()
    return result ?? null
  }

  async requestSeqRange(opts: {
    earliestSeq?: number
    latestSeq?: number
    limit?: number
    eventTypes?: string[]
  }): Promise<SeqEvt[]> {
    let builder = this.db.db
      .selectFrom('plc_seq')
      .selectAll()
      .where('seq', 'is not', null)
      .where('invalidated', '=', 0)
      .orderBy('seq', 'asc')

    if (opts.earliestSeq !== undefined) {
      builder = builder.where('seq', '>', opts.earliestSeq)
    }
    if (opts.latestSeq !== undefined) {
      builder = builder.where('seq', '<=', opts.latestSeq)
    }
    if (opts.eventTypes !== undefined && opts.eventTypes.length > 0) {
      if (opts.eventTypes.length === 1) {
        builder = builder.where(sql`event->>'$type'`, '=', opts.eventTypes[0])
      } else {
        builder = builder.where(sql`event->>'$type'`, 'in', opts.eventTypes)
      }
    }
    if (opts.limit !== undefined) {
      builder = builder.limit(opts.limit)
    }

    const rows = await builder.execute()

    return rows
      .filter((row) => row.seq !== null && row.sequencedAt !== null)
      .map((row) => ({
        seq: row.seq as number,
        sequencedAt: row.sequencedAt as Date,
        type: row.type as EventType,
        event: row.event as PlcEvent,
      }))
  }

  async pollDb(): Promise<void> {
    this.polling = true
    try {
      const evts = await this.requestSeqRange({
        earliestSeq: this.lastSeen,
        limit: 1000,
      })
      if (evts.length > 0) {
        this.emit('events', evts)
        this.lastSeen = evts.at(-1)?.seq ?? this.lastSeen
      }
    } catch (err) {
      console.error('Sequencer failed to poll', err)
    } finally {
      this.polling = false
    }
  }

  destroy(): void {
    this.destroyed = true
    if (this.pollInterval) {
      clearInterval(this.pollInterval)
      this.pollInterval = null
    }
    super.removeAllListeners()
  }
}
