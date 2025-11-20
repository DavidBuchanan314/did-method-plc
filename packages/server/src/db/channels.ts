import { sql } from 'kysely'
import { EventEmitter } from 'events'
import { Client as PgClient } from 'pg'
import Database from './index'

export class Channels {
  new_plc_event: DbChannel
  outgoing_plc_seq: DbChannel

  constructor(public db: Database) {
    this.new_plc_event = new DbChannel(db, 'new_plc_event')
    this.outgoing_plc_seq = new DbChannel(db, 'outgoing_plc_seq')
  }

  async destroy(): Promise<void> {
    await Promise.all([
      this.new_plc_event.destroy(),
      this.outgoing_plc_seq.destroy(),
    ])
  }
}

export class DbChannel {
  listener = new EventEmitter()
  client: PgClient | null = null
  destroyed = false

  constructor(public db: Database, public name: string) {
    this.setup()
  }

  private async setup(): Promise<void> {
    try {
      // Get a dedicated client for LISTEN
      const pool = (this.db.db as any).getExecutor().adapter.pool
      this.client = await pool.connect()

      if (!this.client) {
        throw new Error('Failed to get database client')
      }

      this.client.on('notification', (msg) => {
        if (msg.channel === this.name) {
          this.listener.emit('message')
        }
      })

      this.client.on('error', (err) => {
        console.error(`Database channel error (${this.name}):`, err)
        if (!this.destroyed) {
          // Attempt to reconnect after a delay
          setTimeout(() => this.setup(), 5000)
        }
      })

      await this.client.query(`LISTEN "${this.name}"`)
    } catch (err) {
      console.error(`Failed to setup channel ${this.name}:`, err)
      if (!this.destroyed) {
        setTimeout(() => this.setup(), 5000)
      }
    }
  }

  async notify(): Promise<void> {
    await sql`NOTIFY ${sql.ref(this.name)}`.execute(this.db.db)
  }

  async destroy(): Promise<void> {
    this.destroyed = true
    this.listener.removeAllListeners()
    if (this.client) {
      try {
        await this.client.query(`UNLISTEN "${this.name}"`)
        // For pg pool clients, we need to call release to return to pool
        ;(this.client as any).release?.()
      } catch (err) {
        console.error(`Error destroying channel ${this.name}:`, err)
      }
      this.client = null
    }
  }
}
