import { PlcDatabase } from './db'
import { Sequencer } from './sequencer'

export class AppContext {
  constructor(
    private opts: {
      db: PlcDatabase
      sequencer: Sequencer
      version: string
      port?: number
      adminSecret?: string
    },
  ) {}

  get db() {
    return this.opts.db
  }

  get sequencer() {
    return this.opts.sequencer
  }

  get version() {
    return this.opts.version
  }

  get port() {
    return this.opts.port
  }

  get adminSecret() {
    return this.opts.adminSecret
  }
}

export default AppContext
