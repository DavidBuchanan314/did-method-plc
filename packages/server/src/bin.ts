import './env'
import { Database, PlcDatabase } from './db'
import PlcServer from '.'
import { SequencerLeader } from './sequencer'

const run = async () => {
  const dbUrl = process.env.DATABASE_URL

  let db: PlcDatabase
  let leader: SequencerLeader | undefined
  if (dbUrl) {
    const pgDb = Database.postgres({ url: dbUrl })
    await pgDb.migrateToLatestOrThrow()
    db = pgDb

    // Start sequencer leader
    leader = new SequencerLeader(pgDb)
    leader.run().catch((err) => {
      console.error('Sequencer leader error:', err)
    })
    console.log('[*] Sequencer leader started')
  } else {
    db = Database.mock()
  }

  const envPort = parseInt(process.env.PORT || '')
  const port = isNaN(envPort) ? 2582 : envPort
  const adminSecret = process.env.ADMIN_SECRET || undefined

  const plc = PlcServer.create({ db, port, adminSecret })
  await plc.start()
  console.log(`👤 PLC server is running at http://localhost:${port}`)

  // Graceful shutdown
  const shutdown = async () => {
    console.log('\nShutting down...')
    leader?.destroy()
    await plc.destroy()
    process.exit(0)
  }

  process.on('SIGINT', shutdown)
  process.on('SIGTERM', shutdown)
}

run()
