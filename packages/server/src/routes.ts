import { CID } from 'multiformats/cid'
import express from 'express'
import { WebSocketServer, WebSocket } from 'ws'
import * as plc from '@did-plc/lib'
import { ServerError } from './error'
import { AppContext } from './context'
import { assertValidIncomingOp } from './constraints'
import { timingSafeStringEqual } from './util'
import { Outbox } from './sequencer'

export const createRouter = (ctx: AppContext): express.Router => {
  const router = express.Router()

  router.get('/', async function (req, res) {
    // HTTP temporary redirect to project homepage
    res.redirect(302, 'https://web.plc.directory')
  })

  router.get('/_health', async function (req, res) {
    const { db, version } = ctx
    try {
      await db.healthCheck()
    } catch (err) {
      req.log.error(err, 'failed health check')
      return res.status(503).send({ version, error: 'Service Unavailable' })
    }
    res.send({ version })
  })

  // Export ops in the form of paginated json lines
  router.get('/export', async function (req, res) {
    const parsedCount = req.query.count ? parseInt(req.query.count, 10) : 1000
    if (isNaN(parsedCount) || parsedCount < 1) {
      throw new ServerError(400, 'Invalid count parameter')
    }
    const count = Math.min(parsedCount, 1000)
    const after = req.query.after ? new Date(req.query.after) : undefined
    const ops = await ctx.db.exportOps(count, after)
    res.setHeader('content-type', 'application/jsonlines')
    res.status(200)
    for (let i = 0; i < ops.length; i++) {
      if (i > 0) {
        res.write('\n')
      }
      const line = JSON.stringify(ops[i])
      res.write(line)
    }
    res.end()
  })

  // TODO: init elsewhere?
  const wss = new WebSocketServer({
    noServer: true,
  })

  // Stream sequenced operations over WebSocket
  router.get('/export/stream', async function (req, res) {
    if (!req.headers.upgrade) {
      throw new ServerError(426, 'upgrade required')
    }

    // Parse cursor parameter for backfill
    const cursor = req.query.cursor ? parseInt(req.query.cursor, 10) : undefined
    if (cursor !== undefined && (isNaN(cursor) || cursor < 0)) {
      throw new ServerError(400, 'Invalid cursor parameter')
    }

    req.ws.handled = true
    wss.handleUpgrade(
      req,
      req.ws.socket,
      req.ws.head,
      async function (ws: WebSocket) {
        const abortController = new AbortController()
        const outbox = new Outbox(ctx.sequencer)

        ws.on('close', () => {
          abortController.abort()
        })

        ws.on('error', (err) => {
          req.log.error({ err }, 'websocket error')
          abortController.abort()
        })

        try {
          for await (const evt of outbox.events(cursor, abortController.signal)) {
            if (ws.readyState !== WebSocket.OPEN) {
              break
            }
            // Send event as JSON line
            ws.send(JSON.stringify(evt))
          }
        } catch (err) {
          if (!abortController.signal.aborted) {
            req.log.error({ err }, 'error streaming events')
          }
        } finally {
          if (ws.readyState === WebSocket.OPEN) {
            ws.close()
          }
        }
      },
    )
  })

  // Get data for a DID document
  router.get('/:did', async function (req, res) {
    const { did } = req.params
    const last = await ctx.db.lastOpForDid(did)
    if (!last) {
      throw new ServerError(404, `DID not registered: ${did}`)
    }
    const data = plc.opToData(did, last)
    if (data === null) {
      throw new ServerError(404, `DID not available: ${did}`)
    }
    const doc = await plc.formatDidDoc(data)
    res.type('application/did+ld+json')
    res.send(JSON.stringify(doc))
  })

  // Get data for a DID document
  router.get('/:did/data', async function (req, res) {
    const { did } = req.params
    const last = await ctx.db.lastOpForDid(did)
    if (!last) {
      throw new ServerError(404, `DID not registered: ${did}`)
    }
    const data = plc.opToData(did, last)
    if (data === null) {
      throw new ServerError(404, `DID not available: ${did}`)
    }
    res.json(data)
  })

  // Get operation log for a DID
  router.get('/:did/log', async function (req, res) {
    const { did } = req.params
    const log = await ctx.db.opsForDid(did)
    if (log.length === 0) {
      throw new ServerError(404, `DID not registered: ${did}`)
    }
    res.json(log)
  })

  // Get operation log for a DID
  router.get('/:did/log/audit', async function (req, res) {
    const { did } = req.params
    const ops = await ctx.db.indexedOpsForDid(did, true)
    if (ops.length === 0) {
      throw new ServerError(404, `DID not registered: ${did}`)
    }
    const log = ops.map((op) => ({
      ...op,
      cid: op.cid.toString(),
      createdAt: op.createdAt.toISOString(),
    }))

    res.json(log)
  })

  // Get the most recent operation in the log for a DID
  router.get('/:did/log/last', async function (req, res) {
    const { did } = req.params
    const last = await ctx.db.lastOpForDid(did)
    if (!last) {
      throw new ServerError(404, `DID not registered: ${did}`)
    }
    res.json(last)
  })

  // Update or create a DID doc
  router.post('/:did', async function (req, res) {
    const { did } = req.params
    const op = req.body
    assertValidIncomingOp(op)
    await ctx.db.validateAndAddOp(did, op, new Date())
    res.sendStatus(200)
  })

  // We only have one admin endpoint, so an auth middleware would probably be overkill
  router.post('/admin/removeInvalidOps', async function (req, res) {
    const { adminSecret, did, cid } = req.body

    // admin auth
    if (!ctx.adminSecret) {
      throw new ServerError(401, 'admin secret has not been configured')
    }
    if (!timingSafeStringEqual(adminSecret, ctx.adminSecret)) {
      throw new ServerError(401, 'invalid admin secret')
    }

    const removedOps = await ctx.db.removeInvalidOps(did, cid)
    res.json(removedOps)
  })

  return router
}

export default createRouter
