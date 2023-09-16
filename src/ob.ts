import cluster from 'cluster'
import path from 'path'
import { WebSocket, WebSocketServer } from 'ws'
import url from 'url'
import process from 'process'
import { HttpsProxyAgent } from 'https-proxy-agent'
import pb from 'protobufjs'
import { CAC } from 'cac'

type WebSocketEx = WebSocket & {
  token: string
  tag: string
  obws: WebSocket
}

export interface Config {
  servers: string[]
  num_workers: number
  port: number
  host: string
  proxy?: string
}

let $options: {
  config: string
  descriptor: string
}

try {
  const cli: CAC = require('cac')()
  cli.command('')
    .option('-c, --config [config]', 'Config file', { default: path.resolve(process.cwd(), './data/majsoul/obconfig.json') })
    .option('-d, --descriptor [descriptor]', 'ProtobufJS descriptor', { default: './liqi.json' })
  $options = cli.parse().options as any
} catch {
  $options = {
    config: path.resolve(process.cwd(), './data/majsoul/obconfig.json'),
    descriptor: './liqi.json',
  }
}

const config: Config = require($options.config)

if (cluster.isPrimary) {
  console.log(`Primary ${process.pid} is running`)

  for (let i = 0; i < config.num_workers; i++) {
    cluster.fork()
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`worker [${worker.id}:${worker.process.pid}] died`)
    setTimeout(() => cluster.fork(), 30000)
  })
} else {
  const root = pb.Root.fromJSON(require($options.descriptor))
  const wrapper = root.lookupType('Wrapper')

  const wss = new WebSocketServer({ port: config.port, host: config.host })

  function getMatchDescription(raw: any) {
    const data = JSON.parse(raw)
    if (data.error) throw data
    const head = JSON.parse(data.head)
    let ret = `${head.uuid} ${data.start_time}`
    for (const player of head.players) {
      ret += ` ${player.nickname},`
    }
    return ret
  }

  const state = {
    server_index: 0,
  }

  function getObServerAddress() {
    state.server_index = (state.server_index + 1) % config.servers.length
    return config.servers[state.server_index]
  }

  wss.on('connection', function connection(ws: WebSocketEx, req) {
    const parameters = new url.URL(req.url)
    ws.token = parameters.searchParams.get('token')
    ws.tag = parameters.searchParams.get('tag') || ''
    ws.obws = new WebSocket(getObServerAddress(), config.proxy ? { agent: new HttpsProxyAgent(config.proxy) } : undefined)
    console.log(`[${cluster.worker.id}][${ws.tag}] Receive OB Request ${ws.token} using ${state.server_index}`)
    ws.obws.on('open', function open() {
      const req1 = `<= Auth 1 {"token":"${ws.token}"}`
      const req2 = '<= StartOb 2 {}'
      ws.obws.send(req1)
      ws.obws.send(req2)
    })
    ws.obws.on('close', function close() {
      ws.obws.close()
      ws.close(4001)
      // console.log(`[${cluster.worker.id}][${ws.tag}] Closed`)
    })
    ws.obws.on('error', function error(e) {
      ws.obws.close()
      ws.close(4002)
      console.log(`[${cluster.worker.id}][${ws.tag}] Failed ${e}`)
    })
    ws.obws.on('message', function message(data: Buffer) {
      // console.log(data.subarray(0, 1).toString())
      try {
        if (data.subarray(0, 2).toString() === '=>') {
          if (data.subarray(0, 5).toString() === '=> 1 ') {
            console.log(`[${cluster.worker.id}][${ws.tag}] Fetched:`, getMatchDescription(data.subarray(5)))
          } else {
            console.log(`[${cluster.worker.id}][${ws.tag}]`, data.toString().slice(0, 100))
          }
          ws.send(JSON.stringify({
            name: 'ob_init',
            seq: 0,
            data: data.slice(5).toString(),
          }))
          return
        }
      } catch (e) {
        console.log(`[${cluster.worker.id}][${ws.tag}] Failed to init`, e)
        ws.obws.close()
        setTimeout(() => ws.close(4003), 1000)
        return
      }
      let decodeData: any
      try {
        const seq = data[0] + data[1] * 256
        decodeData = wrapper.decode(data.slice(14))
        decodeData.data = root.lookupType(decodeData.name).decode(decodeData.data)
        ws.send(JSON.stringify({
          name: decodeData.name,
          seq,
          data: decodeData.data.toJSON(),
        }))
      } catch (e) {
        console.log(`[${cluster.worker.id}][${ws.tag}] What hell`, e, data.toString())
        return
      }
      if (decodeData.name === '.lq.GameEndAction') {
        ws.obws.close()
        setTimeout(() => {
          ws.close(4003)
          // console.log(`[${cluster.worker.id}][${ws.tag}] Finished`)
        }, 10 * 1000)
      }
    })
  })

  console.log(`OB Worker [${cluster.worker.id}:${process.pid}] is running at : ${config.port}`)
}
