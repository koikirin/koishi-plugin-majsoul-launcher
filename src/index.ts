import { Context, Logger, Schema, Service } from 'koishi'
import type { Config as OBConfig } from './ob'
import type { Config as GatewayConfig } from './gateway'
import { spawn, spawnSync } from 'child_process'
import { resolve } from 'path'
import strip from 'strip-ansi'
import { mkdir, writeFile } from 'fs/promises'

export class Majsoul {
  static filter = false

  constructor(public ctx: Context, config: Majsoul.Config) {
    ctx.on('ready', async () => {
      await mkdir(resolve(this.ctx.baseDir, 'data/majsoul/'), { recursive: true })
      await writeFile(resolve(ctx.baseDir, 'data/majsoul/gatewayconfig.json'), JSON.stringify(config.gateway))
      await writeFile(resolve(ctx.baseDir, 'data/majsoul/obconfig.json'), JSON.stringify(config.ob))
      try {
        this.start('gateway')
        this.start('ob')
      } catch (e) {
        ctx.logger('majsoul').error(e)
      }
    })
  }

  start(fn: string) {
    const logger = new Logger(`majsoul.${fn}`)

    const p = spawn('node', [resolve(__dirname, `../lib/${fn}.js`)], {
      env: {
        FORCE_TTY: '1',
        ...process.env,
      }
    })

    const handleData = async (data: any) => {
      data = strip(data.toString()).trim()
      if (!data) return
      for (const line of data.split('\n')) {
        logger.info(line)
      }
    }

    p.stdout.on('data', handleData)
    p.stderr.on('data', handleData)

    p.on('error', (error) => {
      logger.warn(error)
    })

    p.on('exit', () => {
      logger.warn('exit')
    })

    this.ctx.collect(fn, () => (p.kill(), true))

    return p
  }
}

export namespace Majsoul {
  export const OBConfig: Schema<OBConfig> = Schema.object({
    servers: Schema.array(Schema.string()).default(['wss://live-hw.maj-soul.net/ob']),
    num_workers: Schema.number().default(1),
    host: Schema.string().default('127.0.0.1'),
    port: Schema.number().default(7237),
    proxy: Schema.string()
  })

  export const GatewayConfig: Schema<GatewayConfig> = Schema.object({
    accounts: Schema.array(Schema.intersect([
      Schema.object({
        enabled: Schema.boolean().default(true),
        version_url: Schema.string().default('https://game.maj-soul.com/1/version.json'),
        gateway: Schema.string().default('wss://gateway-v2.maj-soul.com/gateway'),
        type: Schema.union([0, 10] as const),
      }),
      Schema.union([
        Schema.object({
          type: Schema.const(0).required(),
          account: Schema.string(),
          password: Schema.string().role('secret'),
        }),
        Schema.object({
          type: Schema.const(10).required(),
          access_token: Schema.string(),
        })
      ])
    ])),
    database: Schema.object({
      uri: Schema.string().default('mongodb://127.0.0.1:27017/majob')
    }),
    num_workers: Schema.number().default(2),
    timeout: Schema.number().default(5000),
    host: Schema.string().default('127.0.0.1'),
    port: Schema.number().default(7236),
    proxy: Schema.string()
  })

  export interface Config {
    gateway: GatewayConfig
    ob: OBConfig
  }

  export const Config: Schema<Config> = Schema.object({
    gateway: GatewayConfig,
    ob: OBConfig,
  })
}

export default Majsoul
