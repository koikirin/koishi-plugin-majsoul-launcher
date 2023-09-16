import cluster from 'cluster'
import http from 'http'
import path from 'path'
import axios from 'axios'
import { MongoClient } from 'mongodb'
import process from 'process'
import MJSoul from 'mjsoul'
import { HttpsProxyAgent } from 'https-proxy-agent'
import crypto from 'crypto'
import pb from 'protobufjs'
import type { CAC } from 'cac'

declare module 'mjsoul' {
  interface Config {
    root?: pb.Root
    wrapper?: pb.Type
  }

  namespace MJSoul {
    export type Res<T> = {
      error?: {
        code: number
      }
    } & T

    export interface CustomizedContest {
      unique_id: number
      creator_id: number
      contest_id: number
      contest_number: string
      state: number
      create_time: number
      start_time: number
      finish_time: number
      open: boolean
      rank_rule: number
      deadline: number
      auto_match: boolean
      auto_disable_end_chat: boolean
      contest_type: number
      hidden_zones: number[]
      banned_zones: number[]
      observer_switch: number
      emoji_switch: number
      player_roster_type: number
    }

    export interface GameLiveHead {
      uuid: string
      start_time: number
      game_config: any
      players: PlayerGameView[]
      seat_list: number[]
    }

    export interface PlayerGameView {
      account_id: number
      nickname: string
    }

    export interface CustomizedContestAbstract {
      unique_id: number
      contest_id: number
      contest_name: string
      open: boolean
    }
  }
}

interface BaseAccount {
  enabled: boolean
  version_url: string
  gateway: string
  comment?: string
}

interface ExtendAccount {
  bind_worker?: number
}

type Account = BaseAccount & ExtendAccount & ({
  type: 0
  account: string
  password?: string
} | {
  type: 10
  access_token: string
})

export interface Config {
  accounts: Account[]
  database?: {
    uri: string
  }
  timeout: number
  num_workers: number
  port: number
  host: string
  proxy?: string
}

namespace Message {
  interface BaseMessage {
    action: string
  }

  export interface StartMessage extends BaseMessage {
    action: 'start'
    accountIndex?: number
  }

  export interface StateMessage extends BaseMessage {
    action: 'state'
    state: any
  }
}

type Message = Message.StartMessage | Message.StateMessage

interface PrimaryState {
  account_indexs: Record<number, number>
  maintaince?: boolean
  last_active_time: number
}

interface WorkerState {
  maintaince?: boolean
  should_exit?: boolean
  last_active_time: number
  is_reconnecting: boolean
  timeout_retries: number
  timeout_retries_max: number
}

let $options: {
  config: string
  descriptor: string
}

try {
  const cli: CAC = require('cac')()
  cli.command('')
    .option('-c, --config [config]', 'Config file', { default: path.resolve(process.cwd(), './data/majsoul/gatewayconfig.json') })
    .option('-d, --descriptor [descriptor]', 'ProtobufJS descriptor', { default: './liqi.json' })
  $options = cli.parse().options as any
} catch {
  $options = {
    config: path.resolve(process.cwd(), './data/majsoul/gatewayconfig.json'),
    descriptor: './liqi.json',
  }
}

const config: Config = require($options.config)

if (cluster.isPrimary) {
  const state: PrimaryState = {
    last_active_time: -1,
    maintaince: false,
    account_indexs: {},
  }

  function log(message: any, ...optionalParams: any) {
    console.log(`[M] ${message}`, ...optionalParams)
  }

  function getAccountIndex(workerId: number) {
    for (const [i, accountInfo] of config.accounts.entries()) {
      if (!accountInfo.enabled) continue
      if (!accountInfo.bind_worker) {
        accountInfo.bind_worker = workerId
        state.account_indexs[workerId] = i
        return i
      }
    }
    return -1
  }

  console.log(`Primary ${process.pid} is running`)

  for (let i = 0; i < config.num_workers && i < config.accounts.length; i++) {
    const worker = cluster.fork()
    worker.on('message', msg => {
      if (msg.state) {
        state[worker.id] = msg.state
      } else if (msg.action === 'start') {
        worker.send({
          action: 'start',
          accountIndex: getAccountIndex(worker.id),
        })
      }
    })
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`worker [${worker.id}:${worker.process.pid}] died`)
    // Should never died until full restart
    // state[worker.id].alive = false
    const accountIndex = state.account_indexs[worker.id]
    if (accountIndex !== undefined && accountIndex !== null && accountIndex !== -1) {
      log('Worker is going to be restart')
      config.accounts[accountIndex].bind_worker = null
      setTimeout(() => {
        const worker = cluster.fork()
        worker.on('message', (msg: Message) => {
          if (msg.action === 'state') {
            state[worker.id] = msg.state
          } else if (msg.action === 'start') {
            worker.send({
              action: 'start',
              accountIndex: getAccountIndex(worker.id),
            })
          }
        })
      }, 1000 * 60 * 10)
    }
  })
} else {
  function log(message: any, ...optionalParams: any) {
    console.log(`[${cluster.worker.id}] ${message}`, ...optionalParams)
  }

  let accountInfo = null
  let mjsoul: MJSoul = null

  const state: WorkerState = {
    is_reconnecting: false,
    timeout_retries: 0,
    timeout_retries_max: 10,
    last_active_time: -1,
    maintaince: false,
    should_exit: false,
  }

  const loginConfig = {
    client_version: '',
    client_version_string: '',
  }

  const root = pb.Root.fromJSON(require($options.descriptor))
  const wrapper = root.lookupType('Wrapper')

  let client: MongoClient = null
  if (config.database) {
    client = new MongoClient(config.database.uri)
    client.connect()
  } else {
    log('Running without database')
  }

  async function initMjsoul(accountIndex) {
    accountInfo = config.accounts[accountIndex]
    if (accountInfo.enabled) { log(`Initialized with account [${accountIndex}]${accountInfo.account || accountInfo.access_token}`) } else {
      log(`Account [${accountIndex}]${accountInfo.account} disabled, exit.`)
      return
    }

    mjsoul = new MJSoul({
      url: accountInfo.gateway,
      timeout: config.timeout || 5000,
      wsOption: config.proxy ? { agent: new HttpsProxyAgent(config.proxy) } : undefined,
      root,
      wrapper,
    })
    mjsoul.on('NotifyAccountLogout', login)
    mjsoul.on('error', async err => {
      log('error: ', err)
    })
    mjsoul.on('close', async () => {
      mjsoul.open(login)
    })
    mjsoul.open(login)
  }

  async function login() {
    try {
      if (await checkVersion()) return
      let data: any
      if (accountInfo.access_token) {
        data = await mjsoul.sendAsync('oauth2Login', {
          currency_platforms: [2, 6, 8, 10, 11],
          access_token: accountInfo.access_token,
          reconnect: false,
          device: {
            platform: 'pc',
            hardware: 'pc',
            os: 'windows',
            os_version: 'win10',
            is_browser: true,
            software: 'Chrome',
            sale_platform: 'web',
          },
          random_key: crypto.randomUUID(),
          client_version: { resource: loginConfig.client_version },
          client_version_string: loginConfig.client_version_string,
          type: accountInfo.type,
        })
      } else {
        data = await mjsoul.sendAsync('login', {
          currency_platforms: [2, 6, 8, 10, 11],
          account: accountInfo.account,
          password: mjsoul.hash(accountInfo.password),
          reconnect: false,
          device: {
            platform: 'pc',
            hardware: 'pc',
            os: 'windows',
            os_version: 'win10',
            is_browser: true,
            software: 'Chrome',
            sale_platform: 'web',
          },
          random_key: crypto.randomUUID(),
          client_version: { resource: loginConfig.client_version },
          client_version_string: loginConfig.client_version_string,
          gen_access_token: true,
          type: accountInfo.type,
        })
      }
      log('Login', data)
      state.maintaince = false
    } catch (e) {
      log('Login failed', e.error)
      state.should_exit = false
      if (e.error && e.error.code === 103) {
        state.maintaince = true
        state.should_exit = true
      } else state.maintaince = false
      if (e.error && e.error.code === 151) {
        log('Current version outdated')
        // state.should_exit = true
      }
    }
    state.is_reconnecting = false
    process.send({ state })
    // Kill self
    if (state.should_exit) {
      process.exit(1)
    }
  }

  async function checkVersion(restart = true) {
    log(`${Date()} checking client version from ${loginConfig.client_version}`)
    try {
      const data = (await axios.get(`${accountInfo.version_url}?randv=${(new Date()).valueOf()}`)).data
      if (data.version !== loginConfig.client_version) {
        const version = data.version.slice(0, data.version.length - 2)
        loginConfig.client_version = `${version}.w`
        loginConfig.client_version_string = `web-${version}`
        log(`Update client version to ${version}`)
        if (restart) mjsoul.close()
        return true
      }
    } catch (e) { log(e) }
    return false
  }

  function buildError(code: number, msg: string, extra = {}) {
    return {
      err: true,
      msg,
      code,
      extra,
    }
  }

  const nullError = buildError(0, '')

  function reconnect(canReconnect = true) {
    if (!canReconnect || state.is_reconnecting) return
    log(`Reconnecting... (${state.timeout_retries} / ${state.timeout_retries_max})`)
    // state.is_reconnecting = true
    state.timeout_retries = 0
    mjsoul.close()
  }

  async function getLivelistByFilterid(id, canReconnect = true) {
    let res: MJSoul.MJSoul.Res<{
      live_list: MJSoul.MJSoul.GameLiveHead[]
    }>
    // Normal room(rank, event, etc)
    try {
      res = await mjsoul.sendAsync('fetchGameLiveList', { filter_id: id })
    } catch (e) {
      // log("Error when GetLiveList: ", e)
      if (e.error.code === 2 && ++state.timeout_retries < state.timeout_retries_max) throw buildError(2, 'Invalid room', { id })
      if (e.error.code > 9000 && ++state.timeout_retries < state.timeout_retries_max) throw nullError
      reconnect(canReconnect)
      throw buildError(-1, 'getLivelistByFilterid failed', {
        id, ...state,
      })
    }
    return res.live_list
  }

  async function getLivelistByUniqueid(id, canReconnect = true) {
    let res: MJSoul.MJSoul.Res<{
      live_list: MJSoul.MJSoul.GameLiveHead[]
    }>
    // Contest Unique_ID
    try {
      res = await mjsoul.sendAsync('fetchCustomizedContestGameLiveList', { unique_id: id })
    } catch (e) {
      if (e.error.code > 9000 && ++state.timeout_retries < state.timeout_retries_max) throw nullError
      reconnect(canReconnect)
      throw buildError(-1, 'getLivelistByUniqueid failed', {
        id, ...state,
      })
    }
    return res.live_list
  }

  async function getContestUniqueId(id, canReconnect = true) {
    let res: MJSoul.MJSoul.Res<{
      contest_info: MJSoul.MJSoul.CustomizedContestAbstract
    }>
    try {
      res = await mjsoul.sendAsync('fetchCustomizedContestByContestId', { contest_id: id })
    } catch (e) {
      // Room not exists
      if (e.error.code === 2501) {
        throw buildError(2501, 'Room not exist', {
          id,
        })
      }
      // log("Error when GetLiveList: ", e)
      if (e.error.code > 9000 && ++state.timeout_retries < state.timeout_retries_max) throw nullError
      reconnect(canReconnect)
      throw buildError(-1, 'GetLiveList failed', {
        id, ...state,
      })
    }
    return res.contest_info.unique_id
  }

  function regularizeWgObject(wg: any) {
    const live = JSON.parse(JSON.stringify(wg))
    delete live.game_config
    for (const player of live.players) {
      delete player.character
      delete player.views
      delete player.avatar_frame
      delete player.verified
    }
    return live
  }

  async function UpdateLiveList(id, canReconnect = true) {
    let livelist: MJSoul.MJSoul.GameLiveHead[]
    let uid = id
    if (id < 1000) {
      livelist = await getLivelistByFilterid(id, canReconnect)
    } else {
      uid = await getContestUniqueId(id, canReconnect)
      livelist = await getLivelistByUniqueid(uid)
    }

    if (client) {
      const collection = client.db().collection('majsoul')
      // log(`Fetched ${id} : ${livelist.length} records`)

      const documents = []
      const aidmap: Record<number, [string, number]> = {}
      // Update to database

      for (const live of livelist) {
        const starttime = live.start_time + 300
        documents.push({
          _id: live.uuid,
          // uuid: live.uuid,
          fid: String(id),
          uid: String(uid),
          wg: regularizeWgObject(live),
          starttime,
        })
        live.players.forEach(p => {
          aidmap[p.account_id] = [p.nickname, live.start_time]
        })
      }
      if (documents.length > 0) {
        try {
          await collection.insertMany(documents, {
            ordered: false,
          })
        } catch (e) { }
      }
      // Update AID -> [NICKNAME, UPDATE_TIME] Table
      if (Object.keys(aidmap).length > 0) {
        const bulk = client.db('majsoul').collection('account_map').initializeUnorderedBulkOp()
        Object.entries(aidmap).map(([aid, [nickname, starttime]]) => {
          bulk.find({ _id: parseInt(aid) }).upsert().updateOne({
            '$set': {
              _id: parseInt(aid),
              nickname,
              starttime,
            },
          })
        })
        try {
          await bulk.execute()
        } catch (e) { console.log(e) }
      }
    }

    return livelist
  }

  async function GetOBToken(uuid, canReconnect = true) {
    try {
      const res = await mjsoul.sendAsync('fetchOBToken', { uuid })
      log(`GetOBToken ${uuid} ${JSON.stringify(res)}`)
      return res
    } catch (e) {
      if (e.error.code === 1803) {
        throw buildError(1803, 'OB not ready', {
          uuid,
        })
      }
      log('Error when GetOBToken: ', e)
      if (e.error.code > 9000 && ++state.timeout_retries < state.timeout_retries_max) throw nullError
      reconnect(canReconnect)
      throw nullError
    }
  }

  async function GetGameRecordHead(uuid, canReconnect = true) {
    try {
      let res = await mjsoul.sendAsync('fetchGameRecord', {
        game_uuid: uuid,
        client_version_string: loginConfig.client_version_string,
      })
      res = {
        error: res.error,
        head: res.head,
        data: null,
        data_url: res.data_url,
      }
      log(`GetGameRecordHead ${uuid}`)
      return res
    } catch (e) {
      if (e.error.code === 1203) {
        throw buildError(1203, 'Game not finish or invalid', {
          uuid,
        })
      }
      log('Error when GetGameRecordHead: ', e)
      if (e.error.code > 9000 && ++state.timeout_retries < state.timeout_retries_max) throw nullError
      reconnect(canReconnect)
      throw nullError
    }
  }

  async function GetGameRecord(uuid, canReconnect = true) {
    try {
      let res = await mjsoul.sendAsync('fetchGameRecord', {
        game_uuid: uuid,
        client_version_string: loginConfig.client_version_string,
      })
      const data = wrapper.decode(res.data) as any
      data.data = root.lookupType(data.name).decode(data.data)
      data.data.actions = data.data.actions.map(ac => {
        if (ac.type === 1) {
          ac.result = wrapper.decode(ac.result)
          ac.result = {
            name: ac.result.name,
            data: root.lookupType(ac.result.name).decode(ac.result.data),
          }
          // ac.result.data = root.lookupType(ac.result.name).decode(ac.result.data)
        }
        return { ...ac }
      })
      res = {
        error: res.error,
        head: res.head,
        data: {
          records: data.data.records,
          version: data.data.version,
          actions: data.data.actions,
          bar: data.data.bar,
        },
        data_url: res.data_url,
      }
      log(`GetGameRecord ${uuid}`)
      return res
    } catch (e) {
      if (e.error.code === 1203) {
        throw buildError(1203, 'Game not finish or invalid', {
          uuid,
        })
      }
      log('Error when GetGameRecord: ', e)
      if (e.error.code > 9000 && ++state.timeout_retries < state.timeout_retries_max) throw nullError
      reconnect(canReconnect)
      throw nullError
    }
  }

  const server = http.createServer(async (request, res) => {
    try {
      const url = new URL('http://localhost' + request.url)
      const canReconnect = url.searchParams.get('reconnect') === null
      if (request.url.startsWith('/livelist')) {
        const id = url.searchParams.get('id')
        // log("Receive request:", id)
        const result = await UpdateLiveList(id, canReconnect)
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.write(JSON.stringify(result))
        res.end()
      } else if (request.url.startsWith('/token')) {
        const uuid = url.searchParams.get('uuid')
        const result = await GetOBToken(uuid, canReconnect)
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.write(JSON.stringify(result))
        res.end()
      } else if (request.url.startsWith('/execute')) {
        const func = url.searchParams.get('func')
        const data = JSON.parse(url.searchParams.get('data'))
        const result = await mjsoul.sendAsync(func, data)
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.write(JSON.stringify(result))
        res.end()
      } else if (request.url.startsWith('/update')) {
        checkVersion()
        res.writeHead(204)
        res.end()
      } else if (request.url.startsWith('/relogin')) {
        reconnect()
        res.writeHead(204)
        res.end()
      } else if (request.url.startsWith('/paipu_head')) {
        const uuid = url.searchParams.get('uuid')
        const result = await GetGameRecordHead(uuid, canReconnect)
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.write(JSON.stringify(result))
        res.end()
      } else if (request.url.startsWith('/paipu')) {
        const uuid = url.searchParams.get('uuid')
        const result = await GetGameRecord(uuid, canReconnect)
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.write(JSON.stringify(result))
        res.end()
      } else {
        res.writeHead(404)
        res.end()
      }
    } catch (e) {
      // if (e) log(e)
      if (e && e.err) {
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.write(JSON.stringify(e))
        res.end()
      } else {
        const err = buildError(-2, 'Unknown faled', {
          err_type: `${e}`,
          ...state,
        })
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.write(JSON.stringify(err))
        res.end()
      }
    }
  })

  process.on('message', async (msg: Message) => {
    if (msg.action === 'start') {
      if (msg.accountIndex === -1) {
        log('Invalid account index, exit.')
        process.exit(1)
      } else {
        await initMjsoul(msg.accountIndex)
        server.listen(config.port, config.host)
        console.log(`Gateway Worker [${cluster.worker.id}:${process.pid}] is running at : ${config.port}`)
      }
    }
  })

  process.send({
    action: 'start',
  })
}
