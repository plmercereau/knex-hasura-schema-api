import knex, { Knex } from 'knex'
import fetch from 'cross-fetch'
import map from 'lodash.map'
export const transactionCommands = [
  'BEGIN;',
  'COMMIT;',
  'ROLLBACK',
  'SAVEPOINT',
  'RELEASE SAVEPOINT'
]

export interface RawQuery {
  method: 'select' | 'insert' | 'update' | 'delete' | 'any'
  sql: string
  postOp?: 'first' | 'pluck'
  pluck?: string
  bindings: any[]
  options: Record<string, any>
  timeout: boolean
  cancelOnTimeout: boolean
  __knexQueryUid: string
  queryContext: any
}

export class HasuraSchemaApiClient extends knex.Client {
  private endpoint: string
  private secret?: string
  constructor(config: Knex.Config) {
    super(config)
    const { connection } = config
    if (typeof connection !== 'object') {
      throw Error('connection must be an object')
    }
    if (!('host' in connection) || !connection.host) {
      throw Error('connection.host must set as the main Hasura endpoint')
    }
    const { host, password } = connection

    this.endpoint = `${host}/v2/query`
    if (typeof password === 'string') {
      this.secret = password
    }
    if (config.dialect) {
      this._attachDialectQueryCompiler(config)
    }
  }

  public acquireConnection() {
    return Promise.resolve({
      __knexUid: 1,
      beginTransaction(cb: () => void) {
        cb()
      },
      commitTransaction(cb: () => void) {
        cb()
      }
    })
  }

  public releaseConnection() {
    return Promise.resolve()
  }

  public processResponse(response: any) {
    return response
  }

  query(connection: any, queryParam: any) {
    return super
      .query(connection, {
        ...queryParam,
        sql: this.raw(queryParam.sql, queryParam.bindings).toString(),
        bindings: []
      })
      .then((resp: any) => {
        if (queryParam.output) {
          return queryParam.output.call(this, resp)
        }
        if (queryParam.method === 'raw') return resp
        const { returning } = queryParam
        if (resp.command === 'SELECT') {
          if (queryParam.method === 'first') return resp.rows[0]
          if (queryParam.method === 'pluck')
            return map(resp.rows, queryParam.pluck)
          return resp.rows
        }
        if (returning) {
          const returns = []
          for (let i = 0, l = resp.rows.length; i < l; i++) {
            const row = resp.rows[i]
            returns[i] = row
          }
          return returns
        }
        if (resp.command === 'UPDATE' || resp.command === 'DELETE') {
          return resp.rowCount
        }
        return resp
      })
  }

  public async _query(_connection: any, { bindings, sql, method }: RawQuery) {
    if (
      typeof method === 'undefined' &&
      transactionCommands.some(trxCommand => sql.startsWith(trxCommand))
    ) {
      return undefined
    }
    const query = super.raw(sql).toString()
    const headers: HeadersInit = {
      'Content-Type': 'application/json'
    }
    if (this.secret) {
      headers['x-hasura-admin-secret'] = this.secret
    }
    const res = await fetch(this.endpoint, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        type: 'run_sql',
        args: {
          source: 'default',
          sql: query
        }
      })
    })
    if (!res.ok) {
      throw Error(`Error: ${res.status} ${res.statusText}`)
    }
    // ? https://node-postgres.com/api/result
    const { result } = await res.json()
    // TODO this is probably incorrect e.g. 'pluck' and 'first'
    const command = method?.toUpperCase() || sql.split(' ')[0].toUpperCase()
    if (Array.isArray(result)) {
      const [fields, ...rows] = result
      return {
        command,
        rowCount: rows.length,
        oid: null,
        rows: rows.map(row =>
          fields.reduce(
            (acc: Record<string, any>, field: string, i: number) => {
              acc[field] = row[i]
              return acc
            },
            {}
          )
        ),
        fields: fields.map((field: string, id: number) => ({
          // ? not clear what the syntax is, and if it's important
          name: field,
          // tableID: 13609,
          columnID: id + 1
          // dataTypeID: 19,
          // dataTypeSize: 64,
          // dataTypeModifier: -1,
          // format: 'text'
        })),
        RowCtor: null
      }
    } else {
      // * not an array but 'null' when executing commands like 'INSERT' or 'CREATE'
      if (result !== null) {
        // TODO not sure if it can happen
        console.warn('Unexpected result', result)
      }
      return {
        command,
        rowCount: 0,
        oid: null,
        rows: [],
        fields: [],
        RowCtor: null,
        rowAsArray: false // ? not sure what this is
      }
    }
  }

  private _attachDialectQueryCompiler(config: Knex.Config<any>) {
    const { resolveClientNameWithAliases } = require('knex/lib/util/helpers')
    const { SUPPORTED_CLIENTS } = require('knex/lib/constants')

    if (!SUPPORTED_CLIENTS.includes(config.dialect)) {
      throw new Error(
        `knex-mock-client: Unknown configuration option 'dialect' value ${config.dialect}.\nNote that it is case-sensitive, check documentation for supported values.`
      )
    }

    const resolvedClientName = resolveClientNameWithAliases(config.dialect)
    const Dialect = require(`knex/lib/dialects/${resolvedClientName}/index.js`)
    const dialect = new Dialect(config)

    Object.setPrototypeOf(this.constructor.prototype, dialect) // make the specific dialect client to be the prototype of this class.
  }
}
