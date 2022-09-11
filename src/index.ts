import Knex from 'knex'

import { HasuraSchemaApiClient } from './knex-client'

const run = async (client: ReturnType<typeof Knex>) => {
  const usersTableExists = await client.schema.hasTable('test')
  console.log('usersTableExists', usersTableExists)
  if (!usersTableExists) {
    await client.schema.createTable('test', table => {
      table.uuid('id').primary().defaultTo(client.raw('gen_random_uuid()'))
      table.string('name')
    })
  }

  await client.insert({ name: 'John' }).into('test')
  const rows = await client.select('*').from('test')
  console.log(rows)
  console.log('finished')
  await client.schema.dropTable('test')
  client.destroy()
}

const main = async () => {
  console.log('\nRunning the example with a direct Postgres connection')
  await run(
    Knex({
      client: 'pg',
      connection: 'postgres://postgres:postgres@localhost:5432/postgres'
    })
  )
  console.log('\nRunning the example with the /v2/query Hasura endpoint')
  await run(
    Knex({
      client: HasuraSchemaApiClient,
      dialect: 'pg',
      connection: {
        host: 'http://localhost:1337',
        password: 'nhost-admin-secret'
      }
    })
  )
}

main()
