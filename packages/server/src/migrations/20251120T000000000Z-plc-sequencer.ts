import { Kysely, sql } from 'kysely'

export async function up(db: Kysely<any>): Promise<void> {
  // Create sequence for assigning sequence numbers
  await sql`CREATE SEQUENCE plc_seq_sequence`.execute(db)

  // Create plc_seq table
  await db.schema
    .createTable('plc_seq')
    .addColumn('id', 'bigserial', (col) => col.primaryKey())
    .addColumn('seq', 'bigint')
    .addColumn('event', 'jsonb', (col) => col.notNull())
    .addColumn('invalidated', 'integer', (col) => col.notNull().defaultTo(0))
    .addColumn('sequencedAt', 'timestamptz')
    .execute()

  // Create indexes
  await db.schema
    .createIndex('plc_seq_seq_idx')
    .on('plc_seq')
    .column('seq')
    .execute()
}

export async function down(db: Kysely<any>): Promise<void> {
  await db.schema.dropIndex('plc_seq_seq_idx').execute()
  await db.schema.dropTable('plc_seq').execute()
  await sql`DROP SEQUENCE plc_seq_sequence`.execute(db)
}
