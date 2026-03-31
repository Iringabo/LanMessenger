const crypto = require('crypto');
const sqlite3 = require('sqlite3').verbose();

const db = new sqlite3.Database('./chat.db');

function dbExec(sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (err) => {
      if (err) {
        reject(err);
        return;
      }
      resolve();
    });
  });
}

function dbRun(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(err) {
      if (err) {
        reject(err);
        return;
      }
      resolve(this);
    });
  });
}

function dbAll(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(rows || []);
    });
  });
}

async function addColumnIfMissing(tableName, columnName, columnSql) {
  const rows = await dbAll(`PRAGMA table_info(${tableName})`);
  if (rows.some((row) => row.name === columnName)) {
    return false;
  }

  await dbRun(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnSql}`);
  return true;
}

function createInviteCode() {
  return crypto.randomBytes(4).toString('hex');
}

const ready = (async () => {
  await dbExec(`
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS Users(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT UNIQUE,
      password_hash TEXT NOT NULL DEFAULT '',
      last_seen TEXT,
      display_name TEXT,
      status_message TEXT,
      avatar_path TEXT
    );

    CREATE TABLE IF NOT EXISTS Messages(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sender TEXT,
      receiver TEXT,
      group_id INTEGER,
      message TEXT,
      file_path TEXT,
      file_name TEXT,
      message_type TEXT NOT NULL DEFAULT 'text',
      timestamp TEXT,
      seen INTEGER DEFAULT 0,
      edited_at TEXT,
      deleted_at TEXT,
      deleted_by TEXT,
      original_message TEXT
    );

    CREATE TABLE IF NOT EXISTS Groups(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT,
      creator TEXT,
      is_public INTEGER NOT NULL DEFAULT 1,
      invite_code TEXT
    );

    CREATE TABLE IF NOT EXISTS GroupMembers(
      group_id INTEGER,
      username TEXT,
      role TEXT NOT NULL DEFAULT 'member'
    );

    CREATE TABLE IF NOT EXISTS Sessions(
      token TEXT PRIMARY KEY,
      username TEXT NOT NULL,
      created_at TEXT NOT NULL,
      expires_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS RateLimits(
      key TEXT PRIMARY KEY,
      count INTEGER NOT NULL DEFAULT 0,
      reset_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS Reactions(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      message_id INTEGER NOT NULL,
      username TEXT NOT NULL,
      emoji TEXT NOT NULL,
      UNIQUE(message_id, username, emoji)
    );

    CREATE INDEX IF NOT EXISTS idx_messages_private
    ON Messages(sender, receiver, id);

    CREATE INDEX IF NOT EXISTS idx_messages_group
    ON Messages(group_id, id);

    CREATE INDEX IF NOT EXISTS idx_group_members
    ON GroupMembers(group_id);

    CREATE UNIQUE INDEX IF NOT EXISTS idx_group_members_unique
    ON GroupMembers(group_id, username);

    CREATE INDEX IF NOT EXISTS idx_sessions_username
    ON Sessions(username);

    CREATE INDEX IF NOT EXISTS idx_sessions_expires
    ON Sessions(expires_at);
  `);

  await addColumnIfMissing('Users', 'password_hash', "TEXT NOT NULL DEFAULT ''");
  await addColumnIfMissing('Users', 'display_name', 'TEXT');
  await addColumnIfMissing('Users', 'status_message', 'TEXT');
  await addColumnIfMissing('Users', 'avatar_path', 'TEXT');
  await addColumnIfMissing('Messages', 'file_name', 'TEXT');
  await addColumnIfMissing('Messages', 'message_type', "TEXT NOT NULL DEFAULT 'text'");
  await addColumnIfMissing('Messages', 'edited_at', 'TEXT');
  await addColumnIfMissing('Messages', 'deleted_at', 'TEXT');
  await addColumnIfMissing('Messages', 'deleted_by', 'TEXT');
  await addColumnIfMissing('Messages', 'original_message', 'TEXT');
  await addColumnIfMissing('Groups', 'is_public', 'INTEGER NOT NULL DEFAULT 1');
  await addColumnIfMissing('Groups', 'invite_code', 'TEXT');
  await addColumnIfMissing('GroupMembers', 'role', "TEXT NOT NULL DEFAULT 'member'");

  await dbRun(
    `UPDATE Users
     SET display_name = username
     WHERE display_name IS NULL OR display_name = ''`
  );
  await dbRun(`UPDATE Groups SET is_public = 1 WHERE is_public IS NULL`);
  await dbRun(
    `UPDATE GroupMembers
     SET role = 'member'
     WHERE role IS NULL OR role = ''`
  );

  const groupsMissingInvite = await dbAll(
    `SELECT id FROM Groups WHERE invite_code IS NULL OR invite_code = ''`
  );

  for (const group of groupsMissingInvite) {
    await dbRun('UPDATE Groups SET invite_code = ? WHERE id = ?', [
      createInviteCode(),
      group.id
    ]);
  }

  await dbRun(`
    UPDATE GroupMembers
    SET role = 'creator'
    WHERE EXISTS (
      SELECT 1
      FROM Groups g
      WHERE g.id = GroupMembers.group_id
        AND g.creator = GroupMembers.username
    )
  `);
})();

module.exports = { db, ready };
