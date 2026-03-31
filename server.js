require('dotenv').config();
const crypto = require('crypto');
const express = require('express');
const fs = require('fs');
const http = require('http');
const multer = require('multer');
const path = require('path');
const socketio = require('socket.io');
const { db, ready: dbReady } = require('./database');

let compression = null;
try {
  compression = require('compression');
} catch (_err) {
  compression = null;
}

const app = express();
const server = http.createServer(app);

const io = socketio(server, {
  transports: ['websocket'],
  pingInterval: 15000,
  pingTimeout: 10000,
  maxHttpBufferSize: Number(process.env.MAX_FILE_SIZE_MB || 20) * 1024 * 1024
});

const uploadsDir = path.join(__dirname, 'uploads');
fs.mkdirSync(uploadsDir, { recursive: true });

if (compression) {
  app.use(compression());
}
app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public')));
// /uploads is now served via authenticated route below

const socketsByUser = new Map();
const SESSION_TTL_DAYS = Number(process.env.SESSION_TTL_DAYS || 7);
// in-memory maps kept for backward compat but DB-backed rate limiting is used for auth/socket
const authRateLimits = new Map();
const socketRateLimits = new Map();

const allowedMimeTypes = new Set([
  'text/plain',
  'application/pdf',
  'application/msword',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  'image/png',
  'image/jpeg',
  'image/gif',
  'image/webp',
  'audio/webm',
  'audio/ogg',
  'audio/wav',
  'audio/mpeg',
  'audio/mp4'
]);

const avatarMimeTypes = new Set([
  'image/png',
  'image/jpeg',
  'image/gif',
  'image/webp'
]);

const upload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, uploadsDir),
    filename: (_req, file, cb) => {
      const extension = path.extname(file.originalname).toLowerCase().slice(0, 10);
      cb(null, `${Date.now()}-${crypto.randomBytes(4).toString('hex')}${extension}`);
    }
  }),
  limits: { fileSize: Number(process.env.MAX_FILE_SIZE_MB || 20) * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    if (allowedMimeTypes.has(file.mimetype) || file.mimetype.startsWith('audio/')) {
      cb(null, true);
      return;
    }

    cb(new Error('Unsupported file type'));
  }
});

const avatarUpload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, uploadsDir),
    filename: (_req, file, cb) => {
      const extension = path.extname(file.originalname).toLowerCase().slice(0, 10);
      cb(null, `avatar-${Date.now()}-${crypto.randomBytes(4).toString('hex')}${extension}`);
    }
  }),
  limits: { fileSize: Number(process.env.MAX_AVATAR_SIZE_MB || 2) * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    if (avatarMimeTypes.has(file.mimetype)) {
      cb(null, true);
      return;
    }

    cb(new Error('Unsupported avatar type'));
  }
});

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

function dbGet(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) {
        reject(err);
        return;
      }

      resolve(row || null);
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

function checkRateLimit(store, key, limit, windowMs) {
  const now = Date.now();
  const existing = store.get(key);

  if (!existing || existing.resetAt <= now) {
    store.set(key, { count: 1, resetAt: now + windowMs });
    return { allowed: true, retryAfterMs: 0 };
  }

  if (existing.count >= limit) {
    return { allowed: false, retryAfterMs: existing.resetAt - now };
  }

  existing.count += 1;
  return { allowed: true, retryAfterMs: 0 };
}

async function checkRateLimitDb(key, limit, windowMs) {
  const now = Date.now();
  const resetAt = new Date(now + windowMs).toISOString();

  // Upsert: if key missing or expired, reset; otherwise increment
  await dbRun(
    `INSERT INTO RateLimits(key, count, reset_at) VALUES(?, 1, ?)
     ON CONFLICT(key) DO UPDATE SET
       count = CASE WHEN reset_at <= ? THEN 1 ELSE count + 1 END,
       reset_at = CASE WHEN reset_at <= ? THEN ? ELSE reset_at END`,
    [key, resetAt, new Date(now).toISOString(), new Date(now).toISOString(), resetAt]
  );

  const row = await dbGet('SELECT count, reset_at FROM RateLimits WHERE key = ?', [key]);
  if (!row) return { allowed: true, retryAfterMs: 0 };

  const resetMs = new Date(row.reset_at).getTime();
  if (row.count > limit) {
    return { allowed: false, retryAfterMs: Math.max(0, resetMs - now) };
  }
  return { allowed: true, retryAfterMs: 0 };
}

function normalizeUsername(input) {
  return String(input || '').trim();
}

function isValidUsername(username) {
  return /^[a-zA-Z0-9_]{3,20}$/.test(username);
}

function isValidPassword(password) {
  return typeof password === 'string' && password.length >= 6 && password.length <= 72;
}

function normalizeGroupName(name) {
  return String(name || '').trim().slice(0, 40);
}

function sanitizeMessage(message) {
  return String(message || '').trim().slice(0, 3000);
}

function sanitizeDisplayName(value) {
  return String(value || '').trim().slice(0, 40);
}

function sanitizeStatusMessage(value) {
  return String(value || '').trim().slice(0, 140);
}

function sanitizeFilePath(value) {
  const cleaned = path.basename(String(value || '').trim());

  if (!cleaned || cleaned === '.' || cleaned === '..') {
    return '';
  }

  return cleaned.slice(0, 255);
}

function sanitizeFileName(value) {
  return String(value || '').trim().slice(0, 255);
}

function bytesMatch(buffer, signature, offset = 0) {
  if (!buffer || buffer.length < signature.length + offset) {
    return false;
  }
  for (let i = 0; i < signature.length; i += 1) {
    if (buffer[offset + i] !== signature[i]) {
      return false;
    }
  }
  return true;
}

async function readFileHeader(filePath, length = 24) {
  const handle = await fs.promises.open(filePath, 'r');
  try {
    const buffer = Buffer.alloc(length);
    await handle.read(buffer, 0, length, 0);
    return buffer;
  } finally {
    await handle.close();
  }
}

async function detectFileSignature(filePath) {
  const buffer = await readFileHeader(filePath);
  if (bytesMatch(buffer, [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])) {
    return 'png';
  }
  if (bytesMatch(buffer, [0xff, 0xd8, 0xff])) {
    return 'jpeg';
  }
  if (bytesMatch(buffer, [0x47, 0x49, 0x46, 0x38, 0x37, 0x61]) || bytesMatch(buffer, [0x47, 0x49, 0x46, 0x38, 0x39, 0x61])) {
    return 'gif';
  }
  if (bytesMatch(buffer, [0x52, 0x49, 0x46, 0x46]) && bytesMatch(buffer, [0x57, 0x45, 0x42, 0x50], 8)) {
    return 'webp';
  }
  if (bytesMatch(buffer, [0x25, 0x50, 0x44, 0x46])) {
    return 'pdf';
  }
  if (bytesMatch(buffer, [0x4f, 0x67, 0x67, 0x53])) {
    return 'ogg';
  }
  if (bytesMatch(buffer, [0x52, 0x49, 0x46, 0x46]) && bytesMatch(buffer, [0x57, 0x41, 0x56, 0x45], 8)) {
    return 'wav';
  }
  if (bytesMatch(buffer, [0x49, 0x44, 0x33]) || (buffer[0] === 0xff && (buffer[1] & 0xe0) === 0xe0)) {
    return 'mp3';
  }
  if (bytesMatch(buffer, [0x1a, 0x45, 0xdf, 0xa3])) {
    return 'webm';
  }
  if (bytesMatch(buffer, [0x50, 0x4b, 0x03, 0x04])) {
    return 'zip';
  }
  if (bytesMatch(buffer, [0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1])) {
    return 'doc';
  }
  if (bytesMatch(buffer, [0x66, 0x74, 0x79, 0x70], 4)) {
    return 'mp4';
  }
  return 'unknown';
}

function isMimeTypeConsistent(mimeType, detected) {
  const normalized = String(mimeType || '').toLowerCase();
  const imageTypes = {
    'image/png': ['png'],
    'image/jpeg': ['jpeg'],
    'image/gif': ['gif'],
    'image/webp': ['webp']
  };
  const docTypes = {
    'application/pdf': ['pdf'],
    'application/msword': ['doc'],
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': ['zip', 'docx']
  };
  const audioTypes = {
    'audio/ogg': ['ogg'],
    'audio/wav': ['wav'],
    'audio/mpeg': ['mp3'],
    'audio/mp4': ['mp4'],
    'audio/webm': ['webm']
  };

  if (imageTypes[normalized]) {
    return imageTypes[normalized].includes(detected);
  }
  if (docTypes[normalized]) {
    return docTypes[normalized].includes(detected);
  }
  if (audioTypes[normalized]) {
    return audioTypes[normalized].includes(detected);
  }
  if (normalized.startsWith('audio/')) {
    return ['ogg', 'wav', 'mp3', 'mp4', 'webm'].includes(detected);
  }
  return detected !== 'unknown';
}

async function validateUploadedFile(file, allowedSet, allowAudio = false) {
  if (!file) {
    return false;
  }
  const mimeType = String(file.mimetype || '').toLowerCase();
  const isAllowed = allowedSet.has(mimeType) || (allowAudio && mimeType.startsWith('audio/'));
  if (!isAllowed) {
    return false;
  }
  const detected = await detectFileSignature(file.path);
  return isMimeTypeConsistent(mimeType, detected);
}

function resolveMessageType(rawType, filePath) {
  const requested = String(rawType || '').toLowerCase();

  if (requested === 'audio') {
    return 'audio';
  }

  if (requested === 'image') {
    return 'image';
  }

  if (filePath) {
    return 'file';
  }

  return 'text';
}

function createPasswordHash(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const digest = crypto.scryptSync(password, salt, 64).toString('hex');
  return `${salt}:${digest}`;
}

function verifyPassword(password, storedHash) {
  if (!storedHash || !storedHash.includes(':')) {
    return false;
  }

  const [salt, digestHex] = storedHash.split(':');

  if (!salt || !digestHex) {
    return false;
  }

  const digest = Buffer.from(digestHex, 'hex');
  const candidate = crypto.scryptSync(password, salt, 64);

  if (digest.length !== candidate.length) {
    return false;
  }

  return crypto.timingSafeEqual(digest, candidate);
}

function createSessionToken() {
  return crypto.randomBytes(32).toString('hex');
}

function createInviteCode() {
  return crypto.randomBytes(4).toString('hex');
}

function extractBearerToken(authHeader) {
  if (!authHeader) {
    return '';
  }

  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : '';
}

async function cleanupExpiredSessions() {
  await dbRun(`DELETE FROM Sessions WHERE expires_at <= datetime('now')`);
}

async function createSession(username) {
  const token = createSessionToken();
  await dbRun(
    `INSERT INTO Sessions(token, username, created_at, expires_at)
     VALUES(?, ?, datetime('now'), datetime('now', ?))`,
    [token, username, `+${SESSION_TTL_DAYS} days`]
  );
  return token;
}

async function deleteSession(token) {
  if (!token) {
    return;
  }
  await dbRun('DELETE FROM Sessions WHERE token = ?', [token]);
}

async function getSessionUsername(token) {
  if (!token) {
    return '';
  }

  await cleanupExpiredSessions();
  const row = await dbGet(
    `SELECT username
     FROM Sessions
     WHERE token = ? AND expires_at > datetime('now')`,
    [token]
  );
  return row ? row.username : '';
}

async function requireAuth(req, res, next) {
  try {
    const token = extractBearerToken(req.get('authorization'));
    const username = await getSessionUsername(token);

    if (!username) {
      res.status(401).json({ error: 'Unauthorized' });
      return;
    }

    req.token = token;
    req.username = username;
    next();
  } catch (err) {
    console.error('Auth check error:', err);
    res.status(500).json({ error: 'Authentication failed' });
  }
}

function addSocket(username, socketId) {
  if (!socketsByUser.has(username)) {
    socketsByUser.set(username, new Set());
  }

  socketsByUser.get(username).add(socketId);
}

function removeSocket(username, socketId) {
  const socketIds = socketsByUser.get(username);

  if (!socketIds) {
    return;
  }

  socketIds.delete(socketId);

  if (socketIds.size === 0) {
    socketsByUser.delete(username);
  }
}

function emitPresence() {
  io.emit('presence:update', Array.from(socketsByUser.keys()));
}

function emitToUser(username, eventName, payload) {
  const socketIds = socketsByUser.get(username);

  if (!socketIds) {
    return;
  }

  socketIds.forEach((socketId) => {
    io.to(socketId).emit(eventName, payload);
  });
}

async function groupMembers(groupId) {
  const rows = await dbAll(
    'SELECT username FROM GroupMembers WHERE group_id = ?',
    [groupId]
  );

  return rows.map((row) => row.username);
}

async function isGroupMember(groupId, username) {
  const row = await dbGet(
    'SELECT 1 AS ok FROM GroupMembers WHERE group_id = ? AND username = ?',
    [groupId, username]
  );

  return Boolean(row);
}

async function getGroupMemberRole(groupId, username) {
  const row = await dbGet(
    'SELECT role FROM GroupMembers WHERE group_id = ? AND username = ?',
    [groupId, username]
  );

  return row && row.role ? row.role : '';
}

function isModeratorRole(role) {
  return role === 'moderator';
}

async function getGroup(groupId) {
  return dbGet(
    `SELECT id, name, creator, COALESCE(is_public, 1) AS is_public, invite_code
     FROM Groups
     WHERE id = ?`,
    [groupId]
  );
}

async function getGroupByInviteCode(inviteCode) {
  return dbGet(
    `SELECT id, name, creator, COALESCE(is_public, 1) AS is_public, invite_code
     FROM Groups
     WHERE invite_code = ?`,
    [inviteCode]
  );
}

async function emitToGroup(groupId, eventName, payload, excludedUsername = '') {
  const members = await groupMembers(groupId);

  members.forEach((member) => {
    if (excludedUsername && member === excludedUsername) {
      return;
    }

    emitToUser(member, eventName, payload);
  });
}

async function getMessageById(id) {
  return dbGet(
    `SELECT id, sender, receiver, group_id, message, file_path, file_name,
            message_type, timestamp, seen, edited_at, deleted_at, deleted_by, original_message
     FROM Messages
     WHERE id = ?`,
    [id]
  );
}

app.post('/auth/register', async (req, res) => {
  try {
    const rate = await checkRateLimitDb(`register:${req.ip}`, 5, 60 * 1000);
    if (!rate.allowed) {
      const retryAfter = Math.max(1, Math.ceil(rate.retryAfterMs / 1000));
      res
        .status(429)
        .json({ error: `Too many registration attempts. Try again in ${retryAfter}s.` });
      return;
    }

    const username = normalizeUsername(req.body.username);
    const password = String(req.body.password || '');

    if (!isValidUsername(username)) {
      res.status(400).json({ error: 'User ID must be 3-20 letters, numbers, or _' });
      return;
    }

    if (!isValidPassword(password)) {
      res.status(400).json({ error: 'Password must be 6-72 characters' });
      return;
    }

    const existingUser = await dbGet(
      `SELECT username, COALESCE(password_hash, '') AS password_hash
       FROM Users
       WHERE username = ?`,
      [username]
    );

    const passwordHash = createPasswordHash(password);

    if (existingUser && existingUser.password_hash) {
      res.status(409).json({ error: 'User ID already exists' });
      return;
    }

    if (existingUser) {
      await dbRun(
        `UPDATE Users
         SET password_hash = ?, last_seen = datetime('now')
         WHERE username = ?`,
        [passwordHash, username]
      );
    } else {
      await dbRun(
        `INSERT INTO Users(username, password_hash, last_seen, display_name)
         VALUES(?, ?, datetime('now'), ?)`,
        [username, passwordHash, username]
      );
    }

    res.status(201).json({ ok: true });
  } catch (err) {
    console.error('Register error:', err);
    res.status(500).json({ error: 'Registration failed' });
  }
});

app.post('/auth/login', async (req, res) => {
  try {
    const rate = await checkRateLimitDb(`login:${req.ip}`, 5, 60 * 1000);
    if (!rate.allowed) {
      const retryAfter = Math.max(1, Math.ceil(rate.retryAfterMs / 1000));
      res
        .status(429)
        .json({ error: `Too many login attempts. Try again in ${retryAfter}s.` });
      return;
    }

    const username = normalizeUsername(req.body.username);
    const password = String(req.body.password || '');

    if (!isValidUsername(username) || !isValidPassword(password)) {
      res.status(400).json({ error: 'Invalid credentials' });
      return;
    }

    const user = await dbGet(
      `SELECT username, COALESCE(password_hash, '') AS password_hash
       FROM Users
       WHERE username = ?`,
      [username]
    );

    if (!user || !verifyPassword(password, user.password_hash)) {
      res.status(401).json({ error: 'Invalid credentials' });
      return;
    }

    const token = await createSession(user.username);

    await dbRun(
      `UPDATE Users
       SET last_seen = datetime('now')
       WHERE username = ?`,
      [user.username]
    );

    res.json({ token, username: user.username });
  } catch (err) {
    console.error('Login error:', err);
    res.status(500).json({ error: 'Login failed' });
  }
});

app.post('/auth/logout', requireAuth, async (req, res) => {
  try {
    await deleteSession(req.token);
    res.json({ ok: true });
  } catch (err) {
    console.error('Logout error:', err);
    res.status(500).json({ error: 'Logout failed' });
  }
});

app.get('/profile', requireAuth, async (req, res) => {
  try {
    const user = await dbGet(
      `SELECT username, display_name, status_message, avatar_path
       FROM Users
       WHERE username = ?`,
      [req.username]
    );

    if (!user) {
      res.status(404).json({ error: 'Profile not found' });
      return;
    }

    res.json({
      username: user.username,
      display_name: user.display_name || user.username,
      status_message: user.status_message || '',
      avatar_path: user.avatar_path || ''
    });
  } catch (err) {
    console.error('Profile fetch error:', err);
    res.status(500).json({ error: 'Failed to load profile' });
  }
});

app.put('/profile', requireAuth, async (req, res) => {
  try {
    const displayName = sanitizeDisplayName(req.body.displayName) || req.username;
    const statusMessage = sanitizeStatusMessage(req.body.statusMessage);

    await dbRun(
      `UPDATE Users
       SET display_name = ?, status_message = ?
       WHERE username = ?`,
      [displayName, statusMessage, req.username]
    );

    res.json({
      ok: true,
      display_name: displayName,
      status_message: statusMessage
    });
  } catch (err) {
    console.error('Profile update error:', err);
    res.status(500).json({ error: 'Failed to update profile' });
  }
});

app.post('/profile/avatar', requireAuth, avatarUpload.single('avatar'), async (req, res) => {
  try {
    if (!req.file) {
      res.status(400).json({ error: 'No avatar uploaded' });
      return;
    }

    const validAvatar = await validateUploadedFile(req.file, avatarMimeTypes, false);
    if (!validAvatar) {
      await fs.promises.unlink(req.file.path).catch(() => {});
      res.status(400).json({ error: 'Unsupported avatar type' });
      return;
    }

    // Delete old avatar file before saving new one
    const existingUser = await dbGet('SELECT avatar_path FROM Users WHERE username = ?', [req.username]);
    if (existingUser && existingUser.avatar_path) {
      const oldPath = path.join(uploadsDir, existingUser.avatar_path);
      await fs.promises.unlink(oldPath).catch(() => {});
    }

    await dbRun(
      `UPDATE Users
       SET avatar_path = ?
       WHERE username = ?`,
      [req.file.filename, req.username]
    );

    res.json({
      ok: true,
      avatar_path: req.file.filename
    });
  } catch (err) {
    console.error('Avatar upload error:', err);
    res.status(500).json({ error: 'Failed to update avatar' });
  }
});

app.get('/users', requireAuth, async (req, res) => {
  try {
    const q = String(req.query.q || '').trim();
    const params = [];
    let whereClause = '';
    if (q) {
      whereClause = `WHERE username LIKE ? ESCAPE '\\' OR display_name LIKE ? ESCAPE '\\'`;
      const pattern = `%${q.replace(/[%_]/g, '\\$&')}%`;
      params.push(pattern, pattern);
    }
    const users = await dbAll(
      `SELECT username, last_seen, display_name, status_message, avatar_path
       FROM Users
       ${whereClause}
       ORDER BY username COLLATE NOCASE
       LIMIT 100`,
      params
    );

    res.json(
      users.map((user) => ({
        username: user.username,
        last_seen: user.last_seen,
        display_name: user.display_name || user.username,
        status_message: user.status_message || '',
        avatar_path: user.avatar_path || '',
        online: socketsByUser.has(user.username)
      }))
    );
  } catch (err) {
    console.error('Users fetch error:', err);
    res.status(500).json({ error: 'Failed to load users' });
  }
});

app.get('/groups', requireAuth, async (req, res) => {
  try {
    const rows = await dbAll(
      `SELECT g.id,
              g.name,
              g.creator,
              COALESCE(g.is_public, 1) AS is_public,
              gm.role AS member_role,
              CASE WHEN gm.username IS NULL THEN 0 ELSE 1 END AS is_member,
              (SELECT COUNT(*) FROM GroupMembers WHERE group_id = g.id) AS member_count
       FROM Groups g
       LEFT JOIN GroupMembers gm
              ON gm.group_id = g.id
             AND gm.username = ?
       WHERE COALESCE(g.is_public, 1) = 1
          OR g.creator = ?
          OR gm.username IS NOT NULL
       ORDER BY g.name COLLATE NOCASE`,
      [req.username, req.username]
    );

    res.json(
      rows.map((row) => ({
        id: row.id,
        name: row.name,
        creator: row.creator,
        is_public: Boolean(row.is_public),
        is_member: Boolean(row.is_member),
        is_creator: row.creator === req.username,
        is_moderator: row.member_role === 'moderator',
        member_count: row.member_count || 0
      }))
    );
  } catch (err) {
    console.error('Groups fetch error:', err);
    res.status(500).json({ error: 'Failed to load groups' });
  }
});

app.post('/groups', requireAuth, async (req, res) => {
  try {
    const name = normalizeGroupName(req.body.name);
    const isPublic =
      typeof req.body.isPublic === 'boolean' ? req.body.isPublic : true;
    const inviteCode = createInviteCode();

    if (name.length < 2) {
      res.status(400).json({ error: 'Group name must be at least 2 characters' });
      return;
    }

    const insertResult = await dbRun(
      'INSERT INTO Groups(name, creator, is_public, invite_code) VALUES(?, ?, ?, ?)',
      [name, req.username, isPublic ? 1 : 0, inviteCode]
    );

    await dbRun(
      `INSERT OR IGNORE INTO GroupMembers(group_id, username, role)
       VALUES(?, ?, 'creator')`,
      [insertResult.lastID, req.username]
    );

    res.status(201).json({
      id: insertResult.lastID,
      name,
      creator: req.username,
      is_public: Boolean(isPublic),
      invite_code: inviteCode,
      is_member: true,
      is_creator: true,
      is_moderator: false
    });
  } catch (err) {
    console.error('Group create error:', err);
    res.status(500).json({ error: 'Failed to create group' });
  }
});

app.post('/groups/:groupId/join', requireAuth, async (req, res) => {
  try {
    const groupId = Number.parseInt(req.params.groupId, 10);

    if (!Number.isInteger(groupId) || groupId <= 0) {
      res.status(400).json({ error: 'Invalid group ID' });
      return;
    }

    const group = await getGroup(groupId);

    if (!group) {
      res.status(404).json({ error: 'Group not found' });
      return;
    }

    if (!group.is_public) {
      res.status(403).json({ error: 'This group is private' });
      return;
    }

    const insertResult = await dbRun(
      `INSERT OR IGNORE INTO GroupMembers(group_id, username, role)
       VALUES(?, ?, 'member')`,
      [groupId, req.username]
    );

    if (insertResult.changes > 0) {
      await emitToGroup(groupId, 'group:members-updated', { groupId });
      emitToUser(req.username, 'group:membership-changed', {
        groupId,
        groupName: group.name,
        action: 'added'
      });
    }

    res.json({
      id: group.id,
      name: group.name,
      creator: group.creator,
      is_public: Boolean(group.is_public),
      is_member: true,
      is_creator: group.creator === req.username,
      is_moderator: false
    });
  } catch (err) {
    console.error('Group join error:', err);
    res.status(500).json({ error: 'Failed to join group' });
  }
});

app.post('/groups/join-by-code', requireAuth, async (req, res) => {
  try {
    const inviteCode = String(req.body.inviteCode || '').trim();

    if (inviteCode.length < 4) {
      res.status(400).json({ error: 'Invalid invite code' });
      return;
    }

    const group = await getGroupByInviteCode(inviteCode);
    if (!group) {
      res.status(404).json({ error: 'Invite code not found' });
      return;
    }

    const insertResult = await dbRun(
      `INSERT OR IGNORE INTO GroupMembers(group_id, username, role)
       VALUES(?, ?, 'member')`,
      [group.id, req.username]
    );

    if (insertResult.changes > 0) {
      await emitToGroup(group.id, 'group:members-updated', { groupId: group.id });
      emitToUser(req.username, 'group:membership-changed', {
        groupId: group.id,
        groupName: group.name,
        action: 'added'
      });
    }

    res.json({
      id: group.id,
      name: group.name,
      creator: group.creator,
      is_public: Boolean(group.is_public),
      is_member: true,
      is_creator: group.creator === req.username,
      is_moderator: false
    });
  } catch (err) {
    console.error('Join by code error:', err);
    res.status(500).json({ error: 'Failed to join group' });
  }
});

app.patch('/groups/:groupId', requireAuth, async (req, res) => {
  try {
    const groupId = Number.parseInt(req.params.groupId, 10);
    const name = normalizeGroupName(req.body.name);

    if (!Number.isInteger(groupId) || groupId <= 0) {
      res.status(400).json({ error: 'Invalid group ID' });
      return;
    }

    if (name.length < 2) {
      res.status(400).json({ error: 'Group name must be at least 2 characters' });
      return;
    }

    const group = await getGroup(groupId);
    if (!group) {
      res.status(404).json({ error: 'Group not found' });
      return;
    }

    if (group.creator !== req.username) {
      res.status(403).json({ error: 'Only the creator can rename this group' });
      return;
    }

    await dbRun('UPDATE Groups SET name = ? WHERE id = ?', [name, groupId]);
    await emitToGroup(groupId, 'group:updated', { groupId, name });
    io.emit('group:updated', { groupId, name });

    res.json({ ok: true, groupId, name });
  } catch (err) {
    console.error('Group rename error:', err);
    res.status(500).json({ error: 'Failed to rename group' });
  }
});

app.delete('/groups/:groupId', requireAuth, async (req, res) => {
  try {
    const groupId = Number.parseInt(req.params.groupId, 10);

    if (!Number.isInteger(groupId) || groupId <= 0) {
      res.status(400).json({ error: 'Invalid group ID' });
      return;
    }

    const group = await getGroup(groupId);
    if (!group) {
      res.status(404).json({ error: 'Group not found' });
      return;
    }

    if (group.creator !== req.username) {
      res.status(403).json({ error: 'Only the creator can delete this group' });
      return;
    }

    // Notify all members before deleting
    await emitToGroup(groupId, 'group:deleted', { groupId, groupName: group.name });

    await dbRun('DELETE FROM GroupMembers WHERE group_id = ?', [groupId]);
    await dbRun('DELETE FROM Groups WHERE id = ?', [groupId]);

    io.emit('group:updated', { groupId, deleted: true });
    res.json({ ok: true, groupId });
  } catch (err) {
    console.error('Group delete error:', err);
    res.status(500).json({ error: 'Failed to delete group' });
  }
});

app.post('/groups/:groupId/transfer', requireAuth, async (req, res) => {
  try {
    const groupId = Number.parseInt(req.params.groupId, 10);
    const targetUsername = normalizeUsername(req.body.username);

    if (!Number.isInteger(groupId) || groupId <= 0) {
      res.status(400).json({ error: 'Invalid group ID' });
      return;
    }

    if (!isValidUsername(targetUsername)) {
      res.status(400).json({ error: 'Invalid member username' });
      return;
    }

    const group = await getGroup(groupId);
    if (!group) {
      res.status(404).json({ error: 'Group not found' });
      return;
    }

    if (group.creator !== req.username) {
      res.status(403).json({ error: 'Only the creator can transfer ownership' });
      return;
    }

    if (targetUsername === group.creator) {
      res.status(400).json({ error: 'User is already the creator' });
      return;
    }

    const targetIsMember = await isGroupMember(groupId, targetUsername);
    if (!targetIsMember) {
      res.status(404).json({ error: 'User is not a member of this group' });
      return;
    }

    await dbRun('UPDATE Groups SET creator = ? WHERE id = ?', [targetUsername, groupId]);
    await dbRun(
      `UPDATE GroupMembers
       SET role = 'moderator'
       WHERE group_id = ? AND username = ?`,
      [groupId, req.username]
    );
    await dbRun(
      `UPDATE GroupMembers
       SET role = 'creator'
       WHERE group_id = ? AND username = ?`,
      [groupId, targetUsername]
    );

    await emitToGroup(groupId, 'group:members-updated', { groupId });
    await emitToGroup(groupId, 'group:updated', {
      groupId,
      name: group.name,
      creator: targetUsername
    });
    io.emit('group:updated', { groupId, name: group.name, creator: targetUsername });

    res.json({ ok: true, groupId, creator: targetUsername });
  } catch (err) {
    console.error('Group transfer error:', err);
    res.status(500).json({ error: 'Failed to transfer ownership' });
  }
});

app.get('/groups/:groupId/invite', requireAuth, async (req, res) => {
  try {
    const groupId = Number.parseInt(req.params.groupId, 10);

    if (!Number.isInteger(groupId) || groupId <= 0) {
      res.status(400).json({ error: 'Invalid group ID' });
      return;
    }

    const group = await getGroup(groupId);
    if (!group) {
      res.status(404).json({ error: 'Group not found' });
      return;
    }

    const requesterRole = await getGroupMemberRole(groupId, req.username);
    const requesterIsCreator = group.creator === req.username;
    const requesterIsModerator = isModeratorRole(requesterRole);

    if (!requesterIsCreator && !requesterIsModerator) {
      res.status(403).json({ error: 'Only creator or moderator can view invite code' });
      return;
    }

    res.json({ groupId: group.id, invite_code: group.invite_code || '' });
  } catch (err) {
    console.error('Invite fetch error:', err);
    res.status(500).json({ error: 'Failed to fetch invite code' });
  }
});

app.post('/groups/:groupId/invite/rotate', requireAuth, async (req, res) => {
  try {
    const groupId = Number.parseInt(req.params.groupId, 10);

    if (!Number.isInteger(groupId) || groupId <= 0) {
      res.status(400).json({ error: 'Invalid group ID' });
      return;
    }

    const group = await getGroup(groupId);
    if (!group) {
      res.status(404).json({ error: 'Group not found' });
      return;
    }

    if (group.creator !== req.username) {
      res.status(403).json({ error: 'Only the creator can rotate invite code' });
      return;
    }

    const inviteCode = createInviteCode();
    await dbRun('UPDATE Groups SET invite_code = ? WHERE id = ?', [
      inviteCode,
      groupId
    ]);

    res.json({ groupId, invite_code: inviteCode });
  } catch (err) {
    console.error('Invite rotate error:', err);
    res.status(500).json({ error: 'Failed to rotate invite code' });
  }
});

app.get('/groups/:groupId/members', requireAuth, async (req, res) => {
  try {
    const groupId = Number.parseInt(req.params.groupId, 10);

    if (!Number.isInteger(groupId) || groupId <= 0) {
      res.status(400).json({ error: 'Invalid group ID' });
      return;
    }

    const group = await getGroup(groupId);
    if (!group) {
      res.status(404).json({ error: 'Group not found' });
      return;
    }

    const requesterIsMember = await isGroupMember(groupId, req.username);
    if (!requesterIsMember) {
      res.status(403).json({ error: 'Join this group first' });
      return;
    }

    const rows = await dbAll(
      `SELECT u.username,
              u.last_seen,
              gm.role,
              CASE WHEN u.username = ? THEN 1 ELSE 0 END AS is_creator
       FROM GroupMembers gm
       JOIN Users u ON u.username = gm.username
       WHERE gm.group_id = ?
       ORDER BY u.username COLLATE NOCASE`,
      [group.creator, groupId]
    );

    res.json({
      groupId: group.id,
      groupName: group.name,
      creator: group.creator,
      is_public: Boolean(group.is_public),
      members: rows.map((row) => ({
        username: row.username,
        last_seen: row.last_seen,
        online: socketsByUser.has(row.username),
        role: row.role || 'member',
        is_creator: Boolean(row.is_creator),
        is_moderator: row.role === 'moderator'
      }))
    });
  } catch (err) {
    console.error('Group members fetch error:', err);
    res.status(500).json({ error: 'Failed to load group members' });
  }
});

app.post('/groups/:groupId/members', requireAuth, async (req, res) => {
  try {
    const groupId = Number.parseInt(req.params.groupId, 10);
    const targetUsername = normalizeUsername(req.body.username);

    if (!Number.isInteger(groupId) || groupId <= 0) {
      res.status(400).json({ error: 'Invalid group ID' });
      return;
    }

    if (!isValidUsername(targetUsername)) {
      res.status(400).json({ error: 'Invalid member username' });
      return;
    }

    const group = await getGroup(groupId);
    if (!group) {
      res.status(404).json({ error: 'Group not found' });
      return;
    }

    const requesterRole = await getGroupMemberRole(groupId, req.username);
    const requesterIsCreator = group.creator === req.username;
    const requesterIsModerator = isModeratorRole(requesterRole);

    if (!requesterIsCreator && !requesterIsModerator) {
      res
        .status(403)
        .json({ error: 'Only the creator or a moderator can add members' });
      return;
    }

    const targetUser = await dbGet(
      'SELECT username FROM Users WHERE username = ?',
      [targetUsername]
    );

    if (!targetUser) {
      res.status(404).json({ error: 'User not found' });
      return;
    }

    const insertResult = await dbRun(
      `INSERT OR IGNORE INTO GroupMembers(group_id, username, role)
       VALUES(?, ?, 'member')`,
      [groupId, targetUsername]
    );

    if (insertResult.changes > 0) {
      await emitToGroup(groupId, 'group:members-updated', { groupId });
      emitToUser(targetUsername, 'group:membership-changed', {
        groupId,
        groupName: group.name,
        action: 'added'
      });
    }

    res.status(insertResult.changes > 0 ? 201 : 200).json({
      ok: true,
      groupId,
      username: targetUsername,
      added: insertResult.changes > 0
    });
  } catch (err) {
    console.error('Group member add error:', err);
    res.status(500).json({ error: 'Failed to add member' });
  }
});

app.post('/groups/:groupId/moderators', requireAuth, async (req, res) => {
  try {
    const groupId = Number.parseInt(req.params.groupId, 10);
    const targetUsername = normalizeUsername(req.body.username);
    const makeModerator = req.body.makeModerator === true;

    if (!Number.isInteger(groupId) || groupId <= 0) {
      res.status(400).json({ error: 'Invalid group ID' });
      return;
    }

    if (!isValidUsername(targetUsername)) {
      res.status(400).json({ error: 'Invalid member username' });
      return;
    }

    const group = await getGroup(groupId);
    if (!group) {
      res.status(404).json({ error: 'Group not found' });
      return;
    }

    if (group.creator !== req.username) {
      res.status(403).json({ error: 'Only the creator can manage moderators' });
      return;
    }

    if (targetUsername === group.creator) {
      res.status(400).json({ error: 'Creator role cannot be changed' });
      return;
    }

    const targetIsMember = await isGroupMember(groupId, targetUsername);
    if (!targetIsMember) {
      res.status(404).json({ error: 'User is not a member of this group' });
      return;
    }

    const newRole = makeModerator ? 'moderator' : 'member';

    await dbRun(
      'UPDATE GroupMembers SET role = ? WHERE group_id = ? AND username = ?',
      [newRole, groupId, targetUsername]
    );

    await emitToGroup(groupId, 'group:members-updated', { groupId });
    emitToUser(targetUsername, 'group:membership-changed', {
      groupId,
      groupName: group.name,
      action: 'role',
      role: newRole
    });

    res.json({
      ok: true,
      groupId,
      username: targetUsername,
      role: newRole
    });
  } catch (err) {
    console.error('Group moderator update error:', err);
    res.status(500).json({ error: 'Failed to update moderator' });
  }
});

app.delete('/groups/:groupId/members/:memberUsername', requireAuth, async (req, res) => {
  try {
    const groupId = Number.parseInt(req.params.groupId, 10);
    const targetUsername = normalizeUsername(req.params.memberUsername);

    if (!Number.isInteger(groupId) || groupId <= 0) {
      res.status(400).json({ error: 'Invalid group ID' });
      return;
    }

    if (!isValidUsername(targetUsername)) {
      res.status(400).json({ error: 'Invalid member username' });
      return;
    }

    const group = await getGroup(groupId);
    if (!group) {
      res.status(404).json({ error: 'Group not found' });
      return;
    }

    const requesterRole = await getGroupMemberRole(groupId, req.username);
    const requesterIsCreator = group.creator === req.username;
    const requesterIsModerator = isModeratorRole(requesterRole);
    const requesterIsSelfTarget = targetUsername === req.username;

    if (!requesterIsCreator && !requesterIsModerator && !requesterIsSelfTarget) {
      res
        .status(403)
        .json({ error: 'Only creator or moderator can remove other members' });
      return;
    }

    if (targetUsername === group.creator) {
      res
        .status(400)
        .json({ error: 'Group creator cannot be removed from this endpoint' });
      return;
    }

    const targetIsMember = await isGroupMember(groupId, targetUsername);
    if (!targetIsMember) {
      res.status(404).json({ error: 'User is not a member of this group' });
      return;
    }

    await dbRun(
      'DELETE FROM GroupMembers WHERE group_id = ? AND username = ?',
      [groupId, targetUsername]
    );

    await emitToGroup(groupId, 'group:members-updated', { groupId });
    emitToUser(targetUsername, 'group:membership-changed', {
      groupId,
      groupName: group.name,
      action: 'removed'
    });

    res.json({
      ok: true,
      groupId,
      username: targetUsername,
      removed: true
    });
  } catch (err) {
    console.error('Group member remove error:', err);
    res.status(500).json({ error: 'Failed to remove member' });
  }
});

app.get('/search', requireAuth, async (req, res) => {
  try {
    const q = String(req.query.q || '').trim();
    const limitRaw = Number.parseInt(req.query.limit || '50', 10);
    const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(limitRaw, 1), 200) : 50;
    const beforeIdRaw = Number.parseInt(req.query.beforeId || '', 10);
    const beforeId = Number.isInteger(beforeIdRaw) && beforeIdRaw > 0 ? beforeIdRaw : 0;

    if (q.length < 2) {
      res.status(400).json({ error: 'Search query must be at least 2 characters' });
      return;
    }

    const pattern = `%${q.replace(/[%_]/g, '\\$&')}%`;

    const beforeFilter = beforeId ? 'AND m.id < ?' : '';
    const rows = await dbAll(
      `
      SELECT m.id,
             m.sender,
             m.receiver,
             m.group_id,
             m.message,
             m.file_path,
             m.file_name,
             m.message_type,
             m.timestamp,
             m.edited_at,
             m.deleted_at,
             m.deleted_by,
             NULL AS group_name
      FROM Messages m
      WHERE m.group_id IS NULL
        AND (m.sender = ? OR m.receiver = ?)
        AND m.deleted_at IS NULL
        ${beforeFilter}
        AND (
          m.message LIKE ? ESCAPE '\\'
          OR m.sender LIKE ? ESCAPE '\\'
          OR m.file_name LIKE ? ESCAPE '\\'
        )
      UNION ALL
      SELECT m.id,
             m.sender,
             m.receiver,
             m.group_id,
             m.message,
             m.file_path,
             m.file_name,
             m.message_type,
             m.timestamp,
             m.edited_at,
             m.deleted_at,
             m.deleted_by,
             g.name AS group_name
      FROM Messages m
      JOIN GroupMembers gm
        ON gm.group_id = m.group_id
       AND gm.username = ?
      JOIN Groups g
        ON g.id = m.group_id
      WHERE m.group_id IS NOT NULL
        AND m.deleted_at IS NULL
        ${beforeFilter}
        AND (
          m.message LIKE ? ESCAPE '\\'
          OR m.sender LIKE ? ESCAPE '\\'
          OR m.file_name LIKE ? ESCAPE '\\'
        )
      ORDER BY id DESC
      LIMIT ?
      `,
      [
        req.username,
        req.username,
        ...(beforeId ? [beforeId] : []),
        pattern,
        pattern,
        pattern,
        req.username,
        ...(beforeId ? [beforeId] : []),
        pattern,
        pattern,
        pattern,
        limit
      ]
    );

    res.json(rows || []);
  } catch (err) {
    console.error('Search error:', err);
    res.status(500).json({ error: 'Failed to search messages' });
  }
});

app.get('/history/private/:target', requireAuth, async (req, res) => {
  try {
    const target = normalizeUsername(req.params.target);
    const limitRaw = Number.parseInt(req.query.limit || '100', 10);
    const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(limitRaw, 1), 200) : 100;
    const beforeIdRaw = Number.parseInt(req.query.beforeId || '', 10);
    const beforeId = Number.isInteger(beforeIdRaw) && beforeIdRaw > 0 ? beforeIdRaw : 0;

    if (!isValidUsername(target)) {
      res.status(400).json({ error: 'Invalid target user' });
      return;
    }

    const beforeFilter = beforeId ? 'AND id < ?' : '';
    const messages = await dbAll(
      `SELECT id, sender, receiver, group_id, message, file_path, file_name,
              message_type, timestamp, seen, edited_at, deleted_at, deleted_by, original_message
       FROM Messages
       WHERE ((sender = ? AND receiver = ?)
          OR (sender = ? AND receiver = ?))
         ${beforeFilter}
       ORDER BY id DESC
       LIMIT ?`,
      [
        req.username,
        target,
        target,
        req.username,
        ...(beforeId ? [beforeId] : []),
        limit
      ]
    );

    // Mark messages from target as seen and notify sender
    const unseenCount = await dbRun(
      `UPDATE Messages SET seen = 1
       WHERE receiver = ? AND sender = ? AND seen = 0`,
      [req.username, target]
    );
    if (unseenCount.changes > 0) {
      emitToUser(target, 'chat:seen', { from: req.username, to: target, count: unseenCount.changes });
    }

    res.json(messages.reverse());
  } catch (err) {
    console.error('Private history error:', err);
    res.status(500).json({ error: 'Failed to load private history' });
  }
});

app.get('/history/group/:groupId', requireAuth, async (req, res) => {
  try {
    const groupId = Number.parseInt(req.params.groupId, 10);
    const limitRaw = Number.parseInt(req.query.limit || '100', 10);
    const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(limitRaw, 1), 200) : 100;
    const beforeIdRaw = Number.parseInt(req.query.beforeId || '', 10);
    const beforeId = Number.isInteger(beforeIdRaw) && beforeIdRaw > 0 ? beforeIdRaw : 0;

    if (!Number.isInteger(groupId) || groupId <= 0) {
      res.status(400).json({ error: 'Invalid group ID' });
      return;
    }

    const member = await isGroupMember(groupId, req.username);

    if (!member) {
      res.status(403).json({ error: 'Join this group first' });
      return;
    }

    const beforeFilter = beforeId ? 'AND id < ?' : '';
    const messages = await dbAll(
      `SELECT id, sender, receiver, group_id, message, file_path, file_name,
              message_type, timestamp, seen, edited_at, deleted_at, deleted_by, original_message
       FROM Messages
       WHERE group_id = ?
         ${beforeFilter}
       ORDER BY id DESC
       LIMIT ?`,
      [groupId, ...(beforeId ? [beforeId] : []), limit]
    );

    res.json(messages.reverse());
  } catch (err) {
    console.error('Group history error:', err);
    res.status(500).json({ error: 'Failed to load group history' });
  }
});

// Authenticated file serving
app.get('/uploads/:filename', requireAuth, async (req, res) => {
  try {
    const filename = sanitizeFilePath(req.params.filename);
    if (!filename) {
      res.status(400).json({ error: 'Invalid filename' });
      return;
    }
    const filePath = path.join(uploadsDir, filename);
    try {
      await fs.promises.access(filePath);
    } catch (_err) {
      res.status(404).json({ error: 'File not found' });
      return;
    }
    res.sendFile(filePath);
  } catch (err) {
    console.error('File serve error:', err);
    res.status(500).json({ error: 'Failed to serve file' });
  }
});

// Message edit history
app.get('/messages/:id/history', requireAuth, async (req, res) => {
  try {
    const messageId = Number.parseInt(req.params.id, 10);
    if (!Number.isInteger(messageId) || messageId <= 0) {
      res.status(400).json({ error: 'Invalid message ID' });
      return;
    }
    const message = await getMessageById(messageId);
    if (!message) {
      res.status(404).json({ error: 'Message not found' });
      return;
    }
    // Only sender or recipient/group member can view history
    const canView =
      message.sender === req.username ||
      message.receiver === req.username ||
      (message.group_id && (await isGroupMember(message.group_id, req.username)));
    if (!canView) {
      res.status(403).json({ error: 'Not authorized' });
      return;
    }
    res.json({
      id: message.id,
      original_message: message.original_message || null,
      edited_at: message.edited_at || null
    });
  } catch (err) {
    console.error('Message history error:', err);
    res.status(500).json({ error: 'Failed to load message history' });
  }
});

// Message reactions
app.get('/messages/:id/reactions', requireAuth, async (req, res) => {
  try {
    const messageId = Number.parseInt(req.params.id, 10);
    if (!Number.isInteger(messageId) || messageId <= 0) {
      res.status(400).json({ error: 'Invalid message ID' });
      return;
    }
    const rows = await dbAll(
      `SELECT emoji, COUNT(*) AS count,
              MAX(CASE WHEN username = ? THEN 1 ELSE 0 END) AS reacted_by_me
       FROM Reactions WHERE message_id = ? GROUP BY emoji`,
      [req.username, messageId]
    );
    res.json(rows.map((r) => ({ emoji: r.emoji, count: r.count, reacted_by_me: Boolean(r.reacted_by_me) })));
  } catch (err) {
    console.error('Reactions fetch error:', err);
    res.status(500).json({ error: 'Failed to load reactions' });
  }
});

app.post('/messages/:id/react', requireAuth, async (req, res) => {
  try {
    const messageId = Number.parseInt(req.params.id, 10);
    const emoji = String(req.body.emoji || '').trim().slice(0, 8);
    if (!Number.isInteger(messageId) || messageId <= 0 || !emoji) {
      res.status(400).json({ error: 'Invalid message ID or emoji' });
      return;
    }
    const message = await getMessageById(messageId);
    if (!message) {
      res.status(404).json({ error: 'Message not found' });
      return;
    }
    // Check access
    const canReact =
      message.sender === req.username ||
      message.receiver === req.username ||
      (message.group_id && (await isGroupMember(message.group_id, req.username)));
    if (!canReact) {
      res.status(403).json({ error: 'Not authorized' });
      return;
    }
    // Toggle: try insert, if exists delete
    const existing = await dbGet(
      'SELECT id FROM Reactions WHERE message_id = ? AND username = ? AND emoji = ?',
      [messageId, req.username, emoji]
    );
    if (existing) {
      await dbRun('DELETE FROM Reactions WHERE id = ?', [existing.id]);
    } else {
      await dbRun(
        'INSERT OR IGNORE INTO Reactions(message_id, username, emoji) VALUES(?, ?, ?)',
        [messageId, req.username, emoji]
      );
    }
    // Emit to relevant parties
    const reactionPayload = { messageId, emoji, username: req.username, removed: Boolean(existing) };
    if (message.group_id) {
      await emitToGroup(message.group_id, 'chat:reaction', reactionPayload);
    } else {
      emitToUser(message.sender, 'chat:reaction', reactionPayload);
      if (message.receiver && message.receiver !== message.sender) {
        emitToUser(message.receiver, 'chat:reaction', reactionPayload);
      }
    }
    res.json({ ok: true, removed: Boolean(existing) });
  } catch (err) {
    console.error('Reaction error:', err);
    res.status(500).json({ error: 'Failed to toggle reaction' });
  }
});

// Account deletion
app.delete('/auth/account', requireAuth, async (req, res) => {
  try {
    // Soft-delete: anonymize user, remove sessions and group memberships
    await dbRun('DELETE FROM Sessions WHERE username = ?', [req.username]);
    await dbRun('DELETE FROM GroupMembers WHERE username = ?', [req.username]);
    await dbRun(
      `UPDATE Users SET display_name = '[deleted]', password_hash = '', avatar_path = '', status_message = ''
       WHERE username = ?`,
      [req.username]
    );
    // Disconnect active sockets
    const socketIds = socketsByUser.get(req.username);
    if (socketIds) {
      socketIds.forEach((sid) => {
        const s = io.sockets.sockets.get(sid);
        if (s) s.disconnect(true);
      });
    }
    res.json({ ok: true });
  } catch (err) {
    console.error('Account delete error:', err);
    res.status(500).json({ error: 'Failed to delete account' });
  }
});

app.post('/upload', requireAuth, upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      res.status(400).json({ error: 'No file uploaded' });
      return;
    }

    const validFile = await validateUploadedFile(req.file, allowedMimeTypes, true);
    if (!validFile) {
      await fs.promises.unlink(req.file.path).catch(() => {});
      res.status(400).json({ error: 'Unsupported file type' });
      return;
    }

    res.json({
      filePath: req.file.filename,
      fileName: req.file.originalname,
      mimeType: req.file.mimetype
    });
  } catch (err) {
    console.error('Upload error:', err);
    res.status(500).json({ error: 'Failed to upload file' });
  }
});

io.use(async (socket, next) => {
  try {
    const token = socket.handshake.auth && socket.handshake.auth.token;
    const username = await getSessionUsername(token);

    if (!username) {
      next(new Error('Unauthorized'));
      return;
    }

    socket.username = username;
    next();
  } catch (err) {
    console.error('Socket auth error:', err);
    next(new Error('Unauthorized'));
  }
});

io.on('connection', (socket) => {
  addSocket(socket.username, socket.id);
  emitPresence();

  socket.on('chat:private', async (payload = {}, ack) => {
    const respond = typeof ack === 'function' ? ack : () => {};

    try {
      const rate = await checkRateLimitDb(`msg:${socket.username}`, 20, 10 * 1000);
      if (!rate.allowed) {
        const retryAfter = Math.max(1, Math.ceil(rate.retryAfterMs / 1000));
        respond({ ok: false, error: `Rate limit exceeded. Try again in ${retryAfter}s.` });
        return;
      }

      const to = normalizeUsername(payload.to);
      const message = sanitizeMessage(payload.message);
      const filePath = sanitizeFilePath(payload.filePath);
      const fileName = sanitizeFileName(payload.fileName) || filePath;
      const messageType = resolveMessageType(payload.messageType, filePath);

      if (!isValidUsername(to)) {
        respond({ ok: false, error: 'Invalid recipient' });
        return;
      }

      if (!message && !filePath) {
        respond({ ok: false, error: 'Message is empty' });
        return;
      }

      const recipient = await dbGet(
        'SELECT username FROM Users WHERE username = ?',
        [to]
      );

      if (!recipient) {
        respond({ ok: false, error: 'Recipient does not exist' });
        return;
      }

      const insertResult = await dbRun(
        `INSERT INTO Messages(sender, receiver, message, file_path, file_name, message_type, timestamp)
         VALUES(?, ?, ?, ?, ?, ?, datetime('now'))`,
        [
          socket.username,
          to,
          message || null,
          filePath || null,
          fileName || null,
          messageType
        ]
      );

      const savedMessage = await getMessageById(insertResult.lastID);

      if (savedMessage) {
        emitToUser(socket.username, 'chat:message', savedMessage);
        if (to !== socket.username) {
          emitToUser(to, 'chat:message', savedMessage);
        }
      }

      respond({ ok: true });
    } catch (err) {
      console.error('Private message error:', err);
      respond({ ok: false, error: 'Failed to send message' });
    }
  });

  socket.on('chat:group', async (payload = {}, ack) => {
    const respond = typeof ack === 'function' ? ack : () => {};

    try {
      const rate = await checkRateLimitDb(`msg:${socket.username}`, 20, 10 * 1000);
      if (!rate.allowed) {
        const retryAfter = Math.max(1, Math.ceil(rate.retryAfterMs / 1000));
        respond({ ok: false, error: `Rate limit exceeded. Try again in ${retryAfter}s.` });
        return;
      }

      const groupId = Number.parseInt(payload.groupId, 10);
      const message = sanitizeMessage(payload.message);
      const filePath = sanitizeFilePath(payload.filePath);
      const fileName = sanitizeFileName(payload.fileName) || filePath;
      const messageType = resolveMessageType(payload.messageType, filePath);

      if (!Number.isInteger(groupId) || groupId <= 0) {
        respond({ ok: false, error: 'Invalid group ID' });
        return;
      }

      if (!message && !filePath) {
        respond({ ok: false, error: 'Message is empty' });
        return;
      }

      const member = await isGroupMember(groupId, socket.username);

      if (!member) {
        respond({ ok: false, error: 'Join this group before sending' });
        return;
      }

      const insertResult = await dbRun(
        `INSERT INTO Messages(sender, group_id, message, file_path, file_name, message_type, timestamp)
         VALUES(?, ?, ?, ?, ?, ?, datetime('now'))`,
        [
          socket.username,
          groupId,
          message || null,
          filePath || null,
          fileName || null,
          messageType
        ]
      );

      const savedMessage = await getMessageById(insertResult.lastID);

      if (savedMessage) {
        await emitToGroup(groupId, 'chat:message', savedMessage);
      }

      respond({ ok: true });
    } catch (err) {
      console.error('Group message error:', err);
      respond({ ok: false, error: 'Failed to send message' });
    }
  });

  socket.on('chat:edit', async (payload = {}, ack) => {
    const respond = typeof ack === 'function' ? ack : () => {};

    try {
      const rate = await checkRateLimitDb(`edit:${socket.username}`, 20, 10 * 1000);
      if (!rate.allowed) {
        const retryAfter = Math.max(1, Math.ceil(rate.retryAfterMs / 1000));
        respond({ ok: false, error: `Rate limit exceeded. Try again in ${retryAfter}s.` });
        return;
      }
      const messageId = Number.parseInt(payload.id, 10);
      const messageText = sanitizeMessage(payload.message);

      if (!Number.isInteger(messageId) || messageId <= 0) {
        respond({ ok: false, error: 'Invalid message ID' });
        return;
      }

      if (!messageText) {
        respond({ ok: false, error: 'Edited message is empty' });
        return;
      }

      const existing = await getMessageById(messageId);
      if (!existing) {
        respond({ ok: false, error: 'Message not found' });
        return;
      }

      if (existing.deleted_at) {
        respond({ ok: false, error: 'Cannot edit a deleted message' });
        return;
      }

      if (existing.sender !== socket.username) {
        respond({ ok: false, error: 'You can only edit your own messages' });
        return;
      }

      if (existing.file_path || existing.message_type !== 'text') {
        respond({ ok: false, error: 'Only text messages can be edited' });
        return;
      }

      await dbRun(
        `UPDATE Messages
         SET original_message = COALESCE(original_message, message),
             message = ?,
             edited_at = datetime('now')
         WHERE id = ?`,
        [messageText, messageId]
      );

      const updatedMessage = await getMessageById(messageId);
      if (updatedMessage) {
        if (updatedMessage.group_id) {
          await emitToGroup(updatedMessage.group_id, 'chat:message-updated', updatedMessage);
        } else {
          emitToUser(updatedMessage.sender, 'chat:message-updated', updatedMessage);
          if (updatedMessage.receiver && updatedMessage.receiver !== updatedMessage.sender) {
            emitToUser(updatedMessage.receiver, 'chat:message-updated', updatedMessage);
          }
        }
      }

      respond({ ok: true });
    } catch (err) {
      console.error('Edit message error:', err);
      respond({ ok: false, error: 'Failed to edit message' });
    }
  });

  socket.on('chat:delete', async (payload = {}, ack) => {
    const respond = typeof ack === 'function' ? ack : () => {};

    try {
      const rate = await checkRateLimitDb(`del:${socket.username}`, 20, 10 * 1000);
      if (!rate.allowed) {
        const retryAfter = Math.max(1, Math.ceil(rate.retryAfterMs / 1000));
        respond({ ok: false, error: `Rate limit exceeded. Try again in ${retryAfter}s.` });
        return;
      }
      const messageId = Number.parseInt(payload.id, 10);

      if (!Number.isInteger(messageId) || messageId <= 0) {
        respond({ ok: false, error: 'Invalid message ID' });
        return;
      }

      const existing = await getMessageById(messageId);
      if (!existing) {
        respond({ ok: false, error: 'Message not found' });
        return;
      }

      if (existing.deleted_at) {
        respond({ ok: true });
        return;
      }

      let canDelete = existing.sender === socket.username;

      if (!canDelete && existing.group_id) {
        const group = await getGroup(existing.group_id);
        if (group) {
          const requesterRole = await getGroupMemberRole(existing.group_id, socket.username);
          const requesterIsCreator = group.creator === socket.username;
          const requesterIsModerator = isModeratorRole(requesterRole);
          canDelete = requesterIsCreator || requesterIsModerator;
        }
      }

      if (!canDelete) {
        respond({ ok: false, error: 'Not allowed to delete this message' });
        return;
      }

      await dbRun(
        `UPDATE Messages
         SET deleted_at = datetime('now'),
             deleted_by = ?
         WHERE id = ?`,
        [socket.username, messageId]
      );

      const updatedMessage = await getMessageById(messageId);
      if (updatedMessage) {
        if (updatedMessage.group_id) {
          await emitToGroup(updatedMessage.group_id, 'chat:message-updated', updatedMessage);
        } else {
          emitToUser(updatedMessage.sender, 'chat:message-updated', updatedMessage);
          if (updatedMessage.receiver && updatedMessage.receiver !== updatedMessage.sender) {
            emitToUser(updatedMessage.receiver, 'chat:message-updated', updatedMessage);
          }
        }
      }

      respond({ ok: true });
    } catch (err) {
      console.error('Delete message error:', err);
      respond({ ok: false, error: 'Failed to delete message' });
    }
  });

  socket.on('chat:typing', async (payload = {}) => {
    const isTyping = Boolean(payload.isTyping);

    if (payload.to) {
      const to = normalizeUsername(payload.to);

      if (!isValidUsername(to) || to === socket.username) {
        return;
      }

      emitToUser(to, 'chat:typing', {
        from: socket.username,
        to,
        isTyping
      });
      return;
    }

    if (payload.groupId) {
      const groupId = Number.parseInt(payload.groupId, 10);

      if (!Number.isInteger(groupId) || groupId <= 0) {
        return;
      }

      const member = await isGroupMember(groupId, socket.username);
      if (!member) {
        return;
      }

      await emitToGroup(
        groupId,
        'chat:typing',
        {
          from: socket.username,
          groupId,
          isTyping
        },
        socket.username
      );
    }
  });

  socket.on('disconnect', async () => {
    removeSocket(socket.username, socket.id);
    emitPresence();

    try {
      await dbRun(
        `UPDATE Users
         SET last_seen = datetime('now')
         WHERE username = ?`,
        [socket.username]
      );
    } catch (err) {
      console.error('Failed to update last_seen:', err);
    }
  });
});

app.use((err, _req, res, _next) => {
  if (err instanceof multer.MulterError) {
    res.status(400).json({ error: err.message });
    return;
  }

  if (err && err.message === 'Unsupported file type') {
    res.status(400).json({ error: err.message });
    return;
  }

  if (err && err.message === 'Unsupported avatar type') {
    res.status(400).json({ error: err.message });
    return;
  }

  console.error('Unhandled server error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

async function startServer() {
  try {
    await dbReady;
  } catch (err) {
    console.error('Database initialization failed:', err);
    process.exit(1);
  }

  const port = Number.parseInt(process.env.PORT || '3000', 10);
  server.listen(port, '0.0.0.0', () => {
    console.log(`LAN Messenger running on http://0.0.0.0:${port}`);
  });
}

setInterval(() => {
  cleanupExpiredSessions().catch((err) => {
    console.error('Failed to clean expired sessions:', err);
  });
  dbRun(`DELETE FROM RateLimits WHERE reset_at <= datetime('now')`).catch((err) => {
    console.error('Failed to clean rate limits:', err);
  });
}, 30 * 60 * 1000).unref();

startServer();
