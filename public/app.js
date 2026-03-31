const EMOJIS = ['😀', '😂', '😍', '👍', '🔥', '🎉', '🤝', '🙌', '😎', '🙂', '❤️', '👏'];

const state = {
  token: '',
  username: '',
  socket: null,
  users: [],
  groups: [],
  groupMembers: [],
  activeChat: null,
  showGroupPanel: false,
  unreadByChat: {},
  recentChats: {},
  searchResults: [],
  searchMeta: {
    query: '',
    oldestId: 0,
    hasMore: false,
    loading: false,
    pageSize: 50
  },
  editingMessageId: 0,
  highlightMessageId: 0,
  dragDepth: 0,
  historyMeta: {
    oldestId: 0,
    hasMore: true,
    loading: false,
    pageSize: 100
  },
  profile: {
    displayName: '',
    statusMessage: '',
    avatarPath: ''
  },
  profileExpanded: false,
  typingTimerId: 0,
  typingHideTimerId: 0,
  typingShowTimerId: 0,
  statusTimerId: 0,
  mediaRecorder: null,
  mediaStream: null,
  mediaChunks: [],
  commandPalette: {
    open: false,
    query: ''
  },
  draftsByChat: {},
  scrollState: {
    paused: false
  },
  mutedChats: {},
  soundEnabled: true,
  compactMode: false,
  soundContext: null,
  visualizerRaf: 0,
  recordingTimerId: 0,
  recordingSeconds: 0
};

const els = {
  authView: document.getElementById('authView'),
  chatView: document.getElementById('chatView'),
  detailsPanel: document.querySelector('.details-panel'),
  authUsername: document.getElementById('authUsername'),
  authPassword: document.getElementById('authPassword'),
  loginButton: document.getElementById('loginButton'),
  registerButton: document.getElementById('registerButton'),
  statusBar: document.getElementById('statusBar'),
  appStatus: document.getElementById('appStatus'),
  currentIdentity: document.getElementById('currentIdentity'),
  currentIdentityText: document.getElementById('currentIdentityText'),
  profileModal: document.getElementById('profileModal'),
  closeProfileModal: document.getElementById('closeProfileModal'),
  profileAvatar: document.getElementById('profileAvatar'),
  profileNameDisplay: document.getElementById('profileNameDisplay'),
  profileStatusDisplay: document.getElementById('profileStatusDisplay'),
  profileDisplayName: document.getElementById('profileDisplayName'),
  profileStatusMessage: document.getElementById('profileStatusMessage'),
  profileAvatarInput: document.getElementById('profileAvatarInput'),
  saveProfileButton: document.getElementById('saveProfileButton'),
  soundToggleButton: document.getElementById('soundToggleButton'),
  compactModeButton: document.getElementById('compactModeButton'),
  recentList: document.getElementById('recentList'),
  searchInput: document.getElementById('searchInput'),
  searchButton: document.getElementById('searchButton'),
  searchTriggerButton: document.getElementById('searchTriggerButton'),
  searchPanel: document.getElementById('searchPanel'),
  searchResults: document.getElementById('searchResults'),
  searchMoreButton: document.getElementById('searchMoreButton'),
  closeSearchButton: document.getElementById('closeSearchButton'),
  commandPalette: document.getElementById('commandPalette'),
  commandPaletteInput: document.getElementById('commandPaletteInput'),
  commandPaletteList: document.getElementById('commandPaletteList'),
  newMessageBanner: document.getElementById('newMessageBanner'),
  userList: document.getElementById('userList'),
  groupList: document.getElementById('groupList'),
  joinCodeInput: document.getElementById('joinCodeInput'),
  joinCodeButton: document.getElementById('joinCodeButton'),
  groupNameInput: document.getElementById('groupNameInput'),
  groupVisibilitySelect: document.getElementById('groupVisibilitySelect'),
  createGroupButton: document.getElementById('createGroupButton'),
  newActionButton: document.getElementById('newActionButton'),
  newActionModal: document.getElementById('newActionModal'),
  closeNewActionModal: document.getElementById('closeNewActionModal'),
  logoutButton: document.getElementById('logoutButton'),
  chatTitle: document.getElementById('chatTitle'),
  chatSubtitle: document.getElementById('chatSubtitle'),
  groupPanelToggle: document.getElementById('groupPanelToggle'),
  markReadButton: document.getElementById('markReadButton'),
  muteChatButton: document.getElementById('muteChatButton'),
  groupPanel: document.getElementById('groupPanel'),
  groupPanelTitle: document.getElementById('groupPanelTitle'),
  refreshMembersButton: document.getElementById('refreshMembersButton'),
  addMemberForm: document.getElementById('addMemberForm'),
  addMemberInput: document.getElementById('addMemberInput'),
  memberList: document.getElementById('memberList'),
  renameGroupInput: document.getElementById('renameGroupInput'),
  renameGroupButton: document.getElementById('renameGroupButton'),
  renameGroupSetting: document.getElementById('renameGroupSetting'),
  transferGroupInput: document.getElementById('transferGroupInput'),
  transferGroupButton: document.getElementById('transferGroupButton'),
  transferGroupSetting: document.getElementById('transferGroupSetting'),
  inviteCodeDisplay: document.getElementById('inviteCodeDisplay'),
  refreshInviteButton: document.getElementById('refreshInviteButton'),
  inviteCodeSetting: document.getElementById('inviteCodeSetting'),
  deleteGroupButton: document.getElementById('deleteGroupButton'),
  deleteGroupSetting: document.getElementById('deleteGroupSetting'),
  leaveGroupButton: document.getElementById('leaveGroupButton'),
  leaveGroupSetting: document.getElementById('leaveGroupSetting'),
  dropOverlay: document.getElementById('dropOverlay'),
  dropHint: document.getElementById('dropHint'),
  messageList: document.getElementById('messageList'),
  typingIndicator: document.getElementById('typingIndicator'),
  composerForm: document.getElementById('composerForm'),
  messageInput: document.getElementById('messageInput'),
  cancelEditButton: document.getElementById('cancelEditButton'),
  sendButton: document.getElementById('sendButton'),
  fileInput: document.getElementById('fileInput'),
  recordButton: document.getElementById('recordButton'),
  emojiButton: document.getElementById('emojiButton'),
  emojiPanel: document.getElementById('emojiPanel'),
  chatPanel: document.querySelector('.chat-panel'),
  deleteAccountButton: document.getElementById('deleteAccountButton'),
  voiceVisualizer: document.getElementById('voiceVisualizer'),
  voiceTimer: document.getElementById('voiceTimer')
};

initialize();

function initialize() {
  try {
    setupEmojiPanel();
    bindEvents();
    loadPreferences();
    setStatus('Login or create a new account to start chatting.');
    autoResizeComposer();
  } catch (err) {
    const authView = document.getElementById('authView');
    if (authView) authView.classList.add('hidden');
    document.body.innerHTML = `<div style="display:grid;place-items:center;min-height:100vh;font-family:sans-serif;padding:20px;">
      <div style="text-align:center;max-width:400px;">
        <p style="font-size:18px;font-weight:700;color:#e53935;">Failed to start</p>
        <p style="color:#555;margin:8px 0 16px;">${String(err && err.message || err)}</p>
        <button onclick="location.reload()" style="padding:10px 24px;background:#3390ec;color:#fff;border:none;border-radius:12px;font-weight:700;cursor:pointer;">Reload</button>
      </div>
    </div>`;
  }
}

window.addEventListener('unhandledrejection', (event) => {
  const msg = event.reason && (event.reason.message || String(event.reason));
  if (msg) setStatus(msg, true);
});

function bindEvents() {
  els.loginButton.addEventListener('click', () => handleAuth('login'));
  els.registerButton.addEventListener('click', () => handleAuth('register'));
  els.currentIdentity.addEventListener('click', openProfileModal);
  els.authPassword.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
      handleAuth('login');
    }
  });

  if (els.closeProfileModal) {
    els.closeProfileModal.addEventListener('click', closeProfileModal);
  }
  if (els.profileModal) {
    els.profileModal.addEventListener('click', (event) => {
      if (event.target === els.profileModal) closeProfileModal();
    });
  }

  if (els.newActionButton) {
    els.newActionButton.addEventListener('click', openNewActionModal);
  }
  if (els.closeNewActionModal) {
    els.closeNewActionModal.addEventListener('click', closeNewActionModal);
  }
  if (els.newActionModal) {
    els.newActionModal.addEventListener('click', (event) => {
      if (event.target === els.newActionModal) closeNewActionModal();
    });
  }

  // Rail tab switching
  document.querySelectorAll('.rail-tab').forEach((tab) => {
    tab.addEventListener('click', () => {
      const panel = tab.dataset.tab;
      document.querySelectorAll('.rail-tab').forEach((t) => {
        t.classList.toggle('active', t.dataset.tab === panel);
        t.setAttribute('aria-selected', t.dataset.tab === panel ? 'true' : 'false');
      });
      document.querySelectorAll('.rail-list').forEach((list) => {
        list.classList.toggle('hidden', list.dataset.panel !== panel);
      });
    });
  });

  // Search trigger opens command palette
  if (els.searchTriggerButton) {
    els.searchTriggerButton.addEventListener('click', () => {
      openCommandPalette();
    });
  }

  els.saveProfileButton.addEventListener('click', saveProfile);
  els.profileAvatarInput.addEventListener('change', uploadAvatar);
  if (els.soundToggleButton) {
    els.soundToggleButton.addEventListener('click', toggleSoundEnabled);
  }
  if (els.compactModeButton) {
    els.compactModeButton.addEventListener('click', toggleCompactMode);
  }
  if (els.deleteAccountButton) {
    els.deleteAccountButton.addEventListener('click', deleteAccount);
  }

  els.searchButton.addEventListener('click', performSearch);
  els.searchInput.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
      performSearch();
    }
  });
  els.closeSearchButton.addEventListener('click', () => {
    clearSearchResults();
  });
  if (els.searchMoreButton) {
    els.searchMoreButton.addEventListener('click', () => {
      fetchSearchResults(true).catch((err) => {
        setStatus(err.message || 'Failed to load more results.', true);
      });
    });
  }

  els.joinCodeButton.addEventListener('click', joinGroupByCode);

  els.createGroupButton.addEventListener('click', createGroup);
  els.logoutButton.addEventListener('click', logout);
  els.refreshMembersButton.addEventListener('click', () => {
    loadGroupMembers().catch((err) => {
      setStatus(err.message || 'Failed to refresh group members.', true);
    });
  });
  els.addMemberForm.addEventListener('submit', addMemberToActiveGroup);
  els.renameGroupButton.addEventListener('click', renameActiveGroup);
  els.transferGroupButton.addEventListener('click', transferActiveGroup);
  els.refreshInviteButton.addEventListener('click', rotateInviteCode);

  if (els.deleteGroupButton) {
    els.deleteGroupButton.addEventListener('click', deleteActiveGroup);
  }
  if (els.leaveGroupButton) {
    els.leaveGroupButton.addEventListener('click', leaveActiveGroup);
  }

  // Panel tab switching (Members / Settings)
  document.querySelectorAll('.panel-tab').forEach((tab) => {
    tab.addEventListener('click', () => {
      const target = tab.dataset.panelTab;
      document.querySelectorAll('.panel-tab').forEach((t) => {
        t.classList.toggle('active', t.dataset.panelTab === target);
        t.setAttribute('aria-selected', t.dataset.panelTab === target ? 'true' : 'false');
      });
      document.querySelectorAll('.panel-tab-content').forEach((content) => {
        content.classList.toggle('hidden', content.dataset.panelContent !== target);
      });
      // Load invite code when switching to settings
      if (target === 'settings' && state.activeChat && state.activeChat.type === 'group') {
        const canManageSettings = state.activeChat.isCreator || state.activeChat.isModerator;
        if (canManageSettings) {
          loadInviteCode().catch(() => {});
        }
      }
    });
  });
  els.groupPanelToggle.addEventListener('click', () => {
    toggleGroupPanel();
  });
  if (els.markReadButton) {
    els.markReadButton.addEventListener('click', markActiveChatRead);
  }
  if (els.muteChatButton) {
    els.muteChatButton.addEventListener('click', toggleMuteForActiveChat);
  }
  els.chatTitle.addEventListener('click', () => {
    toggleGroupPanel();
  });
  els.chatTitle.addEventListener('keydown', (event) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      toggleGroupPanel();
    }
  });

  els.composerForm.addEventListener('submit', sendTextMessage);
  els.messageInput.addEventListener('input', handleComposerInput);
  els.messageInput.addEventListener('keydown', handleComposerKeydown);
  els.fileInput.addEventListener('change', sendSelectedFile);
  els.recordButton.addEventListener('click', toggleVoiceRecording);
  els.cancelEditButton.addEventListener('click', cancelEdit);

  els.emojiButton.addEventListener('click', () => {
    els.emojiPanel.classList.toggle('hidden');
  });

  document.addEventListener('click', (event) => {
    if (
      !els.emojiPanel.contains(event.target) &&
      event.target !== els.emojiButton
    ) {
      els.emojiPanel.classList.add('hidden');
    }
    // Close any open member kebab menus
    if (!event.target.closest('.member-kebab') && !event.target.closest('.member-menu')) {
      document.querySelectorAll('.member-menu').forEach((m) => m.remove());
    }
  });

  document.addEventListener('keydown', handleGlobalShortcuts);

  if (els.commandPaletteInput) {
    els.commandPaletteInput.addEventListener('input', (event) => {
      state.commandPalette.query = String(event.target.value || '');
      renderCommandPalette();
    });
  }

  if (els.commandPalette) {
    els.commandPalette.addEventListener('click', (event) => {
      if (event.target === els.commandPalette) {
        closeCommandPalette();
      }
    });
  }

  if (els.newMessageBanner) {
    els.newMessageBanner.addEventListener('click', () => {
      scrollMessagesToBottom();
    });
  }

  window.addEventListener('beforeunload', () => {
    if (state.socket) {
      state.socket.disconnect();
    }
  });

  if (els.chatPanel) {
    ['dragenter', 'dragover'].forEach((eventName) => {
      els.chatPanel.addEventListener(eventName, handleDragOver);
    });
    ['dragleave', 'drop'].forEach((eventName) => {
      els.chatPanel.addEventListener(eventName, handleDragLeave);
    });
    els.chatPanel.addEventListener('drop', handleDrop);
  }

  if (els.messageList) {
    els.messageList.addEventListener('scroll', handleMessageListScroll);
  }
}

function setupEmojiPanel() {
  EMOJIS.forEach((emoji) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'emoji-option';
    button.textContent = emoji;
    button.addEventListener('click', () => {
      els.messageInput.value += emoji;
      els.messageInput.focus();
      autoResizeComposer();
    });
    els.emojiPanel.appendChild(button);
  });
}

function loadPreferences() {
  const storedSound = window.localStorage.getItem('lanSoundEnabled');
  if (storedSound !== null) {
    state.soundEnabled = storedSound === 'true';
  }
  const storedCompact = window.localStorage.getItem('lanCompactMode');
  if (storedCompact !== null) {
    state.compactMode = storedCompact === 'true';
  }
  updateSoundToggleButton();
  applyCompactMode();
}

function saveRecentChats() {
  if (!state.username) return;
  try {
    window.localStorage.setItem(
      `lanRecent:${state.username}`,
      JSON.stringify(state.recentChats)
    );
  } catch (_e) { /* storage full — ignore */ }
}

function loadRecentChats() {
  if (!state.username) return;
  try {
    const raw = window.localStorage.getItem(`lanRecent:${state.username}`);
    if (raw) {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object') {
        state.recentChats = parsed;
        renderRecentList();
      }
    }
  } catch (_e) { /* corrupt data — ignore */ }
}function updateSoundToggleButton() {
  if (!els.soundToggleButton) {
    return;
  }
  const label = state.soundEnabled ? 'Sounds: On' : 'Sounds: Off';
  els.soundToggleButton.setAttribute('aria-label', label);
  els.soundToggleButton.setAttribute('title', label);
  const text = els.soundToggleButton.querySelector('.sr-only');
  if (text) {
    text.textContent = label;
  }
}

function updateCompactModeButton() {
  if (!els.compactModeButton) {
    return;
  }
  const label = state.compactMode ? 'Compact: On' : 'Compact: Off';
  els.compactModeButton.setAttribute('aria-label', label);
  els.compactModeButton.setAttribute('title', label);
  const text = els.compactModeButton.querySelector('.sr-only');
  if (text) {
    text.textContent = label;
  }
}

function applyCompactMode() {
  document.body.classList.toggle('compact-mode', state.compactMode);
  updateCompactModeButton();
}

function toggleSoundEnabled() {
  state.soundEnabled = !state.soundEnabled;
  window.localStorage.setItem('lanSoundEnabled', String(state.soundEnabled));
  updateSoundToggleButton();
  setStatus(state.soundEnabled ? 'Sounds enabled.' : 'Sounds muted.');
}

function toggleCompactMode() {
  state.compactMode = !state.compactMode;
  window.localStorage.setItem('lanCompactMode', String(state.compactMode));
  applyCompactMode();
  setStatus(state.compactMode ? 'Compact mode enabled.' : 'Compact mode disabled.');
}

function playNotificationSound(kind) {
  if (!state.soundEnabled) {
    return;
  }

  try {
    if (!state.soundContext) {
      const Context = window.AudioContext || window.webkitAudioContext;
      if (!Context) {
        return;
      }
      state.soundContext = new Context();
    }

    const ctx = state.soundContext;
    const oscillator = ctx.createOscillator();
    const gain = ctx.createGain();
    const now = ctx.currentTime;
    const base = kind === 'group' ? 440 : 520;

    oscillator.type = 'sine';
    oscillator.frequency.setValueAtTime(base, now);
    gain.gain.setValueAtTime(0, now);
    gain.gain.linearRampToValueAtTime(0.08, now + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.001, now + 0.3);

    oscillator.connect(gain);
    gain.connect(ctx.destination);
    oscillator.start(now);
    oscillator.stop(now + 0.32);
  } catch (_err) {
    // Ignore audio failures.
  }
}

function handleComposerKeydown(event) {
  if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {
    event.preventDefault();
    if (els.composerForm && typeof els.composerForm.requestSubmit === 'function') {
      els.composerForm.requestSubmit();
    } else {
      sendTextMessage({ preventDefault: () => {} });
    }
  }
}

function handleGlobalShortcuts(event) {
  const key = String(event.key || '').toLowerCase();
  const isChatViewVisible = !els.chatView.classList.contains('hidden');

  if ((event.ctrlKey || event.metaKey) && key === 'k') {
    if (!isChatViewVisible) {
      return;
    }
    event.preventDefault();
    if (state.commandPalette.open) {
      closeCommandPalette();
    } else {
      openCommandPalette();
    }
    return;
  }

  if ((event.ctrlKey || event.metaKey) && key === 'f') {
    if (!isChatViewVisible) {
      return;
    }
    event.preventDefault();
    openCommandPalette();
    return;
  }

  if (event.key === 'Escape') {
    if (state.commandPalette.open) {
      closeCommandPalette();
      return;
    }
    if (state.editingMessageId) {
      cancelEdit();
    }
  }
}

function openCommandPalette() {
  if (!els.commandPalette || !els.commandPaletteInput || !els.commandPaletteList) {
    return;
  }
  state.commandPalette.open = true;
  state.commandPalette.query = '';
  els.commandPaletteInput.value = '';
  els.commandPalette.classList.remove('hidden');
  renderCommandPalette();
  els.commandPaletteInput.focus();
}

function closeCommandPalette() {
  if (!els.commandPalette) {
    return;
  }
  state.commandPalette.open = false;
  els.commandPalette.classList.add('hidden');
}

function renderCommandPalette() {
  if (!els.commandPaletteList) {
    return;
  }
  els.commandPaletteList.innerHTML = '';

  const query = state.commandPalette.query.trim().toLowerCase();
  const entries = [];

  const addEntry = (entry) => {
    if (query) {
      const haystack = `${entry.title} ${entry.meta} ${entry.searchText}`.toLowerCase();
      if (!haystack.includes(query)) {
        return;
      }
    }
    entries.push(entry);
  };

  const recent = Object.values(state.recentChats)
    .sort((a, b) => b.timestamp - a.timestamp)
    .slice(0, 6);

  recent.forEach((chat) => {
    if (chat.type === 'group') {
      const name = resolveGroupName(chat.id);
      addEntry({
        id: `recent-group-${chat.id}`,
        title: `# ${name}`,
        meta: 'Recent group',
        searchText: `${name} ${chat.id}`,
        action: () => openGroupChat(chat.id, name)
      });
    } else {
      const name = resolveDisplayName(chat.id);
      addEntry({
        id: `recent-user-${chat.id}`,
        title: name,
        meta: `Recent • @${chat.id}`,
        searchText: `${name} ${chat.id}`,
        action: () => openPrivateChat(chat.id)
      });
    }
  });

  state.users
    .filter((user) => user.username !== state.username)
    .forEach((user) => {
      const displayName = user.display_name || user.username;
      addEntry({
        id: `user-${user.username}`,
        title: displayName,
        meta: `User • @${user.username}`,
        searchText: `${displayName} ${user.username}`,
        action: () => openPrivateChat(user.username)
      });
    });

  state.groups
    .filter((group) => group.is_member || group.is_public)
    .forEach((group) => {
      const label = group.name;
      const meta = group.is_member ? 'Group' : 'Public group';
      addEntry({
        id: `group-${group.id}`,
        title: `# ${label}`,
        meta,
        searchText: `${label} ${group.id}`,
        action: () => {
          if (group.is_member) {
            openGroupChat(group.id, group.name);
          } else {
            joinGroup(group.id);
          }
        }
      });
    });

  if (!entries.length) {
    const empty = document.createElement('li');
    empty.className = 'empty-item';
    empty.textContent = 'No matches found.';
    els.commandPaletteList.appendChild(empty);
    return;
  }

  entries.slice(0, 12).forEach((entry) => {
    const item = document.createElement('li');
    item.className = 'command-palette-item';
    const button = document.createElement('button');
    button.type = 'button';
    const title = document.createElement('span');
    title.className = 'command-palette-title';
    title.textContent = entry.title;
    const meta = document.createElement('span');
    meta.className = 'command-palette-meta';
    meta.textContent = entry.meta;
    button.appendChild(title);
    button.appendChild(meta);
    button.addEventListener('click', () => {
      closeCommandPalette();
      entry.action();
    });
    item.appendChild(button);
    els.commandPaletteList.appendChild(item);
  });
}

async function handleAuth(mode) {
  const username = String(els.authUsername.value || '').trim();
  const password = String(els.authPassword.value || '');

  if (!/^[a-zA-Z0-9_]{3,20}$/.test(username)) {
    setStatus('User ID must be 3-20 letters, numbers, or _.', true);
    return;
  }

  if (password.length < 6) {
    setStatus('Password must be at least 6 characters.', true);
    return;
  }

  try {
    if (mode === 'register') {
      await api('/auth/register', {
        method: 'POST',
        body: JSON.stringify({ username, password }),
        skipAuth: true
      });
    }

    const loginResult = await api('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
      skipAuth: true
    });

    state.token = loginResult.token;
    state.username = loginResult.username;

    enterChatView();
    connectSocket();
    await Promise.all([refreshUsers(), refreshGroups(), loadProfile()]);
    loadRecentChats();
    setStatus('Connected.', false);
  } catch (err) {
    setStatus(err.message || 'Authentication failed.', true);
  }
}

async function loadProfile() {
  try {
    const profile = await api('/profile');
    state.profile.displayName = profile.display_name || state.username;
    state.profile.statusMessage = profile.status_message || '';
    state.profile.avatarPath = profile.avatar_path || '';
    updateIdentityDisplay();
  } catch (err) {
    setStatus(err.message || 'Failed to load profile.', true);
  }
}

async function saveProfile() {
  const displayName = String(els.profileDisplayName.value || '').trim();
  const statusMessage = String(els.profileStatusMessage.value || '').trim();

  try {
    const result = await api('/profile', {
      method: 'PUT',
      body: JSON.stringify({
        displayName,
        statusMessage
      })
    });

    state.profile.displayName = result.display_name || state.username;
    state.profile.statusMessage = result.status_message || '';
    updateIdentityDisplay();
    await refreshUsers();
    setStatus('Profile updated.');
  } catch (err) {
    setStatus(err.message || 'Failed to update profile.', true);
  }
}

async function uploadAvatar() {
  const file = els.profileAvatarInput.files[0];
  if (!file) {
    return;
  }

  try {
    const formData = new FormData();
    formData.append('avatar', file);

    const result = await api('/profile/avatar', {
      method: 'POST',
      body: formData
    });

    state.profile.avatarPath = result.avatar_path || '';
    updateIdentityDisplay();
    await refreshUsers();
    setStatus('Avatar updated.');
  } catch (err) {
    setStatus(err.message || 'Failed to update avatar.', true);
  } finally {
    els.profileAvatarInput.value = '';
  }
}

function updateIdentityDisplay() {
  const displayName = state.profile.displayName || state.username;
  const statusMessage = state.profile.statusMessage || '';

  els.currentIdentityText.textContent = displayName
    ? `${displayName} (@${state.username})`
    : `Logged in as ${state.username}`;

  els.profileNameDisplay.textContent = displayName || state.username;
  els.profileStatusDisplay.textContent = statusMessage || 'No status';
  els.profileDisplayName.value = displayName || '';
  els.profileStatusMessage.value = statusMessage || '';
  setAvatarElement(els.profileAvatar, displayName || state.username, state.profile.avatarPath);
}

function openProfileModal() {
  if (!els.profileModal) return;
  els.profileModal.classList.remove('hidden');
  els.currentIdentity.setAttribute('aria-expanded', 'true');
}

function closeProfileModal() {
  if (!els.profileModal) return;
  els.profileModal.classList.add('hidden');
  els.currentIdentity.setAttribute('aria-expanded', 'false');
}

function openNewActionModal() {
  if (!els.newActionModal) return;
  els.newActionModal.classList.remove('hidden');
}

function closeNewActionModal() {
  if (!els.newActionModal) return;
  els.newActionModal.classList.add('hidden');
}

function setAvatarElement(element, name, avatarPath) {
  if (!element) {
    return;
  }

  if (avatarPath) {
    element.style.backgroundImage = `url(/uploads/${encodeURIComponent(avatarPath)})`;
    element.textContent = '';
    return;
  }

  element.style.backgroundImage = '';
  element.textContent = getInitials(name);
}

function getInitials(name) {
  const cleaned = String(name || '').trim();
  if (!cleaned) {
    return '?';
  }

  const parts = cleaned.split(/\s+/).slice(0, 2);
  return parts.map((part) => part[0].toUpperCase()).join('');
}

function createAvatarElement(name, avatarPath) {
  const avatar = document.createElement('span');
  avatar.className = 'avatar';
  setAvatarElement(avatar, name, avatarPath);
  return avatar;
}

function renderEmptyChatState(mode) {
  let badge = 'LAN Messenger';
  let title = 'Choose a conversation';
  let copy = 'Pick a user or group from the sidebar to start messaging.';
  let hint = 'Recent chats, users, and groups stay available on the left so you can jump back in quickly.';

  if (mode === 'conversation' && state.activeChat) {
    if (state.activeChat.type === 'group') {
      badge = 'Group chat';
      title = `# ${state.activeChat.title}`;
      copy = 'This group is ready, but no one has posted yet.';
      hint = 'Send the first message, share a file, or drop an image here to get the conversation moving.';
    } else {
      badge = 'Direct chat';
      title = state.activeChat.title;
      copy = 'No messages yet in this conversation.';
      hint = 'Say hello, send a file, or record a quick voice note to start the exchange.';
    }
  }

  els.messageList.innerHTML = '';
  els.messageList.classList.add('is-empty');

  const card = document.createElement('section');
  card.className = 'empty-chat';

  const badgeEl = document.createElement('div');
  badgeEl.className = 'empty-chat-badge';
  badgeEl.textContent = badge;

  const titleEl = document.createElement('h4');
  titleEl.className = 'empty-chat-title';
  titleEl.textContent = title;

  const copyEl = document.createElement('p');
  copyEl.className = 'empty-chat-copy';
  copyEl.textContent = copy;

  const hintEl = document.createElement('p');
  hintEl.className = 'empty-chat-hint';
  hintEl.textContent = hint;

  card.appendChild(badgeEl);
  card.appendChild(titleEl);
  card.appendChild(copyEl);
  card.appendChild(hintEl);
  els.messageList.appendChild(card);
}

function enterChatView() {
  els.authView.classList.add('hidden');
  els.chatView.classList.remove('hidden');
  updateIdentityDisplay();
  if (!state.activeChat) {
    renderEmptyChatState('idle');
  }
}

function leaveChatView() {
  els.chatView.classList.add('hidden');
  els.authView.classList.remove('hidden');
  els.currentIdentityText.textContent = '';
  closeProfileModal();
  els.messageList.innerHTML = '';
  els.messageList.classList.remove('is-empty');
  els.chatTitle.textContent = 'Select a user or group';
  els.chatSubtitle.textContent = 'No active conversation';
  els.typingIndicator.textContent = '';
}

function connectSocket() {
  if (state.socket) {
    state.socket.disconnect();
  }

  state.socket = io({
    transports: ['websocket'],
    auth: { token: state.token },
    reconnection: true,
    reconnectionDelay: 300,
    reconnectionDelayMax: 2000
  });

  state.socket.on('connect', () => {
    setStatus('Realtime channel connected.');
  });

  state.socket.on('disconnect', () => {
    setStatus('Realtime channel disconnected. Reconnecting...');
  });

  state.socket.on('connect_error', (error) => {
    setStatus(error.message || 'Socket connection failed.', true);
  });

  state.socket.on('presence:update', () => {
    refreshUsers().catch(() => {});
  });

  state.socket.on('chat:message', (message) => {
    handleIncomingMessage(message);
  });

  state.socket.on('chat:message-updated', (message) => {
    handleMessageUpdate(message);
  });

  state.socket.on('chat:seen', (payload) => {
    handleSeenNotification(payload);
  });

  state.socket.on('chat:reaction', (payload) => {
    handleReactionUpdate(payload);
  });

  state.socket.on('chat:typing', (payload) => {
    handleTypingNotification(payload);
  });

  state.socket.on('group:members-updated', (payload) => {
    refreshGroups().catch(() => {});

    if (!state.activeChat || state.activeChat.type !== 'group') {
      return;
    }

    if (Number(payload.groupId) === Number(state.activeChat.id)) {
      if (state.showGroupPanel) {
        loadGroupMembers().catch(() => {});
      }
    }
  });

  state.socket.on('group:membership-changed', (payload) => {
    refreshGroups().catch(() => {});

    if (!payload || !state.activeChat || state.activeChat.type !== 'group') {
      return;
    }

    if (Number(payload.groupId) !== Number(state.activeChat.id)) {
      return;
    }

    if (payload.action === 'removed') {
      resetActiveChat('You were removed from this group.');
      return;
    }

    if (payload.action === 'role') {
      if (state.showGroupPanel) {
        loadGroupMembers().catch(() => {});
      }
      if (payload.role) {
        const label = payload.role === 'moderator' ? 'a moderator' : 'a member';
        setStatus(`You are now ${label} in ${payload.groupName || 'the group'}.`);
      }
      return;
    }

    if (payload.action === 'added') {
      if (state.showGroupPanel) {
        loadGroupMembers().catch(() => {});
      }
    }
  });

  state.socket.on('group:updated', () => {
    refreshGroups().catch(() => {});
  });

  state.socket.on('group:deleted', (payload) => {
    refreshGroups().catch(() => {});
    if (
      state.activeChat &&
      state.activeChat.type === 'group' &&
      Number(payload.groupId) === Number(state.activeChat.id)
    ) {
      resetActiveChat(`Group "${payload.groupName || 'the group'}" was deleted.`);
    }
  });
}

async function refreshUsers() {
  state.users = await api('/users');
  renderUsers();
  renderRecentList();

  if (state.activeChat && state.activeChat.type === 'private') {
    state.activeChat.title = resolveDisplayName(state.activeChat.id) || state.activeChat.id;
    updateChatHeader();
  }
}

async function refreshGroups() {
  state.groups = await api('/groups');
  renderGroups();
  renderRecentList();

  if (!state.activeChat || state.activeChat.type !== 'group') {
    return;
  }

  const activeGroup = state.groups.find(
    (group) => Number(group.id) === Number(state.activeChat.id)
  );

  if (!activeGroup || !activeGroup.is_member) {
    resetActiveChat('You are not a member of the previously opened group.');
    return;
  }

  state.activeChat.title = activeGroup.name;
  state.activeChat.isCreator = Boolean(activeGroup.is_creator);
  state.activeChat.isModerator = Boolean(activeGroup.is_moderator);
  updateChatHeader();
}

function renderUsers() {
  els.userList.innerHTML = '';

  const users = state.users.filter((user) => user.username !== state.username);

  if (users.length === 0) {
    els.userList.appendChild(createEmptyItem('No other users available.'));
    return;
  }

  users.forEach((user) => {
    const item = document.createElement('li');
    item.className = 'list-item';

    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'chat-target';
    const displayName = user.display_name || user.username;
    const metaText = `@${user.username}${user.status_message ? ` • ${user.status_message}` : ''}`;

    if (
      state.activeChat &&
      state.activeChat.type === 'private' &&
      state.activeChat.id === user.username
    ) {
      button.classList.add('active');
    }

    const dot = document.createElement('span');
    dot.className = user.online ? 'status-dot online' : 'status-dot';

    const avatar = createAvatarElement(displayName, user.avatar_path || '');

    const label = document.createElement('div');
    label.className = 'chat-label';

    const nameLine = document.createElement('span');
    nameLine.className = 'chat-name';
    nameLine.textContent = displayName;

    const metaLine = document.createElement('span');
    metaLine.className = 'chat-meta';
    metaLine.textContent = metaText;

    label.appendChild(nameLine);
    label.appendChild(metaLine);

    button.append(dot, avatar, label);

    button.addEventListener('click', () => {
      openPrivateChat(user.username);
    });

    const unread = getUnreadCount(chatKeyForUser(user.username));
    if (unread > 0) {
      const badge = document.createElement('span');
      badge.className = 'unread-badge';
      badge.textContent = String(unread);
      button.appendChild(badge);
    }

    item.appendChild(button);
    els.userList.appendChild(item);
  });
}

function renderGroups() {
  els.groupList.innerHTML = '';

  if (state.groups.length === 0) {
    els.groupList.appendChild(createEmptyItem('No groups yet.'));
    return;
  }

  state.groups.forEach((group) => {
    const item = document.createElement('li');
    item.className = 'list-item';

    const chatButton = document.createElement('button');
    chatButton.type = 'button';
    chatButton.className = 'chat-target';
    const avatar = createAvatarElement(group.name, '');
    const label = document.createElement('div');
    label.className = 'chat-label';

    const nameLine = document.createElement('span');
    nameLine.className = 'chat-name';
    nameLine.textContent = `# ${group.name}`;

    const metaLine = document.createElement('span');
    metaLine.className = 'chat-meta';
    if (group.is_member) {
      metaLine.textContent = `${group.member_count || 0} members`;
    } else if (group.is_public) {
      metaLine.textContent = 'Tap to join';
    } else {
      metaLine.textContent = 'Private • Invite only';
    }

    label.appendChild(nameLine);
    label.appendChild(metaLine);
    chatButton.appendChild(avatar);
    chatButton.appendChild(label);

    if (
      state.activeChat &&
      state.activeChat.type === 'group' &&
      Number(state.activeChat.id) === Number(group.id)
    ) {
      chatButton.classList.add('active');
    }

    if (group.is_member) {
      chatButton.addEventListener('click', () => openGroupChat(group.id, group.name));
    } else if (group.is_public) {
      chatButton.classList.add('locked');
      chatButton.addEventListener('click', () => joinGroup(group.id));
    } else {
      chatButton.classList.add('locked');
      chatButton.addEventListener('click', () => {
        setStatus('This group is private. Ask a moderator to add you.', true);
      });
    }

    const unread = getUnreadCount(chatKeyForGroup(group.id));
    if (unread > 0) {
      const badge = document.createElement('span');
      badge.className = 'unread-badge';
      badge.textContent = String(unread);
      chatButton.appendChild(badge);
    }

    item.appendChild(chatButton);
    els.groupList.appendChild(item);
  });
}

function createEmptyItem(text) {
  const item = document.createElement('li');
  item.className = 'empty-item';
  item.textContent = text;
  return item;
}

async function createGroup() {
  const name = String(els.groupNameInput.value || '').trim();
  const visibility = String(els.groupVisibilitySelect.value || 'public');
  const isPublic = visibility !== 'private';

  if (name.length < 2) {
    setStatus('Group name must be at least 2 characters.', true);
    return;
  }

  try {
    const group = await api('/groups', {
      method: 'POST',
      body: JSON.stringify({ name, isPublic })
    });

    els.groupNameInput.value = '';
    els.groupVisibilitySelect.value = 'public';
    closeNewActionModal();
    await refreshGroups();
    openGroupChat(group.id, group.name);
  } catch (err) {
    setStatus(err.message || 'Failed to create group.', true);
  }
}

async function joinGroup(groupId) {
  try {
    await api(`/groups/${groupId}/join`, { method: 'POST' });
    await refreshGroups();

    const group = state.groups.find((entry) => Number(entry.id) === Number(groupId));
    if (group) {
      openGroupChat(group.id, group.name);
    }
  } catch (err) {
    setStatus(err.message || 'Failed to join group.', true);
  }
}

async function openPrivateChat(username) {
  saveCurrentDraft();
  cancelEdit();
  state.groupMembers = [];
  state.showGroupPanel = false;
  resetHistoryMeta();
  state.activeChat = {
    type: 'private',
    id: username,
    title: resolveDisplayName(username) || username
  };

  clearUnread(chatKeyForUser(username));
  renderUsers();
  renderGroups();
  renderRecentList();
  updateChatHeader();
  restoreDraftForChat();
  await loadHistory();
}

async function openGroupChat(groupId, groupName) {
  const groupMeta = state.groups.find((group) => Number(group.id) === Number(groupId));

  saveCurrentDraft();
  cancelEdit();
  state.groupMembers = [];
  state.showGroupPanel = false;
  resetHistoryMeta();
  state.activeChat = {
    type: 'group',
    id: Number(groupId),
    title: groupName,
    isCreator: Boolean(groupMeta && groupMeta.is_creator),
    isModerator: Boolean(groupMeta && groupMeta.is_moderator)
  };

  clearUnread(chatKeyForGroup(groupId));
  renderUsers();
  renderGroups();
  renderRecentList();
  updateChatHeader();
  restoreDraftForChat();
  await loadHistory();
}

function setDetailsPanelVisible(visible) {
  if (!els.detailsPanel || !els.chatView) {
    return;
  }
  els.detailsPanel.classList.toggle('hidden', !visible);
  els.chatView.classList.toggle('with-details', visible);
}

function updateChatHeader() {
  if (!state.activeChat) {
    els.chatTitle.textContent = 'Select a user or group';
    els.chatSubtitle.textContent = 'No active conversation';
    els.groupPanel.classList.add('hidden');
    els.groupPanelToggle.classList.add('hidden');
    if (els.markReadButton) {
      els.markReadButton.classList.add('hidden');
    }
    if (els.muteChatButton) {
      els.muteChatButton.classList.add('hidden');
    }
    els.chatTitle.classList.remove('clickable');
    els.chatTitle.setAttribute('aria-expanded', 'false');
    els.groupPanelToggle.setAttribute('aria-expanded', 'false');
    setDetailsPanelVisible(false);
    renderEmptyChatState('idle');
    return;
  }

  if (state.activeChat.type === 'private') {
    els.chatTitle.textContent = state.activeChat.title;
    els.chatSubtitle.textContent = 'Direct chat';
    els.groupPanel.classList.add('hidden');
    els.groupPanelToggle.classList.add('hidden');
    els.chatTitle.classList.remove('clickable');
    state.showGroupPanel = false;
    els.chatTitle.setAttribute('aria-expanded', 'false');
    els.groupPanelToggle.setAttribute('aria-expanded', 'false');
    setDetailsPanelVisible(false);
    if (els.markReadButton) {
      els.markReadButton.classList.remove('hidden');
    }
    updateMuteButton();
  } else {
    els.chatTitle.textContent = `# ${state.activeChat.title}`;
    let subtitle = 'Group chat';
    if (state.activeChat.isCreator) {
      subtitle = 'Group chat • You are creator';
    } else if (state.activeChat.isModerator) {
      subtitle = 'Group chat • You are moderator';
    }
    els.chatSubtitle.textContent = subtitle;
    els.groupPanel.classList.toggle('hidden', !state.showGroupPanel);
    els.groupPanelToggle.classList.remove('hidden');
    const toggleLabel = state.showGroupPanel ? 'Hide panel' : 'Members';
    els.groupPanelToggle.setAttribute('aria-label', toggleLabel);
    els.groupPanelToggle.setAttribute('title', toggleLabel);
    const toggleText = els.groupPanelToggle.querySelector('.sr-only');
    if (toggleText) toggleText.textContent = toggleLabel;
    els.chatTitle.classList.add('clickable');
    els.chatTitle.setAttribute('aria-expanded', state.showGroupPanel ? 'true' : 'false');
    els.groupPanelToggle.setAttribute(
      'aria-expanded',
      state.showGroupPanel ? 'true' : 'false'
    );
    setDetailsPanelVisible(state.showGroupPanel);
    if (els.markReadButton) {
      els.markReadButton.classList.remove('hidden');
    }
    updateMuteButton();
  }

  els.typingIndicator.textContent = '';
}

function toggleGroupPanel() {
  if (!state.activeChat || state.activeChat.type !== 'group') {
    return;
  }

  state.showGroupPanel = !state.showGroupPanel;
  updateChatHeader();

  if (!state.showGroupPanel) {
    return;
  }

  loadGroupMembers().catch((err) => {
    setStatus(err.message || 'Failed to load group members.', true);
  });
}

function resetHistoryMeta() {
  state.historyMeta.oldestId = 0;
  state.historyMeta.hasMore = true;
  state.historyMeta.loading = false;
}

async function loadHistory(options = {}) {
  if (!state.activeChat) {
    return;
  }

  try {
    if (state.historyMeta.loading) {
      return;
    }
    state.historyMeta.loading = true;

    const endpoint =
      state.activeChat.type === 'private'
        ? `/history/private/${encodeURIComponent(state.activeChat.id)}`
        : `/history/group/${state.activeChat.id}`;

    const params = new URLSearchParams();
    params.set('limit', String(state.historyMeta.pageSize));
    if (options.older && state.historyMeta.oldestId) {
      params.set('beforeId', String(state.historyMeta.oldestId));
    }

    const history = await api(`${endpoint}?${params.toString()}`);
    if (options.older) {
      prependHistory(history);
    } else {
      renderHistory(history);
    }
    if (history.length) {
      state.historyMeta.oldestId = history[0].id;
      if (!options.older) {
        updateRecentFromMessage(history[history.length - 1]);
        renderRecentList();
      }
    }
    state.historyMeta.hasMore = history.length === state.historyMeta.pageSize;
  } catch (err) {
    if (
      state.activeChat &&
      state.activeChat.type === 'group' &&
      String(err.message || '').includes('Join this group first')
    ) {
      resetActiveChat('You are no longer a member of this group.');
      return;
    }

    setStatus(err.message || 'Failed to load messages.', true);
  } finally {
    state.historyMeta.loading = false;
  }
}

async function loadGroupMembers() {
  if (!state.activeChat || state.activeChat.type !== 'group') {
    state.groupMembers = [];
    els.groupPanel.classList.add('hidden');
    setDetailsPanelVisible(false);
    return;
  }

  const details = await api(`/groups/${state.activeChat.id}/members`);
  state.groupMembers = details.members || [];
  state.activeChat.isCreator = details.creator === state.username;
  state.activeChat.isModerator = state.groupMembers.some(
    (member) => member.username === state.username && member.role === 'moderator'
  );
  renderGroupMembers(details.creator);
  updateChatHeader();
}

function renderGroupMembers(creatorUsername) {
  if (!state.activeChat || state.activeChat.type !== 'group') {
    state.groupMembers = [];
    els.groupPanel.classList.add('hidden');
    setDetailsPanelVisible(false);
    return;
  }

  els.groupPanel.classList.toggle('hidden', !state.showGroupPanel);
  setDetailsPanelVisible(state.showGroupPanel);

  const canManageMembers = state.activeChat.isCreator || state.activeChat.isModerator;
  const canManageSettings = state.activeChat.isCreator || state.activeChat.isModerator;

  const creatorLabel = resolveDisplayName(creatorUsername);
  els.groupPanelTitle.textContent = `# ${state.activeChat.title}`;
  els.addMemberForm.classList.toggle('hidden', !canManageMembers);

  // Show/hide settings fields based on role
  if (els.renameGroupSetting) {
    els.renameGroupSetting.classList.toggle('hidden', !state.activeChat.isCreator);
  }
  if (els.transferGroupSetting) {
    els.transferGroupSetting.classList.toggle('hidden', !state.activeChat.isCreator);
  }
  if (els.inviteCodeSetting) {
    els.inviteCodeSetting.classList.toggle('hidden', !canManageSettings);
  }
  if (els.deleteGroupSetting) {
    els.deleteGroupSetting.classList.toggle('hidden', !state.activeChat.isCreator);
  }
  // Show leave button for members who are not the creator
  if (els.leaveGroupSetting) {
    els.leaveGroupSetting.classList.toggle('hidden', state.activeChat.isCreator);
  }

  // Load invite code if settings tab is active
  const activeTab = document.querySelector('.panel-tab.active');
  if (activeTab && activeTab.dataset.panelTab === 'settings' && canManageSettings) {
    loadInviteCode().catch(() => {});
  } else if (!canManageSettings) {
    els.inviteCodeDisplay.value = '';
  }

  els.memberList.innerHTML = '';

  if (!state.groupMembers.length) {
    const item = document.createElement('li');
    item.className = 'member-empty';
    item.textContent = 'No members found.';
    els.memberList.appendChild(item);
    return;
  }

  state.groupMembers.forEach((member) => {
    const item = document.createElement('li');
    item.className = 'member-item';

    // Online dot
    const dot = document.createElement('span');
    dot.className = member.online ? 'member-dot online' : 'member-dot';

    // Avatar
    const avatarEl = document.createElement('span');
    avatarEl.className = 'member-avatar';
    const displayName = resolveDisplayName(member.username);
    setAvatarElement(avatarEl, displayName, '');

    // Info: name + role chip
    const info = document.createElement('div');
    info.className = 'member-info';

    const nameEl = document.createElement('span');
    nameEl.className = 'member-name';
    nameEl.textContent = displayName !== member.username
      ? `${displayName}`
      : member.username;
    nameEl.title = `@${member.username}`;
    info.appendChild(nameEl);

    if (member.is_creator) {
      const chip = document.createElement('span');
      chip.className = 'member-role-chip creator';
      chip.textContent = 'Creator';
      info.appendChild(chip);
    } else if (member.is_moderator) {
      const chip = document.createElement('span');
      chip.className = 'member-role-chip';
      chip.textContent = 'Moderator';
      info.appendChild(chip);
    }

    item.append(dot, avatarEl, info);

    // Kebab menu button (only if there are actions available)
    const isSelf = member.username === state.username;
    const hasActions = !member.is_creator && (
      isSelf ||
      canManageMembers ||
      (state.activeChat.isCreator && !member.is_creator)
    );

    if (hasActions) {
      const kebab = document.createElement('button');
      kebab.type = 'button';
      kebab.className = 'member-kebab';
      kebab.setAttribute('aria-label', 'Member actions');
      kebab.innerHTML = `<svg class="icon" viewBox="0 0 24 24" aria-hidden="true">
        <circle cx="12" cy="5" r="1.5" fill="currentColor"/>
        <circle cx="12" cy="12" r="1.5" fill="currentColor"/>
        <circle cx="12" cy="19" r="1.5" fill="currentColor"/>
      </svg>`;

      kebab.addEventListener('click', (event) => {
        event.stopPropagation();
        // Remove any existing menus
        document.querySelectorAll('.member-menu').forEach((m) => m.remove());

        const menu = document.createElement('div');
        menu.className = 'member-menu';
        menu.setAttribute('role', 'menu');

        if (state.activeChat.isCreator && !member.is_creator) {
          const modItem = document.createElement('button');
          modItem.type = 'button';
          modItem.className = 'member-menu-item';
          modItem.setAttribute('role', 'menuitem');
          modItem.textContent = member.is_moderator ? 'Remove moderator' : 'Make moderator';
          modItem.addEventListener('click', () => {
            menu.remove();
            setModeratorForMember(member.username, !member.is_moderator).catch((err) => {
              setStatus(err.message || 'Failed to update moderator.', true);
            });
          });
          menu.appendChild(modItem);
        }

        if (isSelf) {
          const leaveItem = document.createElement('button');
          leaveItem.type = 'button';
          leaveItem.className = 'member-menu-item danger';
          leaveItem.setAttribute('role', 'menuitem');
          leaveItem.textContent = 'Leave group';
          leaveItem.addEventListener('click', () => {
            menu.remove();
            removeMemberFromActiveGroup(member.username).catch((err) => {
              setStatus(err.message || 'Failed to leave group.', true);
            });
          });
          menu.appendChild(leaveItem);
        } else if (canManageMembers) {
          const removeItem = document.createElement('button');
          removeItem.type = 'button';
          removeItem.className = 'member-menu-item danger';
          removeItem.setAttribute('role', 'menuitem');
          removeItem.textContent = 'Remove from group';
          removeItem.addEventListener('click', () => {
            menu.remove();
            removeMemberFromActiveGroup(member.username).catch((err) => {
              setStatus(err.message || 'Failed to remove member.', true);
            });
          });
          menu.appendChild(removeItem);
        }

        if (menu.children.length > 0) {
          item.appendChild(menu);
        }
      });

      item.appendChild(kebab);
    }

    els.memberList.appendChild(item);
  });
}

async function addMemberToActiveGroup(event) {
  event.preventDefault();

  const canManageMembers = state.activeChat &&
    state.activeChat.type === 'group' &&
    (state.activeChat.isCreator || state.activeChat.isModerator);

  if (!canManageMembers) {
    setStatus('Only the creator or a moderator can add members.', true);
    return;
  }

  const targetUsername = String(els.addMemberInput.value || '').trim();
  if (!/^[a-zA-Z0-9_]{3,20}$/.test(targetUsername)) {
    setStatus('Invalid user ID format.', true);
    return;
  }

  await api(`/groups/${state.activeChat.id}/members`, {
    method: 'POST',
    body: JSON.stringify({ username: targetUsername })
  });

  els.addMemberInput.value = '';
  await loadGroupMembers();
  setStatus(`Member ${targetUsername} added to the group.`);
}

async function setModeratorForMember(memberUsername, makeModerator) {
  if (!state.activeChat || state.activeChat.type !== 'group' || !state.activeChat.isCreator) {
    setStatus('Only the creator can manage moderators.', true);
    return;
  }

  await api(`/groups/${state.activeChat.id}/moderators`, {
    method: 'POST',
    body: JSON.stringify({
      username: memberUsername,
      makeModerator
    })
  });

  await loadGroupMembers();
  setStatus(
    makeModerator
      ? `${memberUsername} is now a moderator.`
      : `${memberUsername} is no longer a moderator.`
  );
}

async function renameActiveGroup() {
  if (!state.activeChat || state.activeChat.type !== 'group' || !state.activeChat.isCreator) {
    setStatus('Only the creator can rename the group.', true);
    return;
  }

  const name = String(els.renameGroupInput.value || '').trim();
  if (name.length < 2) {
    setStatus('Group name must be at least 2 characters.', true);
    return;
  }

  await api(`/groups/${state.activeChat.id}`, {
    method: 'PATCH',
    body: JSON.stringify({ name })
  });

  els.renameGroupInput.value = '';
  await refreshGroups();
  setStatus('Group renamed.');
}

async function transferActiveGroup() {
  if (!state.activeChat || state.activeChat.type !== 'group' || !state.activeChat.isCreator) {
    setStatus('Only the creator can transfer ownership.', true);
    return;
  }

  const targetUsername = String(els.transferGroupInput.value || '').trim();
  if (!/^[a-zA-Z0-9_]{3,20}$/.test(targetUsername)) {
    setStatus('Invalid user ID format.', true);
    return;
  }

  await api(`/groups/${state.activeChat.id}/transfer`, {
    method: 'POST',
    body: JSON.stringify({ username: targetUsername })
  });

  els.transferGroupInput.value = '';
  await refreshGroups();
  if (state.showGroupPanel) {
    loadGroupMembers().catch(() => {});
  }
  setStatus(`Group ownership transferred to ${targetUsername}.`);
}

async function loadInviteCode() {
  if (!state.activeChat || state.activeChat.type !== 'group') {
    return;
  }

  const data = await api(`/groups/${state.activeChat.id}/invite`);
  els.inviteCodeDisplay.value = data.invite_code || '';
}

async function rotateInviteCode() {
  if (!state.activeChat || state.activeChat.type !== 'group' || !state.activeChat.isCreator) {
    setStatus('Only the creator can rotate invite codes.', true);
    return;
  }

  const data = await api(`/groups/${state.activeChat.id}/invite/rotate`, {
    method: 'POST'
  });
  els.inviteCodeDisplay.value = data.invite_code || '';
  setStatus('Invite code rotated.');
}

async function joinGroupByCode() {
  const inviteCode = String(els.joinCodeInput.value || '').trim();
  if (!inviteCode) {
    setStatus('Enter an invite code first.', true);
    return;
  }

  try {
    const group = await api('/groups/join-by-code', {
      method: 'POST',
      body: JSON.stringify({ inviteCode })
    });

    els.joinCodeInput.value = '';
    closeNewActionModal();
    await refreshGroups();
    openGroupChat(group.id, group.name);
    setStatus(`Joined ${group.name}.`);
  } catch (err) {
    setStatus(err.message || 'Failed to join by code.', true);
  }
}

async function removeMemberFromActiveGroup(memberUsername) {
  if (!state.activeChat || state.activeChat.type !== 'group') {
    return;
  }

  await api(
    `/groups/${state.activeChat.id}/members/${encodeURIComponent(memberUsername)}`,
    { method: 'DELETE' }
  );

  if (memberUsername === state.username) {
    await refreshGroups();
    resetActiveChat('You left the group.');
    return;
  }

  await loadGroupMembers();
  setStatus(`Member ${memberUsername} removed from group.`);
}

async function leaveActiveGroup() {
  if (!state.activeChat || state.activeChat.type !== 'group') return;
  if (!window.confirm(`Leave "${state.activeChat.title}"?`)) return;
  try {
    await removeMemberFromActiveGroup(state.username);
  } catch (err) {
    setStatus(err.message || 'Failed to leave group.', true);
  }
}

async function deleteActiveGroup() {
  if (!state.activeChat || state.activeChat.type !== 'group') return;
  if (!state.activeChat.isCreator) {
    setStatus('Only the creator can delete this group.', true);
    return;
  }
  if (!window.confirm(`Permanently delete "${state.activeChat.title}"? This cannot be undone.`)) return;
  try {
    await api(`/groups/${state.activeChat.id}`, { method: 'DELETE' });
    await refreshGroups();
    resetActiveChat(`Group deleted.`);
  } catch (err) {
    setStatus(err.message || 'Failed to delete group.', true);
  }
}

function resetActiveChat(statusText) {
  saveCurrentDraft();
  state.activeChat = null;
  state.groupMembers = [];
  state.showGroupPanel = false;
  state.highlightMessageId = 0;
  resetHistoryMeta();
  hideNewMessageBanner();
  cancelEdit();
  els.messageList.innerHTML = '';
  els.memberList.innerHTML = '';
  els.addMemberInput.value = '';
  els.groupPanel.classList.add('hidden');
  updateChatHeader();
  renderUsers();
  renderGroups();

  if (statusText) {
    setStatus(statusText);
  }
}

function renderHistory(messages) {
  els.messageList.innerHTML = '';
  state.scrollState.paused = false;
  hideNewMessageBanner();

  if (!messages.length) {
    renderEmptyChatState('conversation');
    return;
  }

  els.messageList.classList.remove('is-empty');
  messages.forEach((message) => appendMessage(message, false));

  if (state.highlightMessageId) {
    highlightMessageInView(state.highlightMessageId);
    state.highlightMessageId = 0;
    return;
  }

  scrollMessagesToBottom();
}

function prependHistory(messages) {
  if (!messages.length) {
    return;
  }

  if (els.messageList.classList.contains('is-empty')) {
    renderHistory(messages);
    return;
  }

  const previousHeight = els.messageList.scrollHeight;
  const previousScrollTop = els.messageList.scrollTop;
  const fragment = document.createDocumentFragment();

  messages.forEach((message) => {
    const row = createMessageElement(message);
    if (row) {
      fragment.appendChild(row);
    }
  });

  els.messageList.insertBefore(fragment, els.messageList.firstChild);
  const newHeight = els.messageList.scrollHeight;
  els.messageList.scrollTop = previousScrollTop + (newHeight - previousHeight);
}

function appendMessage(message, shouldScroll) {
  const messageRow = createMessageElement(message);
  if (!messageRow) {
    return;
  }

  els.messageList.classList.remove('is-empty');
  els.messageList.appendChild(messageRow);

  if (shouldScroll) {
    scrollMessagesToBottom();
  }
}

function highlightMessageInView(messageId) {
  const row = document.getElementById(`message-${messageId}`);
  if (!row) {
    setStatus('Message is not in the recent history view.', true);
    scrollMessagesToBottom();
    return;
  }

  row.classList.add('highlight');
  row.scrollIntoView({ block: 'center', behavior: 'smooth' });

  window.setTimeout(() => {
    row.classList.remove('highlight');
  }, 1600);
}

function createMessageElement(message) {
  if (!message) {
    return null;
  }

  const row = document.createElement('div');
  row.id = `message-${message.id}`;
  row.className =
    message.sender === state.username ? 'message-row outgoing' : 'message-row incoming';

  const bubble = document.createElement('article');
  bubble.className = 'bubble';

  const meta = document.createElement('p');
  meta.className = 'meta';
  const senderLabel =
    message.sender === state.username ? 'You' : resolveDisplayName(message.sender);
  let metaText = `${senderLabel} • ${formatTime(message.timestamp)}`;
  if (message.deleted_at) {
    metaText += ' • deleted';
  } else if (message.edited_at) {
    metaText += ' • ';
  }
  meta.textContent = metaText;

  // Edited history link
  if (message.edited_at && !message.deleted_at) {
    const editedLink = document.createElement('button');
    editedLink.type = 'button';
    editedLink.className = 'edited-link';
    editedLink.textContent = 'edited';
    editedLink.addEventListener('click', () => showEditHistory(message.id, editedLink));
    meta.appendChild(editedLink);
  }

  // Seen tick for outgoing private messages
  if (message.sender === state.username && !message.group_id) {
    const tick = document.createElement('span');
    tick.className = 'seen-tick';
    tick.dataset.messageId = message.id;
    tick.textContent = message.seen ? ' ✓✓' : ' ✓';
    tick.title = message.seen ? 'Seen' : 'Sent';
    meta.appendChild(tick);
  }

  bubble.appendChild(meta);

  if (message.deleted_at) {
    const deleted = document.createElement('p');
    deleted.className = 'deleted-message';
    deleted.textContent = 'This message was deleted.';
    bubble.appendChild(deleted);
  } else {
    if (message.message) {
      const content = document.createElement('p');
      content.className = 'text-content';
      content.textContent = message.message;
      bubble.appendChild(content);
    }

    if (message.file_path) {
      const fileUrl = `/uploads/${encodeURIComponent(message.file_path)}`;
      if (message.message_type === 'audio') {
        const audio = document.createElement('audio');
        audio.controls = true;
        audio.src = fileUrl;
        bubble.appendChild(audio);
      } else if (message.message_type === 'image') {
        const img = document.createElement('img');
        img.className = 'image-preview';
        img.src = fileUrl;
        img.alt = message.file_name || 'Image';
        bubble.appendChild(img);
      } else {
        const fileLink = document.createElement('a');
        fileLink.className = 'file-link';
        fileLink.href = fileUrl;
        fileLink.download = message.file_name || message.file_path;
        fileLink.textContent = `Download ${message.file_name || message.file_path}`;
        bubble.appendChild(fileLink);
      }
    }

    const actions = buildMessageActions(message);
    if (actions) {
      bubble.appendChild(actions);
    }

    // Reactions area
    const reactionsArea = document.createElement('div');
    reactionsArea.className = 'reactions-area';
    reactionsArea.dataset.messageId = message.id;
    bubble.appendChild(reactionsArea);
    // Load reactions async
    loadReactionsForMessage(message.id, reactionsArea);
  }

  row.appendChild(bubble);
  return row;
}

function buildReplySnippet(message) {
  let snippet = '';
  if (message.message) {
    snippet = message.message;
  } else if (message.message_type === 'audio') {
    snippet = 'Voice note';
  } else if (message.message_type === 'image') {
    snippet = 'Image';
  } else if (message.file_name) {
    snippet = `File: ${message.file_name}`;
  } else {
    snippet = 'Attachment';
  }
  snippet = snippet.replace(/\s+/g, ' ').trim();
  if (snippet.length > 120) {
    snippet = `${snippet.slice(0, 117)}...`;
  }
  return snippet;
}

function startReply(message) {
  if (!message) {
    return;
  }
  cancelEdit();
  const sender = message.sender === state.username ? 'You' : resolveDisplayName(message.sender);
  const snippet = buildReplySnippet(message);
  const quote = `> ${sender}: ${snippet}\n`;
  const current = String(els.messageInput.value || '');
  els.messageInput.value = current ? `${current}\n\n${quote}` : quote;
  autoResizeComposer();
  saveCurrentDraft();
  els.messageInput.focus();
}

function createMessageActionButton(label, svgPath, className = '') {
  const button = document.createElement('button');
  button.type = 'button';
  button.className = `message-action-button${className ? ` ${className}` : ''}`;
  button.setAttribute('aria-label', label);
  button.setAttribute('title', label);
  button.innerHTML = `
    <svg class="icon" viewBox="0 0 24 24" aria-hidden="true">
      <path d="${svgPath}"></path>
    </svg>
    <span class="sr-only">${label}</span>
  `;
  return button;
}

function buildMessageActions(message) {
  if (!message || message.deleted_at) {
    return null;
  }

  const canEdit = canEditMessage(message);
  const canDelete = canDeleteMessage(message);

  const actions = document.createElement('div');
  actions.className = 'message-actions';

  const replyButton = createMessageActionButton(
    'Reply',
    'M10 9V5l-7 7 7 7v-4c4.4 0 7.5 1.4 9 4-1-4.5-4-9-9-10z'
  );
  replyButton.addEventListener('click', () => {
    startReply(message);
  });
  actions.appendChild(replyButton);

  // React button
  const reactButton = document.createElement('button');
  reactButton.type = 'button';
  reactButton.className = 'message-action-button';
  reactButton.setAttribute('aria-label', 'React');
  reactButton.setAttribute('title', 'React');
  reactButton.innerHTML = `<svg class="icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 2a10 10 0 1 0 10 10A10 10 0 0 0 12 2zm-3 9a1.5 1.5 0 1 1 1.5-1.5A1.5 1.5 0 0 1 9 11zm6 0a1.5 1.5 0 1 1 1.5-1.5A1.5 1.5 0 0 1 15 11zm-6.3 3a5.5 5.5 0 0 0 6.6 0l1.2 1.6a7.5 7.5 0 0 1-9 0z"/></svg><span class="sr-only">React</span>`;
  reactButton.addEventListener('click', (event) => {
    event.stopPropagation();
    showReactionPicker(message.id, reactButton);
  });
  actions.appendChild(reactButton);

  if (canEdit) {
    const editButton = createMessageActionButton(
      'Edit',
      'M3 17.3V21h3.7l10-10-3.7-3.7-10 10zM20.7 7a1 1 0 0 0 0-1.4l-2.3-2.3a1 1 0 0 0-1.4 0l-1.6 1.6 3.7 3.7z'
    );
    editButton.addEventListener('click', () => {
      startEditingMessage(message);
    });
    actions.appendChild(editButton);
  }

  if (canDelete) {
    const deleteButton = createMessageActionButton(
      'Delete',
      'M6 7h12l-1 13H7L6 7zm3-3h6l1 2H8l1-2z',
      'danger'
    );
    deleteButton.addEventListener('click', () => {
      deleteMessage(message.id).catch((err) => {
        setStatus(err.message || 'Failed to delete message.', true);
      });
    });
    actions.appendChild(deleteButton);
  }

  return actions;
}

function canEditMessage(message) {
  return (
    message.sender === state.username &&
    !message.deleted_at &&
    !message.file_path &&
    message.message_type === 'text'
  );
}

function canDeleteMessage(message) {
  if (message.sender === state.username) {
    return true;
  }

  if (!state.activeChat || state.activeChat.type !== 'group') {
    return false;
  }

  return Boolean(state.activeChat.isCreator || state.activeChat.isModerator);
}

function messageBelongsToActiveChat(message) {
  if (!state.activeChat) {
    return false;
  }

  if (state.activeChat.type === 'private') {
    return (
      (message.sender === state.username && message.receiver === state.activeChat.id) ||
      (message.sender === state.activeChat.id && message.receiver === state.username)
    );
  }

  return Number(message.group_id) === Number(state.activeChat.id);
}

function handleIncomingMessage(message) {
  if (!message) {
    return;
  }

  updateRecentFromMessage(message);

  if (messageBelongsToActiveChat(message)) {
    const shouldScroll = !state.scrollState.paused;
    appendMessage(message, shouldScroll);
    if (!shouldScroll) {
      showNewMessageBanner();
      if (message.sender !== state.username && !isChatMuted(chatKeyForActiveChat())) {
        playNotificationSound(message.group_id ? 'group' : 'direct');
      }
    }
    clearUnread(chatKeyForMessage(message));
    renderUsers();
    renderGroups();
    renderRecentList();
    return;
  }

  if (message.sender !== state.username) {
    incrementUnread(chatKeyForMessage(message));
    const label = message.group_id
      ? `# ${resolveGroupName(message.group_id)}`
      : resolveDisplayName(message.sender);
    if (!isChatMuted(chatKeyForMessage(message))) {
      setStatus(`New message in ${label}`);
      playNotificationSound(message.group_id ? 'group' : 'direct');
    }
    renderUsers();
    renderGroups();
    renderRecentList();
  }
}

function handleMessageUpdate(message) {
  if (!message) {
    return;
  }

  updateRecentFromMessage(message);

  if (state.editingMessageId && message.id === state.editingMessageId && message.deleted_at) {
    cancelEdit();
  }

  if (messageBelongsToActiveChat(message)) {
    updateMessageInView(message);
  }

  renderRecentList();
}

function updateMessageInView(message) {
  const row = document.getElementById(`message-${message.id}`);
  if (!row) {
    return;
  }

  const replacement = createMessageElement(message);
  if (!replacement) {
    return;
  }

  row.replaceWith(replacement);
}

function resolveDisplayName(username) {
  if (!username) {
    return '';
  }

  if (username === state.username) {
    return state.profile.displayName || username;
  }

  const match = state.users.find((user) => user.username === username);
  return match && match.display_name ? match.display_name : username;
}

function resolveGroupName(groupId) {
  const match = state.groups.find((group) => Number(group.id) === Number(groupId));
  return match ? match.name : `Group ${groupId}`;
}

function chatKeyForUser(username) {
  return `user:${username}`;
}

function chatKeyForGroup(groupId) {
  return `group:${groupId}`;
}

function chatKeyForActiveChat() {
  if (!state.activeChat) {
    return '';
  }
  return state.activeChat.type === 'group'
    ? chatKeyForGroup(state.activeChat.id)
    : chatKeyForUser(state.activeChat.id);
}

function chatKeyForMessage(message) {
  if (!message) {
    return '';
  }

  if (message.group_id) {
    return chatKeyForGroup(message.group_id);
  }

  const other =
    message.sender === state.username ? message.receiver : message.sender;
  return other ? chatKeyForUser(other) : '';
}

function saveCurrentDraft() {
  if (!state.activeChat) {
    return;
  }
  const key =
    state.activeChat.type === 'group'
      ? chatKeyForGroup(state.activeChat.id)
      : chatKeyForUser(state.activeChat.id);
  state.draftsByChat[key] = String(els.messageInput.value || '');
}

function restoreDraftForChat() {
  if (!state.activeChat) {
    return;
  }
  const key =
    state.activeChat.type === 'group'
      ? chatKeyForGroup(state.activeChat.id)
      : chatKeyForUser(state.activeChat.id);
  const draft = state.draftsByChat[key] || '';
  els.messageInput.value = draft;
  autoResizeComposer();
}

function clearDraftForActiveChat() {
  if (!state.activeChat) {
    return;
  }
  const key =
    state.activeChat.type === 'group'
      ? chatKeyForGroup(state.activeChat.id)
      : chatKeyForUser(state.activeChat.id);
  delete state.draftsByChat[key];
}

function isChatMuted(chatKey) {
  return Boolean(chatKey && state.mutedChats[chatKey]);
}

function updateMuteButton() {
  if (!els.muteChatButton) {
    return;
  }
  const key = chatKeyForActiveChat();
  if (!key) {
    els.muteChatButton.classList.add('hidden');
    return;
  }
  els.muteChatButton.classList.remove('hidden');
  const label = isChatMuted(key) ? 'Unmute' : 'Mute';
  els.muteChatButton.setAttribute('aria-label', label);
  els.muteChatButton.setAttribute('title', label);
  const text = els.muteChatButton.querySelector('.sr-only');
  if (text) {
    text.textContent = label;
  }
}

function toggleMuteForActiveChat() {
  const key = chatKeyForActiveChat();
  if (!key) {
    return;
  }
  state.mutedChats[key] = !state.mutedChats[key];
  updateMuteButton();
  setStatus(state.mutedChats[key] ? 'Chat muted.' : 'Chat unmuted.');
}

function markActiveChatRead() {
  const key = chatKeyForActiveChat();
  if (!key) {
    return;
  }
  clearUnread(key);
  renderUsers();
  renderGroups();
  renderRecentList();
  setStatus('Marked as read.');
}

function incrementUnread(chatKey) {
  if (!chatKey) {
    return;
  }

  state.unreadByChat[chatKey] = (state.unreadByChat[chatKey] || 0) + 1;
}

function clearUnread(chatKey) {
  if (!chatKey) {
    return;
  }

  delete state.unreadByChat[chatKey];
}

function getUnreadCount(chatKey) {
  return state.unreadByChat[chatKey] || 0;
}

function updateRecentFromMessage(message) {
  if (!message) {
    return;
  }

  const chatKey = chatKeyForMessage(message);
  if (!chatKey) {
    return;
  }

  const isGroup = Boolean(message.group_id);
  const chatId = isGroup
    ? Number(message.group_id)
    : message.sender === state.username
      ? message.receiver
      : message.sender;

  const title = isGroup
    ? `# ${resolveGroupName(chatId)}`
    : resolveDisplayName(chatId);

  let preview = '';
  if (message.deleted_at) {
    preview = 'Message deleted';
  } else if (message.message) {
    preview = message.message;
  } else if (message.message_type === 'audio') {
    preview = 'Voice note';
  } else if (message.message_type === 'image') {
    preview = 'Image';
  } else if (message.file_name) {
    preview = `File: ${message.file_name}`;
  } else {
    preview = 'Attachment';
  }

  if (isGroup && message.sender && message.sender !== state.username) {
    preview = `${resolveDisplayName(message.sender)}: ${preview}`;
  }

  let timestamp = message.timestamp ? new Date(message.timestamp).getTime() : Date.now();
  if (Number.isNaN(timestamp)) {
    timestamp = Date.now();
  }
  state.recentChats[chatKey] = {
    key: chatKey,
    type: isGroup ? 'group' : 'private',
    id: chatId,
    title,
    preview,
    timestamp
  };
  saveRecentChats();
}

function renderRecentList() {
  if (!els.recentList) {
    return;
  }

  els.recentList.innerHTML = '';
  const entries = Object.values(state.recentChats)
    .sort((a, b) => b.timestamp - a.timestamp)
    .slice(0, 8);

  if (!entries.length) {
    els.recentList.appendChild(createEmptyItem('No recent chats.'));
    return;
  }

  entries.forEach((entry) => {
    const item = document.createElement('li');
    item.className = 'list-item';

    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'chat-target';
    const displayTitle =
      entry.type === 'group'
        ? resolveGroupName(entry.id)
        : resolveDisplayName(entry.id);
    const avatarPath =
      entry.type === 'private'
        ? (state.users.find((user) => user.username === entry.id) || {}).avatar_path || ''
        : '';
    const avatar = createAvatarElement(displayTitle, avatarPath);

    const label = document.createElement('div');
    label.className = 'chat-label';

    const nameLine = document.createElement('span');
    nameLine.className = 'chat-name';
    nameLine.textContent = entry.type === 'group' ? `# ${displayTitle}` : displayTitle;

    const metaLine = document.createElement('span');
    metaLine.className = 'chat-meta';
    metaLine.textContent = entry.preview;

    label.appendChild(nameLine);
    label.appendChild(metaLine);
    button.appendChild(avatar);
    button.appendChild(label);

    if (state.activeChat) {
      const isActive =
        (entry.type === 'group' &&
          state.activeChat.type === 'group' &&
          Number(state.activeChat.id) === Number(entry.id)) ||
        (entry.type === 'private' &&
          state.activeChat.type === 'private' &&
          state.activeChat.id === entry.id);
      if (isActive) {
        button.classList.add('active');
      }
    }

    const unread = getUnreadCount(entry.key);
    if (unread > 0) {
      const badge = document.createElement('span');
      badge.className = 'unread-badge';
      badge.textContent = String(unread);
      button.appendChild(badge);
    }

    button.addEventListener('click', () => {
      if (entry.type === 'group') {
        openGroupChat(entry.id, resolveGroupName(entry.id));
      } else {
        openPrivateChat(entry.id);
      }
    });

    item.appendChild(button);
    els.recentList.appendChild(item);
  });
}

async function performSearch() {
  const query = String(els.searchInput.value || '').trim();
  if (query.length < 2) {
    setStatus('Search needs at least 2 characters.', true);
    return;
  }

  try {
    state.searchMeta.query = query;
    state.searchMeta.oldestId = 0;
    state.searchMeta.hasMore = false;
    state.searchMeta.loading = false;
    await fetchSearchResults(false);
  } catch (err) {
    setStatus(err.message || 'Search failed.', true);
  }
}

async function fetchSearchResults(append) {
  if (state.searchMeta.loading) {
    return;
  }
  if (append && !state.searchMeta.hasMore) {
    return;
  }
  state.searchMeta.loading = true;
  try {
    const params = new URLSearchParams();
    params.set('q', state.searchMeta.query);
    params.set('limit', String(state.searchMeta.pageSize));
    if (append && state.searchMeta.oldestId) {
      params.set('beforeId', String(state.searchMeta.oldestId));
    }
    const results = await api(`/search?${params.toString()}`);
    if (append) {
      state.searchResults = state.searchResults.concat(results || []);
    } else {
      state.searchResults = results || [];
    }
    if (results && results.length) {
      state.searchMeta.oldestId = results[results.length - 1].id;
    }
    state.searchMeta.hasMore =
      Array.isArray(results) && results.length === state.searchMeta.pageSize;
    renderSearchResults();
  } finally {
    state.searchMeta.loading = false;
  }
}

function applyHighlight(element, text, query) {
  element.innerHTML = '';
  if (!query) {
    element.textContent = text;
    return;
  }
  const lower = text.toLowerCase();
  const needle = query.toLowerCase();
  let start = 0;
  while (start < text.length) {
    const index = lower.indexOf(needle, start);
    if (index === -1) {
      element.appendChild(document.createTextNode(text.slice(start)));
      break;
    }
    if (index > start) {
      element.appendChild(document.createTextNode(text.slice(start, index)));
    }
    const mark = document.createElement('span');
    mark.className = 'search-highlight';
    mark.textContent = text.slice(index, index + needle.length);
    element.appendChild(mark);
    start = index + needle.length;
  }
}

async function jumpToMessageFromSearch(result) {
  state.highlightMessageId = result.id;
  if (result.group_id) {
    await openGroupChat(result.group_id, result.group_name || resolveGroupName(result.group_id));
  } else {
    const other =
      result.sender === state.username ? result.receiver : result.sender;
    await openPrivateChat(other);
  }
  clearSearchResults();
  await ensureMessageInView(result.id);
}

async function ensureMessageInView(messageId) {
  if (!messageId) {
    return;
  }
  for (let i = 0; i < 6; i += 1) {
    const row = document.getElementById(`message-${messageId}`);
    if (row) {
      highlightMessageInView(messageId);
      return;
    }
    if (!state.historyMeta.hasMore) {
      break;
    }
    await loadHistory({ older: true });
  }
  highlightMessageInView(messageId);
}

function renderSearchResults() {
  els.searchResults.innerHTML = '';
  els.searchPanel.classList.remove('hidden');

  if (!state.searchResults.length) {
    const empty = document.createElement('li');
    empty.className = 'empty-item';
    empty.textContent = 'No results found.';
    els.searchResults.appendChild(empty);
    if (els.searchMoreButton) {
      els.searchMoreButton.classList.add('hidden');
    }
    return;
  }

  state.searchResults.forEach((result) => {
    const item = document.createElement('li');
    item.className = 'search-result';

    const isGroup = Boolean(result.group_id);
    const chatLabel = isGroup
      ? `# ${result.group_name || resolveGroupName(result.group_id)}`
      : resolveDisplayName(
          result.sender === state.username ? result.receiver : result.sender
        );

    const title = document.createElement('div');
    title.className = 'search-result-title';
    applyHighlight(
      title,
      `${chatLabel} • ${resolveDisplayName(result.sender)}`,
      state.searchMeta.query
    );

    const snippet = document.createElement('div');
    snippet.className = 'search-result-snippet';
    applyHighlight(
      snippet,
      result.message ||
        result.file_name ||
        (result.message_type === 'audio'
          ? 'Voice note'
          : result.message_type === 'image'
            ? 'Image'
            : 'Attachment'),
      state.searchMeta.query
    );

    item.appendChild(title);
    item.appendChild(snippet);

    item.addEventListener('click', () => {
      jumpToMessageFromSearch(result).catch((err) => {
        setStatus(err.message || 'Failed to jump to message.', true);
      });
    });

    els.searchResults.appendChild(item);
  });

  if (els.searchMoreButton) {
    els.searchMoreButton.classList.toggle('hidden', !state.searchMeta.hasMore);
  }
}

function clearSearchResults() {
  state.searchResults = [];
  els.searchResults.innerHTML = '';
  els.searchPanel.classList.add('hidden');
  if (els.searchMoreButton) {
    els.searchMoreButton.classList.add('hidden');
  }
  state.searchMeta.query = '';
  state.searchMeta.oldestId = 0;
  state.searchMeta.hasMore = false;
  state.searchMeta.loading = false;
}

async function sendTextMessage(event) {
  event.preventDefault();

  const message = String(els.messageInput.value || '').trim();
  if (!state.activeChat) {
    setStatus('Select a user or group first.', true);
    return;
  }

  try {
    if (state.editingMessageId) {
      await sendEditMessage(message);
      return;
    }

    if (!message) {
      return;
    }

    await sendSocketMessage({ message });
    clearDraftForActiveChat();
    els.messageInput.value = '';
    autoResizeComposer();
    sendTyping(false);
  } catch (err) {
    setStatus(err.message || 'Failed to send message.', true);
  }
}

async function sendSelectedFile() {
  const file = els.fileInput.files[0];
  if (!file) {
    return;
  }

  try {
    await sendFile(file);
  } catch (err) {
    setStatus(err.message || 'Failed to upload file.', true);
  } finally {
    els.fileInput.value = '';
  }
}

async function sendFile(file) {
  if (!state.activeChat) {
    setStatus('Select a user or group first.', true);
    return;
  }

  const uploadResult = await uploadFile(file);
  let messageType = 'file';

  if (uploadResult.mimeType && uploadResult.mimeType.startsWith('audio/')) {
    messageType = 'audio';
  } else if (uploadResult.mimeType && uploadResult.mimeType.startsWith('image/')) {
    messageType = 'image';
  }

  await sendSocketMessage({
    filePath: uploadResult.filePath,
    fileName: uploadResult.fileName,
    messageType
  });
}

function handleDragOver(event) {
  if (!event.dataTransfer || !event.dataTransfer.types.includes('Files')) {
    return;
  }

  event.preventDefault();

  if (event.type === 'dragenter') {
    state.dragDepth += 1;
  }

  els.dropOverlay.classList.remove('hidden');
  if (els.chatPanel) {
    els.chatPanel.classList.add('is-dragging');
  }
}

function handleDragLeave(event) {
  if (!event.dataTransfer) {
    return;
  }

  event.preventDefault();
  state.dragDepth = Math.max(0, state.dragDepth - 1);

  if (state.dragDepth === 0) {
    els.dropOverlay.classList.add('hidden');
    if (els.chatPanel) {
      els.chatPanel.classList.remove('is-dragging');
    }
  }
}

function handleDrop(event) {
  if (!event.dataTransfer) {
    return;
  }

  event.preventDefault();
  state.dragDepth = 0;
  els.dropOverlay.classList.add('hidden');
  if (els.chatPanel) {
    els.chatPanel.classList.remove('is-dragging');
  }

  const file = event.dataTransfer.files && event.dataTransfer.files[0];
  if (!file) {
    return;
  }

  sendFile(file).catch((err) => {
    setStatus(err.message || 'Failed to upload file.', true);
  });
}

async function toggleVoiceRecording() {
  if (state.mediaRecorder && state.mediaRecorder.state === 'recording') {
    state.mediaRecorder.stop();
    return;
  }

  if (!state.activeChat) {
    setStatus('Select a user or group first.', true);
    return;
  }

  if (!navigator.mediaDevices || typeof MediaRecorder === 'undefined') {
    setStatus('Voice recording is not supported in this browser.', true);
    return;
  }

  try {
    state.mediaStream = await navigator.mediaDevices.getUserMedia({ audio: true });
    state.mediaChunks = [];

    // Ensure AudioContext exists for the visualizer
    if (!state.soundContext) {
      const Context = window.AudioContext || window.webkitAudioContext;
      if (Context) state.soundContext = new Context();
    }

    state.mediaRecorder = new MediaRecorder(state.mediaStream);
    state.mediaRecorder.ondataavailable = (event) => {
      if (event.data.size > 0) {
        state.mediaChunks.push(event.data);
      }
    };

    state.mediaRecorder.onstop = async () => {
      setRecordingState(false);
      stopMediaStream();

      if (!state.mediaChunks.length) {
        return;
      }

      try {
        const blob = new Blob(state.mediaChunks, { type: 'audio/webm' });
        const voiceFile = new File([blob], `voice-${Date.now()}.webm`, {
          type: 'audio/webm'
        });

        const uploadResult = await uploadFile(voiceFile);
        await sendSocketMessage({
          filePath: uploadResult.filePath,
          fileName: uploadResult.fileName,
          messageType: 'audio'
        });
      } catch (err) {
        setStatus(err.message || 'Failed to send audio message.', true);
      } finally {
        state.mediaChunks = [];
      }
    };

    state.mediaRecorder.start();
    setRecordingState(true);
    setStatus('Recording voice note...');
  } catch (_err) {
    setStatus('Microphone access denied or unavailable.', true);
    stopMediaStream();
    setRecordingState(false);
  }
}

function setRecordingState(recording) {
  const iconHtml = recording
    ? `<svg class="icon" viewBox="0 0 24 24" aria-hidden="true">
         <rect x="7" y="7" width="10" height="10" rx="2"></rect>
       </svg>
       <span class="sr-only">Stop</span>`
    : `<svg class="icon" viewBox="0 0 24 24" aria-hidden="true">
         <path d="M12 14a3 3 0 0 0 3-3V7a3 3 0 0 0-6 0v4a3 3 0 0 0 3 3zm5-3a5 5 0 0 1-10 0H5a7 7 0 0 0 6 6.9V21h2v-3.1A7 7 0 0 0 19 11z"/>
       </svg>
       <span class="sr-only">Record</span>`;
  if (recording) {
    els.recordButton.classList.add('recording');
    els.recordButton.setAttribute('aria-label', 'Stop');
    els.recordButton.setAttribute('title', 'Stop');
    els.recordButton.innerHTML = iconHtml;
    // Swap textarea for visualizer
    els.messageInput.classList.add('hidden');
    if (els.voiceVisualizer) els.voiceVisualizer.classList.remove('hidden');
    startVisualizer();
    startRecordingTimer();
  } else {
    els.recordButton.classList.remove('recording');
    els.recordButton.setAttribute('aria-label', 'Record');
    els.recordButton.setAttribute('title', 'Record');
    els.recordButton.innerHTML = iconHtml;
    // Restore textarea
    els.messageInput.classList.remove('hidden');
    if (els.voiceVisualizer) els.voiceVisualizer.classList.add('hidden');
    stopVisualizer();
    stopRecordingTimer();
  }
}

function startVisualizer() {
  const bars = els.voiceVisualizer
    ? Array.from(els.voiceVisualizer.querySelectorAll('.vv-bar'))
    : [];
  if (!bars.length) return;

  // Try to use real audio data if analyser is available
  let analyser = null;
  let dataArray = null;
  if (state.soundContext && state.mediaStream) {
    try {
      analyser = state.soundContext.createAnalyser();
      analyser.fftSize = 64;
      const source = state.soundContext.createMediaStreamSource(state.mediaStream);
      source.connect(analyser);
      dataArray = new Uint8Array(analyser.frequencyBinCount);
    } catch (_e) {
      analyser = null;
    }
  }

  function frame() {
    state.visualizerRaf = requestAnimationFrame(frame);
    if (analyser && dataArray) {
      analyser.getByteFrequencyData(dataArray);
      bars.forEach((bar, i) => {
        const idx = Math.floor((i / bars.length) * dataArray.length);
        const val = dataArray[idx] / 255;
        const h = Math.max(4, Math.round(val * 36));
        bar.style.height = `${h}px`;
      });
    } else {
      // Fallback: animated sine wave
      const t = Date.now() / 180;
      bars.forEach((bar, i) => {
        const wave = Math.sin(t + i * 0.55) * 0.5 + 0.5;
        const h = Math.max(4, Math.round(wave * 32));
        bar.style.height = `${h}px`;
      });
    }
  }
  frame();
}

function stopVisualizer() {
  if (state.visualizerRaf) {
    cancelAnimationFrame(state.visualizerRaf);
    state.visualizerRaf = 0;
  }
  // Reset bar heights
  if (els.voiceVisualizer) {
    els.voiceVisualizer.querySelectorAll('.vv-bar').forEach((bar) => {
      bar.style.height = '4px';
    });
  }
}

function startRecordingTimer() {
  state.recordingSeconds = 0;
  if (els.voiceTimer) els.voiceTimer.textContent = '0:00';
  state.recordingTimerId = window.setInterval(() => {
    state.recordingSeconds += 1;
    const m = Math.floor(state.recordingSeconds / 60);
    const s = String(state.recordingSeconds % 60).padStart(2, '0');
    if (els.voiceTimer) els.voiceTimer.textContent = `${m}:${s}`;
  }, 1000);
}

function stopRecordingTimer() {
  window.clearInterval(state.recordingTimerId);
  state.recordingTimerId = 0;
  state.recordingSeconds = 0;
  if (els.voiceTimer) els.voiceTimer.textContent = '0:00';
}

function stopMediaStream() {
  if (!state.mediaStream) {
    return;
  }

  state.mediaStream.getTracks().forEach((track) => track.stop());
  state.mediaStream = null;
}

async function uploadFile(file) {
  const formData = new FormData();
  formData.append('file', file);

  return api('/upload', {
    method: 'POST',
    body: formData
  });
}

function emitWithAck(eventName, payload) {
  if (!state.socket || !state.socket.connected) {
    return Promise.reject(new Error('Realtime connection is not ready.'));
  }

  return new Promise((resolve, reject) => {
    let settled = false;
    const timeoutId = window.setTimeout(() => {
      if (settled) {
        return;
      }
      settled = true;
      reject(new Error('Server response timeout.'));
    }, 5000);

    state.socket.emit(eventName, payload, (result) => {
      if (settled) {
        return;
      }

      settled = true;
      window.clearTimeout(timeoutId);

      if (result && result.ok) {
        resolve(result);
      } else {
        reject(new Error((result && result.error) || 'Request failed.'));
      }
    });
  });
}

async function sendSocketMessage(payload) {
  const eventName =
    state.activeChat.type === 'private' ? 'chat:private' : 'chat:group';

  const eventPayload =
    state.activeChat.type === 'private'
      ? { to: state.activeChat.id, ...payload }
      : { groupId: state.activeChat.id, ...payload };

  return emitWithAck(eventName, eventPayload);
}

async function sendEditMessage(message) {
  const trimmed = String(message || '').trim();
  if (!trimmed) {
    setStatus('Edited message cannot be empty.', true);
    return;
  }

  if (!state.editingMessageId) {
    return;
  }

  await emitWithAck('chat:edit', {
    id: state.editingMessageId,
    message: trimmed
  });

  cancelEdit();
}

async function deleteMessage(messageId) {
  await emitWithAck('chat:delete', { id: messageId });
}

function startEditingMessage(message) {
  state.editingMessageId = message.id;
  els.messageInput.value = message.message || '';
  els.messageInput.focus();
  autoResizeComposer();
  els.sendButton.textContent = 'Save';
  els.cancelEditButton.classList.remove('hidden');
}

function cancelEdit() {
  if (!state.editingMessageId && els.cancelEditButton.classList.contains('hidden')) {
    return;
  }

  state.editingMessageId = 0;
  els.messageInput.value = '';
  autoResizeComposer();
  els.sendButton.textContent = 'Send';
  els.cancelEditButton.classList.add('hidden');
}

function handleComposerInput() {
  autoResizeComposer();
  saveCurrentDraft();

  if (!state.activeChat || !state.socket || !state.socket.connected) {
    return;
  }

  sendTyping(true);
  window.clearTimeout(state.typingTimerId);
  state.typingTimerId = window.setTimeout(() => {
    sendTyping(false);
  }, 800);
}

function sendTyping(isTyping) {
  if (!state.activeChat || !state.socket || !state.socket.connected) {
    return;
  }

  const payload =
    state.activeChat.type === 'private'
      ? { to: state.activeChat.id, isTyping }
      : { groupId: state.activeChat.id, isTyping };

  state.socket.emit('chat:typing', payload);
}

function handleTypingNotification(payload) {
  if (!payload || !state.activeChat || payload.from === state.username) {
    return;
  }

  const inPrivateChat =
    state.activeChat.type === 'private' &&
    payload.to === state.username &&
    payload.from === state.activeChat.id;

  const inGroupChat =
    state.activeChat.type === 'group' &&
    Number(payload.groupId) === Number(state.activeChat.id);

  if (!inPrivateChat && !inGroupChat) {
    return;
  }

  if (!payload.isTyping) {
    window.clearTimeout(state.typingShowTimerId);
    els.typingIndicator.textContent = '';
    return;
  }

  window.clearTimeout(state.typingShowTimerId);
  state.typingShowTimerId = window.setTimeout(() => {
    els.typingIndicator.textContent = `${resolveDisplayName(payload.from)} is typing...`;
  }, 150);
  window.clearTimeout(state.typingHideTimerId);
  state.typingHideTimerId = window.setTimeout(() => {
    els.typingIndicator.textContent = '';
  }, 1200);
}

function autoResizeComposer() {
  els.messageInput.style.height = 'auto';
  els.messageInput.style.height = `${Math.min(els.messageInput.scrollHeight, 140)}px`;
}

async function logout() {
  try {
    await api('/auth/logout', { method: 'POST' });
  } catch (_err) {
    // Ignore logout failures.
  }

  closeCommandPalette();

  if (state.socket) {
    state.socket.disconnect();
    state.socket = null;
  }

  if (state.mediaRecorder && state.mediaRecorder.state === 'recording') {
    state.mediaRecorder.stop();
  }

  stopMediaStream();

  state.token = '';
  state.username = '';
  state.users = [];
  state.groups = [];
  state.groupMembers = [];
  state.activeChat = null;
  state.showGroupPanel = false;
  state.unreadByChat = {};
  state.searchResults = [];
  state.searchMeta.query = '';
  state.searchMeta.oldestId = 0;
  state.searchMeta.hasMore = false;
  state.searchMeta.loading = false;
  state.editingMessageId = 0;
  state.highlightMessageId = 0;
  state.draftsByChat = {};
  state.mutedChats = {};
  resetHistoryMeta();
  state.profile = {
    displayName: '',
    statusMessage: '',
    avatarPath: ''
  };
  state.mediaRecorder = null;
  state.mediaChunks = [];

  els.userList.innerHTML = '';
  els.groupList.innerHTML = '';
  els.memberList.innerHTML = '';
  els.addMemberInput.value = '';
  els.renameGroupInput.value = '';
  els.transferGroupInput.value = '';
  els.inviteCodeDisplay.value = '';
  els.messageList.innerHTML = '';
  els.messageList.classList.remove('is-empty');
  els.searchResults.innerHTML = '';
  els.searchPanel.classList.add('hidden');
  els.searchInput.value = '';
  els.joinCodeInput.value = '';
  els.authPassword.value = '';
  els.authUsername.value = '';
  els.messageInput.value = '';
  els.profileDisplayName.value = '';
  els.profileStatusMessage.value = '';
  els.profileNameDisplay.textContent = '';
  els.profileStatusDisplay.textContent = '';
  setAvatarElement(els.profileAvatar, '', '');
  els.groupVisibilitySelect.value = 'public';
  autoResizeComposer();
  leaveChatView();
  setStatus('Logged out.');
}

function setStatus(message, isError) {
  const text = String(message || '');
  const error = Boolean(isError);

  els.statusBar.textContent = text;
  els.statusBar.classList.toggle('error', error);

  els.appStatus.textContent = text;
  els.appStatus.classList.toggle('error', error);

  window.clearTimeout(state.statusTimerId);
  if (text) {
    state.statusTimerId = window.setTimeout(() => {
      els.appStatus.textContent = '';
      els.appStatus.classList.remove('error');
    }, 3500);
  }
}

function formatTime(value) {
  const date = value ? new Date(value) : new Date();
  if (Number.isNaN(date.getTime())) {
    return '';
  }

  return date.toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit'
  });
}

function isNearBottom() {
  if (!els.messageList) {
    return true;
  }
  const distance =
    els.messageList.scrollHeight -
    (els.messageList.scrollTop + els.messageList.clientHeight);
  return distance <= 40;
}

function showNewMessageBanner() {
  if (!els.newMessageBanner) {
    return;
  }
  els.newMessageBanner.classList.remove('hidden');
}

function hideNewMessageBanner() {
  if (!els.newMessageBanner) {
    return;
  }
  els.newMessageBanner.classList.add('hidden');
}

function handleMessageListScroll() {
  if (!els.messageList) {
    return;
  }

  const nearBottom = isNearBottom();
  if (nearBottom) {
    state.scrollState.paused = false;
    hideNewMessageBanner();
  } else {
    state.scrollState.paused = true;
  }

  if (!state.activeChat || state.historyMeta.loading || !state.historyMeta.hasMore) {
    return;
  }
  if (els.messageList.scrollTop <= 40) {
    loadHistory({ older: true }).catch((err) => {
      setStatus(err.message || 'Failed to load older messages.', true);
    });
  }
}

function scrollMessagesToBottom() {
  els.messageList.scrollTop = els.messageList.scrollHeight;
  state.scrollState.paused = false;
  hideNewMessageBanner();
}

// --- Seen receipts ---
function handleSeenNotification(payload) {
  if (!payload || !state.activeChat) return;
  if (state.activeChat.type !== 'private') return;
  if (payload.from !== state.activeChat.id) return;
  // Update all outgoing ticks in view
  document.querySelectorAll('.seen-tick').forEach((tick) => {
    tick.textContent = ' ✓✓';
    tick.title = 'Seen';
  });
}

// --- Edit history ---
async function showEditHistory(messageId, anchor) {
  // Toggle: remove existing tooltip if present
  const existing = anchor.parentElement && anchor.parentElement.querySelector('.edit-history-tooltip');
  if (existing) { existing.remove(); return; }
  try {
    const data = await api(`/messages/${messageId}/history`);
    if (!data.original_message) return;
    const tooltip = document.createElement('span');
    tooltip.className = 'edit-history-tooltip';
    tooltip.textContent = `Original: ${data.original_message}`;
    anchor.parentElement.appendChild(tooltip);
    setTimeout(() => tooltip.remove(), 4000);
  } catch (_err) { /* ignore */ }
}

// --- Reactions ---
async function loadReactionsForMessage(messageId, container) {
  try {
    const reactions = await api(`/messages/${messageId}/reactions`);
    renderReactionChips(messageId, reactions, container);
  } catch (_err) { /* ignore */ }
}

function renderReactionChips(messageId, reactions, container) {
  if (!container) return;
  container.innerHTML = '';
  if (!reactions || !reactions.length) return;
  reactions.forEach((r) => {
    const chip = document.createElement('button');
    chip.type = 'button';
    chip.className = `reaction-chip${r.reacted_by_me ? ' reacted' : ''}`;
    chip.textContent = `${r.emoji} ${r.count}`;
    chip.title = r.reacted_by_me ? 'Remove reaction' : 'Add reaction';
    chip.addEventListener('click', () => toggleReaction(messageId, r.emoji));
    container.appendChild(chip);
  });
}

async function toggleReaction(messageId, emoji) {
  try {
    await api(`/messages/${messageId}/react`, {
      method: 'POST',
      body: JSON.stringify({ emoji })
    });
  } catch (err) {
    setStatus(err.message || 'Failed to react.', true);
  }
}

function showReactionPicker(messageId, anchor) {
  document.querySelectorAll('.reaction-picker').forEach((p) => p.remove());
  const picker = document.createElement('div');
  picker.className = 'reaction-picker';
  EMOJIS.forEach((emoji) => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'emoji-option';
    btn.textContent = emoji;
    btn.addEventListener('click', () => {
      picker.remove();
      toggleReaction(messageId, emoji);
    });
    picker.appendChild(btn);
  });
  anchor.parentElement.appendChild(picker);
  setTimeout(() => { if (picker.parentElement) picker.remove(); }, 5000);
}

function handleReactionUpdate(payload) {
  if (!payload || !payload.messageId) return;
  const area = document.querySelector(`.reactions-area[data-message-id="${payload.messageId}"]`);
  if (!area) return;
  loadReactionsForMessage(payload.messageId, area);
}

// --- Account deletion ---
async function deleteAccount() {
  if (!window.confirm('Permanently delete your account? This cannot be undone.')) return;
  try {
    await api('/auth/account', { method: 'DELETE' });
    closeProfileModal();
    logout();
  } catch (err) {
    setStatus(err.message || 'Failed to delete account.', true);
  }
}

async function api(path, options = {}) {
  const config = {
    method: options.method || 'GET',
    headers: options.headers ? { ...options.headers } : {},
    body: options.body
  };

  if (!options.skipAuth && state.token) {
    config.headers.Authorization = `Bearer ${state.token}`;
  }

  if (
    config.body &&
    !(config.body instanceof FormData) &&
    !Object.prototype.hasOwnProperty.call(config.headers, 'Content-Type')
  ) {
    config.headers['Content-Type'] = 'application/json';
  }

  const response = await fetch(path, config);
  const rawText = await response.text();

  let payload = null;
  if (rawText) {
    try {
      payload = JSON.parse(rawText);
    } catch (_err) {
      payload = rawText;
    }
  }

  if (!response.ok) {
    const message =
      payload && typeof payload === 'object' && payload.error
        ? payload.error
        : `Request failed (${response.status})`;
    throw new Error(message);
  }

  return payload;
}
