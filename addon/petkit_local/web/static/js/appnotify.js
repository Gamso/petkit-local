import { api, esc, toast } from './core.js';
import { onAction } from './delegate.js';
import { help } from './help.js';

// ---------------- Setup → Phone notifications ----------------
// Push notifications through the owner's own ntfy server (`app_api/notify/` on
// the server). The access token is sent when typed and never read back: the
// server only returns it masked, so the field below starts empty and is only
// sent when something was typed into it.
const jsonPost = (path, body) =>
  api(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

function render(n) {
  const v = document.getElementById('appNotifyView');
  if (!v) return;
  if (n.error) {
    v.innerHTML = `<h3>Phone notifications</h3><p class="sub mut">${esc(n.error)}</p>`;
    return;
  }
  const kinds = n.kinds || [];
  v.innerHTML = `<h3>Phone notifications${help(
    'Sent through your own ntfy server. On the phone, the ntfy app (or an app using it as its UnifiedPush distributor) subscribes to the topic below. The topic is effectively a password: anyone who knows it and may read it gets every notification.',
  )} ${n.enabled ? '<span class="badge ok">on</span>' : '<span class="badge">off</span>'}</h3>
    <p class="sub">Only the ntfy server below is ever contacted. Each kind is sent at most a few times per device and hour, so a flapping device cannot flood the phone.</p>
    <label class="mut" style="display:block;margin-top:6px">ntfy server</label>
    <input id="ntfyServer" value="${esc(n.server)}" placeholder="https://ntfy.example.org" style="min-width:280px">
    ${n.insecure ? '<span class="badge warn">plain HTTP — the token travels in the clear</span>' : ''}
    <label class="mut" style="display:block;margin-top:8px">Access token${help(
      'A token of an ntfy user allowed to WRITE to the topic (ntfy token add <user>). Stored in /data/ntfy.json, never shown again in full and never written to the log. Leave empty to keep the current one.',
    )}</label>
    <span class="cn" style="display:flex;gap:6px;flex-wrap:wrap">
      <input id="ntfyToken" type="password" autocomplete="off" placeholder="${n.token_set ? esc(n.token_masked) + ' (set — type to replace)' : 'tk_…'}" style="min-width:240px">
      ${n.token_set ? '<button class="mini" data-action="ntfy-clear-token">Remove token</button>' : ''}
    </span>
    <label class="mut" style="display:block;margin-top:8px">Topic${help(
      'Generated at random when notifications are first enabled. Regenerating it stops delivery to every phone still subscribed to the old one. You may also paste a topic of your own, for example the one a UnifiedPush endpoint names.',
    )}</label>
    <span class="cn" style="display:flex;gap:6px;flex-wrap:wrap">
      <input id="ntfyTopic" value="${esc(n.topic)}" placeholder="generated when enabled" style="min-width:280px">
      ${n.topic ? '<button class="mini" data-action="ntfy-regenerate-topic">Regenerate</button>' : ''}
    </span>
    <div style="margin-top:10px;display:flex;flex-wrap:wrap;gap:4px 14px">
      ${kinds
        .map(
          k =>
            `<label><input type="checkbox" class="ntfyKind" data-kind="${esc(k.kind)}" ${k.enabled ? 'checked' : ''}> ${esc(k.label)}</label>`,
        )
        .join('')}
    </div>
    <p class="sub mut" style="margin-top:4px">These switches also apply to the phone app’s own notifications — over its own connection or a push endpoint it registered (see Phone app access) — with or without the ntfy topic.</p>
    <div style="margin-top:10px;display:flex;gap:6px;flex-wrap:wrap">
      <button class="act" data-action="ntfy-save" data-enable="1">${n.enabled ? 'Save' : 'Save and enable'}</button>
      <button class="mini" data-action="ntfy-save" data-enable="0">${n.enabled ? 'Disable' : 'Save without enabling'}</button>
      <button class="mini" data-action="ntfy-test">Send test notification</button>
    </div>`;
}

async function loadAppNotify() {
  render(await api('app-notify'));
}

onAction('ntfy-save', async el => {
  const body = {
    enabled: el.dataset.enable === '1',
    server: document.getElementById('ntfyServer').value.trim(),
    kinds: {},
  };
  const topic = document.getElementById('ntfyTopic').value.trim();
  if (topic) body.topic = topic;
  const token = document.getElementById('ntfyToken').value.trim();
  if (token) body.token = token;
  document.querySelectorAll('.ntfyKind').forEach(c => (body.kinds[c.dataset.kind] = c.checked));
  const r = await jsonPost('app-notify', body);
  if (r.error) return toast('Error: ' + r.error);
  toast(r.enabled ? 'Notifications on' : 'Notifications off');
  render(r);
});
onAction('ntfy-clear-token', async () => {
  const r = await jsonPost('app-notify', { token: '' });
  if (r.error) return toast('Error: ' + r.error);
  toast('Token removed');
  render(r);
});
onAction('ntfy-regenerate-topic', async () => {
  if (!confirm('Generate a new topic? Phones subscribed to the old one stop receiving.')) return;
  const r = await jsonPost('app-notify/topic', {});
  if (r.error) return toast('Error: ' + r.error);
  toast('New topic generated — subscribe the phone to it');
  render(r);
});
onAction('ntfy-test', async () => {
  const r = await jsonPost('app-notify/test', {});
  toast(r.ok ? 'Test notification sent' : 'Not sent: ' + (r.error || 'unknown error'));
});

export { loadAppNotify };
