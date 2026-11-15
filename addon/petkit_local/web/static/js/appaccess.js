import { api, esc, relTime, toast } from './core.js';
import { onAction } from './delegate.js';
import { help } from './help.js';

// ---------------- Setup → Phone app access ----------------
// The app API (its own port, `app_api/` on the server) is administered from
// here and only here: its password, the phones signed in with it, and the
// reverse proxies whose forwarded addresses it believes. The password is sent
// once and never read back — the server only says whether one is set.
const jsonPost = (path, body, method = 'POST') =>
  api(path, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

function render(a) {
  const v = document.getElementById('appAccessView');
  if (!v) return;
  if (a.error) {
    v.innerHTML = `<h3>Phone app access</h3><p class="sub mut">${esc(a.error)}</p>`;
    return;
  }
  const sessions = a.sessions || [];
  v.innerHTML = `<h3>Phone app access${help(
    'The phone app reaches petkit-local through its own port, never through this panel. Map that port in the app’s Network settings and point your reverse proxy or tunnel at it. Every request is checked against the password set here; with none set, the app API refuses everything.',
  )} ${a.password_set ? '<span class="badge ok">open to the app</span>' : '<span class="badge">closed — no password</span>'}</h3>
    <p class="sub">Port <code>${esc(a.app_port ?? '?')}</code>. Expose it only through a reverse proxy or tunnel — never this panel, never port 80. Setting, resetting or removing the password signs out every phone.</p>
    <span class="cn" style="display:flex;gap:6px;margin-top:6px;flex-wrap:wrap">
      <input id="appPwd" type="password" autocomplete="new-password" placeholder="${a.password_set ? 'new password' : 'password'} (at least ${esc(a.min_password_length)} characters)" style="min-width:240px">
      <button class="act" data-action="app-set-password">${a.password_set ? 'Reset password' : 'Set password'}</button>
      ${a.password_set ? '<button class="mini" data-action="app-clear-password">Remove password</button>' : ''}
    </span>
    <p class="sub" style="margin-top:14px">Signed-in phones — each signs out by itself after ${esc(a.idle_timeout_days)} days unused.</p>
    ${
      sessions.length
        ? `<table><thead><tr><th>Name</th><th>Signed in</th><th>Last used</th><th>Push${help(
            'Whether the app registered its own push endpoint, and with which server (the endpoint itself is a secret and is never shown). Notifications reach that phone without any ntfy topic set up here, filtered by the kind switches under Phone notifications. Signing the phone out stops them at once.',
          )}</th><th></th></tr></thead><tbody>${sessions
            .map(
              s => `<tr><td>${esc(s.name)}</td>
          <td class="mut">${esc(new Date(s.created * 1000).toLocaleString())}</td>
          <td class="mut">${esc(relTime(s.last_used))}</td>
          <td>${s.push ? `<span class="badge ok">on</span> <span class="mut">${esc(s.push.host)}</span>` : '<span class="mut">—</span>'}</td>
          <td><button class="mini" data-action="app-revoke-session" data-id="${esc(s.id)}">Sign out</button></td></tr>`,
            )
            .join('')}</tbody></table>
      <div style="margin-top:8px"><button class="mini" data-action="app-revoke-all">Sign out all phones</button></div>`
        : '<p class="sub mut">None.</p>'
    }
    <label class="mut" style="display:block;margin-top:14px">Trusted reverse proxies${help(
      'Addresses or CIDRs, comma-separated. Only a request arriving FROM one of these may name the real client in X-Forwarded-For or CF-Connecting-IP — that is the address failed logins are counted against. Leave empty if nothing sits in front of the app port; anyone else sending those headers is ignored.',
    )}</label>
    <span class="cn" style="display:flex;gap:6px;margin-top:4px">
      <input id="appProxies" value="${esc((a.trusted_proxies || []).join(', '))}" placeholder="e.g. 172.30.32.0/23, 192.168.1.10">
      <button class="mini" data-action="app-save-proxies">Set</button></span>`;
}

async function loadAppAccess() {
  render(await api('app-access'));
}

onAction('app-set-password', async () => {
  const input = document.getElementById('appPwd');
  const password = input.value;
  if (!password) return toast('Type a password first');
  const r = await jsonPost('app-access/password', { password });
  if (r.error) return toast('Error: ' + r.error);
  input.value = '';
  toast(r.revoked ? `Password set · ${r.revoked} phone(s) signed out` : 'Password set');
  render(r);
});
onAction('app-clear-password', async () => {
  if (!confirm('Remove the password? The app API refuses everything until a new one is set.'))
    return;
  const r = await api('app-access/password', { method: 'DELETE' });
  if (r.error) return toast('Error: ' + r.error);
  toast('Password removed');
  render(r);
});
onAction('app-revoke-session', async el => {
  const r = await api('app-access/sessions/' + encodeURIComponent(el.dataset.id), {
    method: 'DELETE',
  });
  if (r.error) return toast('Error: ' + r.error);
  toast('Signed out');
  render(r);
});
onAction('app-revoke-all', async () => {
  if (!confirm('Sign out every phone?')) return;
  const r = await api('app-access/sessions', { method: 'DELETE' });
  if (r.error) return toast('Error: ' + r.error);
  toast(`${r.revoked} phone(s) signed out`);
  render(r);
});
onAction('app-save-proxies', async () => {
  const raw = document.getElementById('appProxies').value;
  const list = raw
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
  const r = await jsonPost('app-access/trusted-proxies', { trusted_proxies: list });
  if (r.error) return toast('Error: ' + r.error);
  toast('Trusted proxies saved');
  render(r);
});

export { loadAppAccess };
