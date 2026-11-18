const AREA_COLORS = {
  COMPRAS: "#2563eb",
  SISTEMA: "#7c3aed",
  JURIDICO: "#0f766e",
  CALCULO: "#d97706",
  VAZIO: "#64748b",
  OCIOSO: "#b45309",
  DEFAULT: "#44403c",
  EMPTY: "#d6d3d1",
};

const BAY_BOXES = {
  "D-01": { x: 88, y: 248, w: 128, h: 86, rx: 4 },

  "C-01": { x: 276, y: 244, w: 42, h: 44, rx: 0 },
  "C-02": { x: 276, y: 288, w: 42, h: 44, rx: 0 },
  "C-26": { x: 276, y: 332, w: 42, h: 44, rx: 0 },
  "C-03": { x: 276, y: 376, w: 42, h: 44, rx: 0 },
  "C-04": { x: 276, y: 420, w: 42, h: 44, rx: 0 },

  "C-05": { x: 356, y: 244, w: 58, h: 36, rx: 0 },
  "C-06": { x: 414, y: 244, w: 58, h: 36, rx: 0 },
  "C-07": { x: 472, y: 244, w: 58, h: 36, rx: 0 },
  "C-08": { x: 530, y: 244, w: 58, h: 36, rx: 0 },
  "C-09": { x: 356, y: 280, w: 58, h: 36, rx: 0 },
  "C-10": { x: 414, y: 280, w: 58, h: 36, rx: 0 },
  "C-11": { x: 472, y: 280, w: 58, h: 36, rx: 0 },
  "C-12": { x: 530, y: 280, w: 58, h: 36, rx: 0 },

  "C-13": { x: 356, y: 348, w: 58, h: 36, rx: 0 },
  "C-14": { x: 414, y: 348, w: 58, h: 36, rx: 0 },
  "C-15": { x: 472, y: 348, w: 58, h: 36, rx: 0 },
  "C-16": { x: 530, y: 348, w: 58, h: 36, rx: 0 },
  "C-17": { x: 356, y: 384, w: 58, h: 36, rx: 0 },
  "C-18": { x: 414, y: 384, w: 58, h: 36, rx: 0 },
  "C-19": { x: 472, y: 384, w: 58, h: 36, rx: 0 },
  "C-20": { x: 530, y: 384, w: 58, h: 36, rx: 0 },

  "C-21": { x: 604, y: 244, w: 42, h: 44, rx: 0 },
  "C-22": { x: 604, y: 288, w: 42, h: 44, rx: 0 },
  "C-23": { x: 604, y: 332, w: 42, h: 44, rx: 0 },
  "C-24": { x: 604, y: 376, w: 42, h: 44, rx: 0 },
  "C-25": { x: 604, y: 420, w: 42, h: 44, rx: 0 },

  "J-01": { x: 680, y: 244, w: 52, h: 44, rx: 0 },
  "J-02": { x: 732, y: 244, w: 52, h: 44, rx: 0 },
  "J-03": { x: 680, y: 288, w: 52, h: 44, rx: 0 },
  "J-04": { x: 732, y: 288, w: 52, h: 44, rx: 0 },
  "J-05": { x: 680, y: 332, w: 52, h: 44, rx: 0 },
  "J-06": { x: 732, y: 332, w: 52, h: 44, rx: 0 },
  "J-07": { x: 680, y: 376, w: 52, h: 44, rx: 0 },
  "J-08": { x: 732, y: 376, w: 52, h: 44, rx: 0 },

  "J-09": { x: 836, y: 244, w: 42, h: 44, rx: 0 },
  "J-10": { x: 836, y: 288, w: 42, h: 44, rx: 0 },
  "J-11": { x: 836, y: 332, w: 42, h: 44, rx: 0 },
  "J-12": { x: 836, y: 376, w: 42, h: 44, rx: 0 },

  "R-01": { x: 760, y: 112, w: 86, h: 48, rx: 3 },
};

const AREAS = ["COMPRAS", "SISTEMA", "JURIDICO", "CALCULO", "OCIOSO", "VAZIO"];
const WIN_STATUS = ["ATIVO", "INATIVO", "DESATIVADO"];
const OFFICE_STATUS = ["PENDENTE", "ATIVO", "INATIVO", "NAO"];
const BITLOCKER_STATUS = ["", "ATIVO", "INATIVO"];

const MAP_DEFAULTS = {
  occupancy: "all",
  label: "person",
  rooms: true,
  empty: true,
  zoom: 1,
};

function loadMapOptions() {
  try {
    const saved = JSON.parse(localStorage.getItem("red-map-options") || "{}");
    return { ...MAP_DEFAULTS, ...saved };
  } catch {
    return { ...MAP_DEFAULTS };
  }
}

const state = {
  bays: [],
  machines: [],
  stats: null,
  view: "map",
  areaFilter: "TODAS",
  search: "",
  selectedBay: null,
  selectedMachine: null,
  gotoUsers: [],
  gotoPhones: [],
  gotoHistory: [],
  map: loadMapOptions(),
};

const $ = (id) => document.getElementById(id);
const LAYOUT_BASE = "/layout-app";

async function api(path, options = {}) {
  const res = await fetch(LAYOUT_BASE + path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  if (res.status === 204) return null;
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const detail = data.detail;
    const msg = typeof detail === "string"
      ? detail
      : Array.isArray(detail)
        ? (detail[0]?.msg || "Falha na requisição")
        : "Falha na requisição";
    throw new Error(msg);
  }
  return data;
}

let toastTimer = null;
function toast(msg, kind = "info") {
  const el = $("toast");
  if (!el) return;
  el.textContent = msg;
  el.classList.remove("ok", "err", "info");
  el.classList.add(kind === "ok" || kind === "err" ? kind : "info");
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.hidden = true; }, 6000);
}

function showSyscallManual(created) {
  if (!created || !created.syscall_skipped || !created.syscall_username) return;
  const root = $("syscall-manual");
  if (!root) return;
  const nome = [created.first_name, created.last_name].filter(Boolean).join(" ");
  $("syscall-manual-nome").textContent = nome || created.syscall_username;
  $("syscall-manual-email").textContent = created.email || "";
  $("syscall-manual-user").textContent = created.syscall_username;
  $("syscall-manual-pass").textContent = created.syscall_password || created.syscall_username;
  root.hidden = false;
  const ok = $("syscall-manual-ok");
  if (ok) ok.focus();
}

function setGotoResult(message, kind = "") {
  const result = $("goto-result");
  if (!result) return;
  result.classList.remove("ok", "err", "info");
  if (!message) {
    result.hidden = true;
    result.textContent = "";
    return;
  }
  if (kind) result.classList.add(kind);
  result.hidden = false;
  result.textContent = message;
}

function setView(view) {
  state.view = view;
  document.querySelectorAll(".view-toggle button").forEach((b) => {
    b.classList.toggle("active", b.dataset.view === view);
  });
  $("map-view").classList.toggle("active", view === "map");
  $("inventory-view").classList.toggle("active", view === "inventory");
  $("goto-view").classList.toggle("active", view === "goto");
  $("history-view").classList.toggle("active", view === "history");
  if (view === "goto") {
    loadPlataformaStatus();
    loadMondayStatus();
    loadSyscallStatus();
    loadGotoStatus();
    loadGotoPhones().then(() => {
      loadGotoUsers();
      renderGotoForm();
    });
  }
  if (view === "history") {
    loadGotoHistory();
  }
}

async function loadPlataformaStatus() {
  const el = $("plataforma-status");
  if (!el) return;
  el.classList.remove("ok", "err");
  el.textContent = "Verificando Plataforma Central…";
  try {
    const info = await api("/api/plataforma/status");
    el.classList.add("ok");
    const host = info.host ? ` · ${info.host}` : "";
    const count = typeof info.users === "number" ? ` · ${info.users} usuários` : "";
    el.textContent = `Plataforma Central conectada${host}${count}`;
  } catch (err) {
    el.classList.add("err");
    el.textContent = err.message;
  }
}

async function loadSyscallStatus() {
  const el = $("syscall-status");
  if (!el) return;
  el.classList.remove("ok", "err");
  el.textContent = "Verificando SysCall…";
  try {
    const info = await api("/api/syscall/status");
    el.classList.add("ok");
    const count = typeof info.users === "number" ? ` · ${info.users} usuários` : "";
    el.textContent = `SysCall conectado${count} (só comprador)`;
  } catch (err) {
    el.classList.add("err");
    el.textContent = err.message;
  }
}

async function loadMondayStatus() {
  const el = $("monday-status");
  if (!el) return;
  el.classList.remove("ok", "err");
  el.textContent = "Verificando Monday…";
  try {
    const info = await api("/api/monday/status");
    el.classList.add("ok");
    const board = info.board_name ? ` · quadro ${info.board_name}` : "";
    const email = info.email ? ` (${info.email})` : "";
    el.textContent = `Monday conectada${board}${email}`;
  } catch (err) {
    el.classList.add("err");
    el.textContent = err.message;
  }
}

async function loadGotoStatus() {
  const el = $("goto-status");
  if (!el) return;
  el.classList.remove("ok", "err");
  el.textContent = "Verificando GoTo…";
  try {
    const info = await api("/api/goto/status");
    el.classList.add("ok");
    const name = info.account_name ? ` · ${info.account_name}` : "";
    const email = info.email ? ` (${info.email})` : "";
    el.textContent = `GoTo conectada${name}${email}`;
  } catch (err) {
    el.classList.add("err");
    el.textContent = err.message;
  }
}

function fitOptionText(text, max = 64) {
  const value = String(text || "").trim();
  if (value.length <= max) return value;
  return `${value.slice(0, Math.max(1, max - 1))}…`;
}

function machineOptionLabel(machine) {
  const person = (machine.user_name || "").trim() || "sem colaborador";
  const bay = (machine.bay_id || "").trim();
  return bay ? `${machine.hostname} — ${person} · ${bay}` : `${machine.hostname} — ${person}`;
}

function machineOptionsHtml(selectedId = "") {
  const options = state.machines
    .slice()
    .sort((a, b) => (a.hostname || "").localeCompare(b.hostname || "", "pt"))
    .map((m) => {
      const selected = String(m.id) === String(selectedId) ? "selected" : "";
      return `<option value="${m.id}" ${selected}>${escapeXml(fitOptionText(machineOptionLabel(m)))}</option>`;
    })
    .join("");
  return `<option value="">Sem vínculo</option>${options}`;
}

function updateGotoBayHint() {
  const hint = $("goto-bay-hint");
  const select = $("goto-machine");
  if (!hint || !select) return;
  const machine = state.machines.find((m) => String(m.id) === String(select.value));
  hint.textContent = machine
    ? `Baia desta máquina: ${machine.bay_id || "ainda sem baia"}`
    : "A baia segue a máquina escolhida.";
}

function phoneOptionsHtml(selected = "", currentUserKey = "") {
  const selectedVal = String(selected || "");
  const phones = state.gotoPhones || [];
  const known = new Set(phones.map((phone) => String(phone.number || "")));
  const extra = selectedVal && !known.has(selectedVal)
    ? `<option value="${escapeXml(selectedVal)}" selected>${escapeXml(fitOptionText(`${selectedVal} · atual`))}</option>`
    : "";
  const options = phones.map((phone) => {
    const number = String(phone.number || "");
    const holderUser = phone.assigned_user_key
      ? (state.gotoUsers || []).find((user) => user.user_key === phone.assigned_user_key)
      : null;
    const holderName = holderUser
      ? `${holderUser.first_name || ""} ${holderUser.last_name || ""}`.trim()
      : (phone.assigned_to || "");
    const taken = holderName && phone.assigned_user_key && phone.assigned_user_key !== currentUserKey
      ? ` · ${holderName}`
      : "";
    const selectedMark = number === selectedVal ? "selected" : "";
    return `<option value="${escapeXml(number)}" ${selectedMark}>${escapeXml(fitOptionText(`${phone.label || number}${taken}`))}</option>`;
  }).join("");
  return `<option value="">Sem número</option>${extra}${options}`;
}

function isGotoEnabled() {
  return $("goto-enable")?.getAttribute("aria-pressed") === "true";
}

function selectedArea() {
  return document.querySelector('#goto-form input[name="area"]:checked')?.value || "";
}

function setGotoEnabled(on) {
  const btn = $("goto-enable");
  const options = $("goto-options");
  const submit = $("goto-submit");
  const hint = $("goto-enable-hint");
  const area = selectedArea();
  if (on && area && area !== "COMPRAS") on = false;
  if (btn) {
    btn.setAttribute("aria-pressed", on ? "true" : "false");
    btn.classList.toggle("primary", on);
    btn.disabled = Boolean(area) && area !== "COMPRAS";
  }
  if (options) options.hidden = !on;
  if (submit) submit.textContent = "Criar usuário";
  if (hint) {
    if (area === "JURIDICO" || area === "CALCULO") {
      hint.textContent = "GoTo só para compradores. Esta área entra só na Monday e, se quiser, no inventário.";
    } else if (on) {
      hint.textContent = "GoTo ativo: o colaborador será criado na GoTo com licença de comprador.";
    } else {
      hint.textContent = "Ative somente para compradores. Desligado, nada é criado na GoTo.";
    }
  }
  const areaHint = $("goto-area-hint");
  if (areaHint) {
    if (area === "COMPRAS") {
      areaHint.textContent = "Monday: convite de membro, equipe COMPRAS e rótulo Nome Sobrenome na coluna Comprador dos quadros SISTEMA, SysCall - Inclusões e INCLUSÕES.";
    } else if (area === "JURIDICO") {
      areaHint.textContent = "Monday: convite de membro, equipe JURÍDICO e rótulo Nome Sobrenome na coluna Responsável Jurídico.";
    } else if (area === "CALCULO") {
      areaHint.textContent = "Monday: convite de membro, equipe CÁLCULO e rótulo Nome Sobrenome na coluna Calculista.";
    } else {
      areaHint.textContent = "Escolha o tipo. A Monday recebe convite de membro no Work Management, coloca na equipe e cria o rótulo Nome Sobrenome na coluna da área.";
    }
  }
  if (!on) {
    showExistingGotoUser(null);
    const status = $("goto-email-status");
    if (status) {
      status.classList.remove("ok", "warn", "err");
      status.textContent = "Informe o e-mail para verificar se o usuário já existe.";
    }
    const phone = $("goto-phone");
    if (phone) phone.value = "";
    return;
  }
  renderGotoForm();
  const email = $("goto-form")?.email?.value.trim();
  if (email) lookupGotoEmail(email);
}

function renderGotoForm() {
  const select = $("goto-machine");
  if (select) {
    const current = select.value;
    select.innerHTML = machineOptionsHtml(current);
    updateGotoBayHint();
  }
  const phone = $("goto-phone");
  if (phone) {
    const current = phone.value;
    phone.innerHTML = phoneOptionsHtml(current);
  }
  const hint = $("goto-phone-hint");
  if (hint) {
    const n = (state.gotoPhones || []).length;
    hint.textContent = n
      ? `${n} números na lista da GoTo. Ao escolher um, ele vira identificador e as chamadas vão para o ramal da pessoa.`
      : "Lista telefônica vazia. Clique em Atualizar lista telefônica.";
  }
}

async function loadGotoPhones() {
  try {
    const data = await api("/api/goto/phones");
    state.gotoPhones = data.phones || [];
  } catch (err) {
    state.gotoPhones = [];
    toast(err.message, "err");
  }
  renderGotoForm();
}

function showExistingGotoUser(user) {
  const box = $("goto-existing");
  if (!box) return;
  if (!user) {
    box.hidden = true;
    box.innerHTML = "";
    return;
  }
  const name = `${user.first_name || ""} ${user.last_name || ""}`.trim() || "—";
  box.hidden = false;
  box.innerHTML = `
    <div class="summary-row"><span>Nome</span><strong>${escapeXml(name)}</strong></div>
    <div class="summary-row"><span>E-mail</span><strong>${escapeXml(user.email || "—")}</strong></div>
    <div class="summary-row"><span>Número</span><strong>${escapeXml(user.phone || "—")}</strong></div>
    <div class="summary-row"><span>Máquina</span><strong>${escapeXml(user.hostname || "sem vínculo")}</strong></div>
    <div class="summary-row"><span>Baia</span><strong>${escapeXml(user.bay_id || "—")}</strong></div>
    <div class="summary-row"><span>Colaborador no inventário</span><strong>${escapeXml(user.user_name || "—")}</strong></div>
  `;
}

async function lookupGotoEmail(email) {
  const status = $("goto-email-status");
  if (!status) return null;
  if (!isGotoEnabled()) return null;
  status.classList.remove("ok", "warn", "err");
  if (!email || !email.includes("@")) {
    status.textContent = "Informe o e-mail para verificar se o usuário já existe.";
    showExistingGotoUser(null);
    return null;
  }
  status.textContent = "Verificando e-mail na GoTo…";
  try {
    const data = await api(`/api/goto/users/lookup?email=${encodeURIComponent(email)}`);
    if (!data.found) {
      status.classList.add("ok");
      status.textContent = "E-mail livre. Pode criar o usuário.";
      showExistingGotoUser(null);
      return null;
    }
    status.classList.add("warn");
    status.textContent = "Usuário já cadastrado na GoTo. Dados carregados abaixo.";
    showExistingGotoUser(data.user);
    const form = $("goto-form");
    if (form && data.user) {
      if (!form.first_name.value && data.user.first_name) form.first_name.value = data.user.first_name;
      if (!form.last_name.value && data.user.last_name) form.last_name.value = data.user.last_name;
      if (form.phone) form.phone.innerHTML = phoneOptionsHtml(data.user.phone || "");
      if (data.user.machine_id && form.machine_id) {
        form.machine_id.value = String(data.user.machine_id);
        updateGotoBayHint();
      }
    }
    return data.user;
  } catch (err) {
    status.classList.add("err");
    status.textContent = err.message;
    showExistingGotoUser(null);
    return null;
  }
}

function renderGotoUsers() {
  const body = $("goto-users-body");
  if (!body) return;
  const q = state.search.trim().toLowerCase();
  const rows = (state.gotoUsers || []).filter((user) => {
    if (!q) return true;
    const hay = [
      user.first_name,
      user.last_name,
      user.email,
      user.phone,
      user.hostname,
      user.bay_id,
      user.user_name,
    ].join(" ").toLowerCase();
    return hay.includes(q);
  });
  body.innerHTML = rows.map((user) => {
    const name = escapeXml(`${user.first_name || ""} ${user.last_name || ""}`.trim() || "—");
    const email = escapeXml(user.email || "—");
    const statusLabel = user.status ? String(user.status).toUpperCase() : "—";
    return `
      <tr>
        <td><strong>${name}</strong></td>
        <td>${email}</td>
        <td>
          <div class="cell-select">
            <select data-phone-user="${escapeXml(user.user_key)}" data-email="${escapeXml(user.email || "")}" data-first="${escapeXml(user.first_name || "")}" data-last="${escapeXml(user.last_name || "")}">
              ${phoneOptionsHtml(user.phone || "", user.user_key)}
            </select>
          </div>
        </td>
        <td>
          <div class="cell-select">
            <select data-link-user="${escapeXml(user.user_key)}" data-email="${escapeXml(user.email || "")}" data-first="${escapeXml(user.first_name || "")}" data-last="${escapeXml(user.last_name || "")}">
              ${machineOptionsHtml(user.machine_id || "")}
            </select>
          </div>
        </td>
        <td>${statusBadge(statusLabel)}</td>
        <td>
          <button type="button" class="btn danger" data-suspend-user="${escapeXml(user.user_key)}">Excluir</button>
        </td>
      </tr>`;
  }).join("") || `<tr><td colspan="6">Nenhum usuário encontrado.</td></tr>`;
}

async function loadGotoUsers() {
  const body = $("goto-users-body");
  if (body) body.innerHTML = `<tr><td colspan="6">Carregando usuários da GoTo…</td></tr>`;
  try {
    state.gotoUsers = await api("/api/goto/users");
    renderGotoUsers();
  } catch (err) {
    state.gotoUsers = [];
    if (body) body.innerHTML = `<tr><td colspan="6">${err.message}</td></tr>`;
    toast(err.message);
  }
}

function formatWhen(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleString("pt-BR", { dateStyle: "short", timeStyle: "short" });
}

function renderGotoHistory() {
  const body = $("history-body");
  if (!body) return;
  const q = state.search.trim().toLowerCase();
  const rows = (state.gotoHistory || []).filter((row) => {
    if (!q) return true;
    const hay = [
      row.first_name,
      row.last_name,
      row.email,
      row.phone,
      row.area,
      row.hostname,
      row.bay_id,
    ].join(" ").toLowerCase();
    return hay.includes(q);
  });
  body.innerHTML = rows.map((row) => {
    const name = escapeXml(`${row.first_name || ""} ${row.last_name || ""}`.trim() || "—");
    const place = [row.hostname, row.bay_id].filter(Boolean).join(" · ") || "—";
    return `
      <tr>
        <td><strong>${name}</strong></td>
        <td>${escapeXml(row.email || "—")}</td>
        <td>${escapeXml(row.area || "—")}</td>
        <td>${escapeXml(row.phone || "—")}</td>
        <td>${escapeXml(place)}</td>
        <td>${escapeXml(formatWhen(row.entered_at))}</td>
        <td>${escapeXml(formatWhen(row.excluded_at))}</td>
      </tr>`;
  }).join("") || `<tr><td colspan="7">Nenhum usuário no histórico ainda.</td></tr>`;
}

async function loadGotoHistory() {
  const body = $("history-body");
  if (body) body.innerHTML = `<tr><td colspan="7">Carregando histórico…</td></tr>`;
  try {
    state.gotoHistory = await api("/api/goto/history");
    renderGotoHistory();
  } catch (err) {
    state.gotoHistory = [];
    if (body) body.innerHTML = `<tr><td colspan="7">${err.message}</td></tr>`;
    toast(err.message);
  }
}

function isIdle(machine) {
  return (machine?.area || "").toUpperCase() === "OCIOSO";
}

function areaLabel(area) {
  if ((area || "").toUpperCase() === "OCIOSO") return "Ocioso";
  return area || "";
}

function areaColor(area) {
  if (!area) return AREA_COLORS.DEFAULT;
  return AREA_COLORS[area.toUpperCase()] || AREA_COLORS.DEFAULT;
}

function statusBadge(value) {
  const v = (value || "").toUpperCase();
  if (["ATIVO", "OK", "CONCLUIDO", "ACTIVE"].includes(v)) return `<span class="badge ok">${value}</span>`;
  if (["PENDENTE", "CONVITE"].includes(v)) return `<span class="badge warn">${value}</span>`;
  if (["INATIVO", "DESATIVADO", "NAO", "SUSPENDED", "SUSPENSO"].includes(v)) return `<span class="badge off">${value || "—"}</span>`;
  return `<span class="badge muted">${value || "—"}</span>`;
}

function matchesSearch(machine, bay) {
  const q = state.search.trim().toLowerCase();
  if (!q) return true;
  const hay = [
    bay?.id, bay?.label, bay?.zone,
    machine?.hostname, machine?.user_name, machine?.email,
    machine?.ramal, machine?.numero_ind, machine?.area, machine?.observacao,
  ].join(" ").toLowerCase();
  return hay.includes(q);
}

function visibleByArea(machine) {
  if (state.areaFilter === "TODAS") return true;
  if (state.areaFilter === "LIVRE") return !machine;
  return (machine?.area || "").toUpperCase() === state.areaFilter;
}

function bayMachines(bay) {
  if (bay?.machines?.length) return bay.machines;
  if (bay?.machine) return [bay.machine];
  return [];
}

function bayVisible(bay) {
  const machines = bayMachines(bay);
  const occupied = machines.length > 0;
  if (state.map.occupancy === "busy" && !occupied) return false;
  if (state.map.occupancy === "free" && occupied) return false;
  if (!machines.length) return visibleByArea(null) && matchesSearch(null, bay);
  return machines.some((m) => visibleByArea(m) && matchesSearch(m, bay));
}

function saveMapOptions() {
  localStorage.setItem("red-map-options", JSON.stringify(state.map));
}

function fitMapText(text, max) {
  const value = String(text || "").trim() || "—";
  if (value.length <= max) return value;
  return `${value.slice(0, Math.max(1, max - 1))}…`;
}

function personShort(name) {
  const parts = String(name || "").trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return "";
  if (parts.length === 1) return parts[0];
  return `${parts[0]} ${parts.at(-1)[0]}.`;
}

function bayCaption(machines, box) {
  const max = Math.max(5, Math.floor((box.w - 4) / 4.575));
  if (!machines.length) return fitMapText("livre", max);
  if (machines.length > 1 && state.map.label !== "ramal") {
    return fitMapText(`${machines.length} PCs`, max);
  }
  const machine = machines[0];
  if (state.map.label === "hostname") return fitMapText(machine.hostname || "PC", max);
  if (state.map.label === "ramal") return fitMapText(machine.ramal || "—", max);
  if (state.map.label === "area") return fitMapText(areaLabel(machine.area) || "—", max);
  if (isIdle(machine) && !machine.user_name) return fitMapText("Ocioso", max);
  return fitMapText(personShort(machine.user_name) || machine.hostname || "ocupada", max);
}

function renderMapControls() {
  document.querySelectorAll("[data-map-occupancy]").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.mapOccupancy === state.map.occupancy);
  });
  document.querySelectorAll("[data-map-label]").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.mapLabel === state.map.label);
  });
  const rooms = $("map-rooms");
  const empty = $("map-empty");
  if (rooms) rooms.checked = Boolean(state.map.rooms);
  if (empty) empty.checked = Boolean(state.map.empty);
  const zoom = $("map-zoom-label");
  if (zoom) zoom.textContent = `${Math.round(state.map.zoom * 100)}%`;
}

function applyMapZoom() {
  const svg = $("floorplan")?.querySelector("svg");
  if (svg) {
    svg.style.transform = "none";
    svg.style.width = `${Math.round(state.map.zoom * 100)}%`;
  }
  const zoom = $("map-zoom-label");
  if (zoom) zoom.textContent = `${Math.round(state.map.zoom * 100)}%`;
}

async function loadAll() {
  const [bays, machines, stats] = await Promise.all([
    api("/api/bays"),
    api("/api/machines"),
    api("/api/stats"),
  ]);
  state.bays = bays;
  state.machines = machines;
  state.stats = stats;
  render();
}

function render() {
  renderStats();
  renderFilters();
  renderMapControls();
  renderFloorplan();
  renderInventory();
  renderLegend();
  renderGotoForm();
  renderGotoUsers();
}

function renderStats() {
  const s = state.stats || {};
  $("stats").innerHTML = [
    ["Baias", s.total_bays ?? "—"],
    ["Ocupadas", s.occupied ?? "—"],
    ["Livres", s.empty ?? "—"],
    ["Máquinas", s.machines ?? "—"],
    ["Sem baia", s.unassigned ?? "—"],
  ].map(([label, value]) => `<article class="stat"><span>${label}</span><strong>${value}</strong></article>`).join("");
}

function uniqueAreas() {
  const set = new Set(state.machines.map((m) => (m.area || "").toUpperCase()).filter(Boolean));
  return ["TODAS", "LIVRE", ...[...set].sort()];
}

function renderFilters() {
  const chips = uniqueAreas().map((area) => {
    const color = area === "TODAS" || area === "LIVRE" ? "#a8a29e" : areaColor(area);
    return `<button type="button" class="chip ${state.areaFilter === area ? "active" : ""}" data-area="${area}">
      <span class="dot" style="background:${color}"></span>${area}
    </button>`;
  }).join("");
  $("area-filters").innerHTML = chips;
  $("inv-filters").innerHTML = chips;
}

function renderLegend() {
  const items = [
    ["Livre", AREA_COLORS.EMPTY],
    ["Compras", AREA_COLORS.COMPRAS],
    ["Sistema", AREA_COLORS.SISTEMA],
    ["Jurídico", AREA_COLORS.JURIDICO],
    ["Cálculo", AREA_COLORS.CALCULO],
    ["Ocioso", AREA_COLORS.OCIOSO],
    ["Vazio", AREA_COLORS.VAZIO],
  ];
  $("legend").innerHTML = items.map(([name, color]) =>
    `<span><i class="swatch" style="background:${color}"></i>${name}</span>`
  ).join("");
}

function renderFloorplan() {
  const baysById = Object.fromEntries(state.bays.map((b) => [b.id, b]));
  const desks = Object.entries(BAY_BOXES).map(([id, box]) => {
    const bay = baysById[id];
    const machines = bayMachines(bay);
    const occupied = machines.length > 0;
    const fill = occupied ? areaColor(machines[0].area) : AREA_COLORS.EMPTY;
    const match = bayVisible(bay);
    const selected = state.selectedBay === id;
    const hideEmpty = !state.map.empty && !occupied;
    const shortCap = bayCaption(machines, box);
    const labelY = box.y + Math.min(13, box.h * 0.38);
    const userY = box.y + box.h - Math.min(8, box.h * 0.22);
    const textFill = occupied ? "#fff" : "#57534e";
    const tip = occupied
      ? `${id} · ${machines.map((m) => {
          const who = isIdle(m) && !m.user_name ? "Ocioso — máquina alocada porém disponível" : (m.user_name || m.hostname);
          const extra = [m.hostname, m.ramal].filter(Boolean).join(" · ");
          return `${who}${extra ? ` (${extra})` : ""}`;
        }).join(" · ")}`
      : `${id} · baia livre`;
    return `
      <g class="bay ${selected ? "selected" : ""} ${match ? "" : "dim"} ${hideEmpty ? "hidden" : ""}" data-bay="${id}">
        <title>${escapeXml(tip)}</title>
        <rect class="desk" x="${box.x}" y="${box.y}" width="${box.w}" height="${box.h}" rx="${Math.max(box.rx, 3)}"
          fill="${occupied ? fill : "#f6f3ee"}" stroke="${occupied ? fill : "#a8a29e"}" stroke-width="1.15"
          ${occupied ? 'filter="url(#deskGlow)"' : 'stroke-dasharray="3.5 2.5"'} />
        <text class="bay-label" x="${box.x + box.w / 2}" y="${labelY}" text-anchor="middle" fill="${textFill}">${id}</text>
        <text class="bay-user" x="${box.x + box.w / 2}" y="${userY}" text-anchor="middle" fill="${textFill}">${escapeXml(shortCap)}</text>
      </g>`;
  }).join("");

  $("floorplan").innerHTML = `
    <svg viewBox="20 0 900 538" role="img" aria-label="Planta do escritório RED" class="${state.map.rooms ? "" : "map-rooms-off"}">
      <defs>
        <pattern id="floorGrain" width="22" height="22" patternUnits="userSpaceOnUse">
          <path d="M22 0H0V22" fill="none" stroke="#e9e4db" stroke-width="0.7"/>
        </pattern>
        <filter id="deskGlow" x="-20%" y="-20%" width="140%" height="140%">
          <feDropShadow dx="0" dy="1.2" stdDeviation="1.1" flood-color="#1c1917" flood-opacity="0.22"/>
        </filter>
        <marker id="arrow" markerWidth="7" markerHeight="7" refX="3.5" refY="3.5" orient="auto">
          <path d="M0 0 L7 3.5 L0 7 Z" fill="#78716c"/>
        </marker>
      </defs>

      <rect x="48" y="86" width="842" height="410" fill="#f3efe6"/>
      <rect x="48" y="86" width="842" height="410" fill="url(#floorGrain)" opacity="0.7"/>
      <rect x="48" y="86" width="442" height="92" fill="#ebe6dc"/>
      <rect x="48" y="178" width="214" height="318" fill="#ebe6dc"/>
      <rect x="556" y="86" width="68" height="92" fill="#ebe6dc"/>
      <rect x="624" y="86" width="68" height="92" fill="#ebe6dc"/>
      <rect x="692" y="86" width="56" height="46" fill="#ebe6dc"/>
      <rect x="748" y="86" width="142" height="122" fill="#ebe6dc"/>
      <rect x="262" y="208" width="392" height="288" fill="#e8e3d8"/>
      <rect x="654" y="208" width="236" height="288" fill="#e4efe9"/>

      <rect x="48" y="86" width="842" height="410" fill="none" stroke="#3f3f46" stroke-width="1.6"/>
      <line x1="48" y1="178" x2="490" y2="178" stroke="#3f3f46" stroke-width="1.3"/>
      <line x1="556" y1="178" x2="692" y2="178" stroke="#3f3f46" stroke-width="1.3"/>
      <line x1="262" y1="178" x2="262" y2="496" stroke="#3f3f46" stroke-width="1.3"/>
      <line x1="490" y1="86" x2="490" y2="178" stroke="#3f3f46" stroke-width="1.3"/>
      <line x1="556" y1="86" x2="556" y2="178" stroke="#3f3f46" stroke-width="1.3"/>
      <line x1="624" y1="86" x2="624" y2="178" stroke="#3f3f46" stroke-width="1.3"/>
      <line x1="692" y1="86" x2="692" y2="178" stroke="#3f3f46" stroke-width="1.3"/>
      <line x1="692" y1="132" x2="748" y2="132" stroke="#3f3f46" stroke-width="1.3"/>
      <line x1="748" y1="86" x2="748" y2="170" stroke="#3f3f46" stroke-width="1.3"/>
      <line x1="262" y1="208" x2="320" y2="208" stroke="#3f3f46" stroke-width="1.3"/>
      <line x1="356" y1="208" x2="680" y2="208" stroke="#3f3f46" stroke-width="1.3"/>
      <line x1="716" y1="208" x2="890" y2="208" stroke="#3f3f46" stroke-width="1.3"/>
      <line x1="654" y1="208" x2="654" y2="496" stroke="#3f3f46" stroke-width="1.3"/>
      <line x1="590" y1="108" x2="590" y2="178" stroke="#3f3f46" stroke-width="1"/>
      <line x1="658" y1="108" x2="658" y2="178" stroke="#3f3f46" stroke-width="1"/>

      <g class="room-layer">
        <text class="room-label-in" x="268" y="138" text-anchor="middle">Copa</text>
        <text class="room-label-in" x="523" y="138" text-anchor="middle">Entrada</text>
        <text class="room-label-in" x="720" y="114" text-anchor="middle">Serviço</text>
        <text class="room-label-in" x="500" y="196" text-anchor="middle">Corredor</text>
        <text class="room-label-in" x="155" y="372" text-anchor="middle">Sala Diretoria</text>
        <text class="room-label-in" x="820" y="102" text-anchor="middle">Sala de Reunião</text>
        <text class="room-label" x="430" y="478" text-anchor="middle">Comercial</text>
        <line class="leader" x1="430" y1="466" x2="430" y2="444"/>
        <text class="room-label" x="772" y="478" text-anchor="middle">Jurídico</text>
        <line class="leader" x1="772" y1="466" x2="772" y2="434"/>
        <text class="room-label" x="470" y="24" text-anchor="middle">Banheiro</text>
        <text class="room-label" x="470" y="38" text-anchor="middle">Masculino</text>
        <line class="leader" x1="470" y1="42" x2="590" y2="82" marker-end="url(#arrow)"/>
        <text class="room-label" x="802" y="24" text-anchor="middle">Banheiro</text>
        <text class="room-label" x="802" y="38" text-anchor="middle">Feminino</text>
        <line class="leader" x1="802" y1="42" x2="658" y2="82" marker-end="url(#arrow)"/>
      </g>

      ${desks}
    </svg>`;
  applyMapZoom();
}

function escapeXml(str) {
  return String(str)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function renderInventory() {
  const rows = state.machines
    .filter((m) => {
      const bay = state.bays.find((b) => b.id === m.bay_id);
      return visibleByArea(m) && matchesSearch(m, bay);
    })
    .map((m) => `
      <tr data-machine="${m.id}">
        <td><strong>${m.hostname}</strong></td>
        <td>${m.bay_id || "—"}</td>
        <td>${isIdle(m) && !m.user_name ? "Ocioso" : (m.user_name || "—")}</td>
        <td>${isIdle(m) ? "Ocioso — máquina alocada porém disponível" : (m.area || "—")}</td>
        <td>${m.email || "—"}</td>
        <td>${m.ramal || "—"}</td>
        <td>${m.numero_ind || "—"}</td>
        <td>${statusBadge(m.win_status)}</td>
        <td>${statusBadge(m.office_status)}</td>
        <td>${statusBadge(m.bitlocker_status)}</td>
      </tr>`).join("");
  $("inventory-body").innerHTML = rows || `<tr><td colspan="10">Nenhuma máquina encontrada.</td></tr>`;
}

function openDrawer({ bay = null, machine = null, creating = false } = {}) {
  const machinesHere = bayMachines(bay);
  const mapMode = state.view === "map" && Boolean(bay);
  const isCreate = !mapMode && (creating || (bay && !machine && machinesHere.length === 0));
  state.selectedBay = bay?.id || machine?.bay_id || null;
  state.selectedMachine = machine || null;
  $("drawer").classList.add("open");
  $("drawer").setAttribute("aria-hidden", "false");
  $("drawer-kicker").textContent = bay ? `${bay.label} · ${bay.zone}` : "Inventário";
  if (mapMode) {
    $("drawer-title").textContent = machinesHere.length > 1
      ? `${bay.id} · ${machinesHere.length} máquinas`
      : (machine?.user_name || machine?.hostname || bay.id);
  } else if (isCreate && !machine) {
    $("drawer-title").textContent = "Nova máquina";
  } else {
    $("drawer-title").textContent = machine?.hostname || bay?.id || "Inventário";
  }
  if (mapMode) renderMapPanel({ bay, machine });
  else renderForm({ bay, machine, creating: isCreate && !machine });
  renderFloorplan();
  const focusName = mapMode ? "person_search" : ((isCreate && !machine) ? "hostname" : "user_name");
  $("machine-form").querySelector(`[name=${focusName}]`)?.focus();
}

function closeDrawer() {
  $("drawer").classList.remove("open");
  $("drawer").setAttribute("aria-hidden", "true");
  state.selectedBay = null;
  state.selectedMachine = null;
  renderFloorplan();
}

function field(name, label, value = "", extra = "", attrs = {}) {
  const required = attrs.required ? " required" : "";
  return `<div class="field ${extra}"><label>${label}<input name="${name}" value="${escapeAttr(value || "")}"${required} /></label></div>`;
}

function escapeAttr(str) {
  return String(str).replaceAll("&", "&amp;").replaceAll('"', "&quot;").replaceAll("<", "&lt;");
}

function personLabel(item) {
  if (item.user_name) return `${item.user_name} · ${item.hostname}`;
  return `Sem usuário · ${item.hostname}`;
}

function peopleForAssign(bayId) {
  return [...state.machines].sort((a, b) => {
    const an = (a.user_name || a.hostname).localeCompare(b.user_name || b.hostname, "pt-BR");
    const aNamed = a.user_name ? 0 : 1;
    const bNamed = b.user_name ? 0 : 1;
    return aNamed - bNamed || an;
  }).map((item) => {
    const elsewhere = item.bay_id && item.bay_id !== bayId ? ` (${item.bay_id})` : "";
    return { value: String(item.id), label: `${personLabel(item)}${elsewhere}`, name: item.user_name || "", hostname: item.hostname };
  });
}

function summaryRow(label, value) {
  return `<div class="summary-row"><span>${label}</span><strong>${escapeAttr(value || "—")}</strong></div>`;
}

function renderMapPanel({ bay, machine }) {
  const here = bayMachines(bay);
  const people = peopleForAssign(bay.id);
  const chips = `
    <div class="bay-machine-list">
      ${here.map((item) => `
        <button type="button" class="machine-chip ${machine && item.id === machine.id ? "active" : ""}" data-pick-machine="${item.id}">
          ${escapeAttr(isIdle(item) && !item.user_name ? "Ocioso" : (item.user_name || item.hostname))}
        </button>`).join("")}
    </div>
  `;
  const summary = machine ? `
    <div class="person-summary">
      ${summaryRow("Nome", machine.user_name)}
      ${summaryRow("Hostname", machine.hostname)}
      ${summaryRow("Área", isIdle(machine) ? "Ocioso — máquina alocada porém disponível" : machine.area)}
      ${summaryRow("E-mail", machine.email)}
      ${summaryRow("Ramal", machine.ramal)}
      ${summaryRow("Telefone", machine.numero_ind)}
    </div>
  ` : `<p class="empty-bay">Baia livre. Escolha um colaborador do inventário para alocar.</p>`;

  $("machine-form").innerHTML = `
    ${here.length ? chips : ""}
    <div class="assign-row">
      <div class="field">
        <label>Colaborador
          <input name="person_search" type="search" placeholder="Digite o nome..." autocomplete="off" />
        </label>
      </div>
    </div>
    <div class="field full">
      <label>Alocar nesta baia
        <select class="listbox" name="assign_existing" size="8">
          <option value="">Selecione pelo nome...</option>
          ${people.map((item) => `<option value="${escapeAttr(item.value)}" data-name="${escapeAttr(item.name)}" data-host="${escapeAttr(item.hostname)}">${escapeAttr(item.label)}</option>`).join("")}
        </select>
      </label>
    </div>
    ${summary}
    <div class="form-actions">
      <button type="button" class="btn primary" id="btn-assign">Alocar</button>
      ${machine ? `<button type="button" class="btn" id="btn-unassign">Tirar da baia</button>
                   <button type="button" class="btn ghost" id="btn-edit-inventory">Editar no inventário</button>` : ""}
    </div>
  `;
  $("machine-form").dataset.mode = "assign";
  $("machine-form").dataset.machineId = machine?.id || "";
  $("machine-form").dataset.bayId = bay.id;

  const search = $("machine-form").querySelector("[name=person_search]");
  const select = $("machine-form").querySelector("[name=assign_existing]");
  search.addEventListener("input", () => {
    const q = search.value.trim().toLowerCase();
    [...select.options].forEach((opt, i) => {
      if (i === 0) return;
      const hay = `${opt.dataset.name || ""} ${opt.dataset.host || ""} ${opt.textContent}`.toLowerCase();
      opt.hidden = Boolean(q) && !hay.includes(q);
    });
  });
}

function selectField(name, label, value, options, extra = "") {
  const current = value || "";
  const opts = options.map((item) => {
    const v = typeof item === "string" ? item : item.value;
    const t = typeof item === "string" ? (item || "—") : item.label;
    return `<option value="${escapeAttr(v)}" ${current === v ? "selected" : ""}>${escapeAttr(t)}</option>`;
  }).join("");
  return `<div class="field ${extra}"><label>${label}<select name="${name}">${opts}</select></label></div>`;
}

function secretField(name, label, value) {
  return `<div class="field full"><label>${label}
    <div class="secret">
      <input type="password" name="${name}" value="${escapeAttr(value || "")}" autocomplete="off" />
      <button type="button" class="btn ghost reveal">Mostrar</button>
      <button type="button" class="btn ghost copy">Copiar</button>
    </div>
  </label></div>`;
}

function renderForm({ bay, machine, creating }) {
  const m = machine || {};
  const areaValue = (m.area || "").toUpperCase();
  const areaOptions = ["", ...AREAS];
  if (areaValue && !AREAS.includes(areaValue)) areaOptions.push(areaValue);
  const bayOptions = [
    { value: "", label: "Sem baia" },
    ...state.bays.map((b) => {
      const count = bayMachines(b).length;
      const extra = count ? ` · ${count} PC${count > 1 ? "s" : ""}` : "";
      return { value: b.id, label: `${b.id}${extra}` };
    }),
  ];
  const selectedBay = creating && bay ? bay.id : (m.bay_id || "");

  $("machine-form").innerHTML = `
    <div class="grid">
      ${field("hostname", "Hostname", m.hostname || "", "", { required: true })}
      ${field("ip", "IP", m.ip)}
      ${field("user_name", "Usuário", m.user_name)}
      ${selectField("area", "Área", areaValue, areaOptions)}
      ${field("email", "E-mail", m.email, "full")}
      ${field("ramal", "Ramal", m.ramal)}
      ${field("numero_ind", "Telefone", m.numero_ind)}
      <p class="hint">O ramal e o telefone ficam na máquina. Se trocar o colaborador, os dois permanecem e você pode editar.</p>
      ${selectField("bay_id", "Baia", selectedBay, bayOptions, "full")}
    </div>

    <p class="group-title">Licenças</p>
    <div class="grid">
      ${secretField("win_product_key", "Chave Windows", m.win_product_key)}
      ${secretField("office_key", "Chave Office", m.office_key)}
      ${selectField("win_status", "Windows", m.win_status || "ATIVO", WIN_STATUS)}
      ${selectField("office_status", "Office", m.office_status || "PENDENTE", OFFICE_STATUS)}
      ${selectField("bitlocker_status", "Bitdefender", m.bitlocker_status || "", BITLOCKER_STATUS.map((v) => ({ value: v, label: v || "—" })), "full")}
    </div>

    <details class="more">
      <summary>Mais detalhes</summary>
      <div class="grid">
        ${field("win_product_id", "Product ID", m.win_product_id)}
        ${field("win_versao", "Versão Windows", m.win_versao)}
        ${field("ultima_atualizacao", "Última atualização", m.ultima_atualizacao)}
        ${field("politica", "Política", m.politica)}
        ${field("script", "Script", m.script, "full")}
        <div class="field full">
          <label class="check-row"><input type="checkbox" name="gpu" ${m.gpu ? "checked" : ""} /> Tem GPU</label>
        </div>
        <div class="field full"><label>Observação<textarea name="observacao">${escapeAttr(m.observacao || "")}</textarea></label></div>
        ${bay ? `<div class="field full"><label>Nota da baia<textarea name="bay_notes">${escapeAttr(bay.notes || "")}</textarea></label></div>` : ""}
      </div>
    </details>

    <div class="form-actions">
      <button type="submit" class="btn primary">Salvar</button>
      ${machine ? `<button type="button" class="btn" id="btn-unassign">Tirar da baia</button>
                   <button type="button" class="btn danger" id="btn-delete">Excluir</button>` : ""}
    </div>
  `;

  $("machine-form").dataset.mode = creating ? "create" : (machine ? "edit" : "create");
  $("machine-form").dataset.machineId = machine?.id || "";
  $("machine-form").dataset.bayId = bay?.id || "";

  const userInput = $("machine-form").querySelector("[name=user_name]");
  const emailInput = $("machine-form").querySelector("[name=email]");
  userInput?.addEventListener("blur", () => {
    if (!emailInput || emailInput.value.trim() || !userInput.value.trim()) return;
    const parts = userInput.value
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replace(/\p{Diacritic}/gu, "")
      .split(/\s+/);
    if (parts.length < 2) return;
    emailInput.value = `${parts[0]}.${parts.at(-1)}@rednegocios.com.br`;
  });
}

function formPayload(form) {
  const data = new FormData(form);
  const get = (name) => (data.get(name) || "").toString();
  return {
    hostname: get("hostname").trim(),
    ip: get("ip"),
    observacao: get("observacao"),
    gpu: Boolean(form.querySelector("input[name=gpu]")?.checked),
    script: get("script"),
    user_name: get("user_name"),
    area: get("area").toUpperCase(),
    email: get("email"),
    ramal: get("ramal"),
    numero_ind: get("numero_ind"),
    win_product_key: get("win_product_key"),
    win_product_id: get("win_product_id"),
    win_versao: get("win_versao"),
    win_status: get("win_status"),
    office_key: get("office_key"),
    office_status: get("office_status"),
    ultima_atualizacao: get("ultima_atualizacao"),
    politica: get("politica"),
    bitlocker_status: get("bitlocker_status"),
    bay_id: get("bay_id") || null,
    bay_notes: get("bay_notes"),
  };
}

function bindEvents() {
  const closeSyscallManual = () => {
    const root = $("syscall-manual");
    if (root) root.hidden = true;
  };
  $("syscall-manual-ok")?.addEventListener("click", closeSyscallManual);
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeSyscallManual();
  });

  document.querySelectorAll(".view-toggle button").forEach((btn) => {
    btn.addEventListener("click", () => setView(btn.dataset.view));
  });

  $("search").addEventListener("input", (e) => {
    state.search = e.target.value;
    renderFloorplan();
    renderInventory();
    renderGotoUsers();
    renderGotoHistory();
  });

  $("map-rooms")?.addEventListener("change", (e) => {
    state.map.rooms = e.target.checked;
    saveMapOptions();
    renderFloorplan();
  });
  $("map-empty")?.addEventListener("change", (e) => {
    state.map.empty = e.target.checked;
    saveMapOptions();
    renderFloorplan();
  });
  $("map-print")?.addEventListener("click", () => {
    setView("map");
    window.print();
  });

  document.body.addEventListener("click", async (e) => {
    const chip = e.target.closest(".chip");
    if (chip?.dataset.area) {
      state.areaFilter = chip.dataset.area;
      render();
      return;
    }
    const occupancy = e.target.closest("[data-map-occupancy]");
    if (occupancy) {
      state.map.occupancy = occupancy.dataset.mapOccupancy;
      saveMapOptions();
      renderMapControls();
      renderFloorplan();
      return;
    }
    const labelMode = e.target.closest("[data-map-label]");
    if (labelMode) {
      state.map.label = labelMode.dataset.mapLabel;
      saveMapOptions();
      renderMapControls();
      renderFloorplan();
      return;
    }
    const zoomBtn = e.target.closest("[data-map-zoom]");
    if (zoomBtn) {
      const next = state.map.zoom + (zoomBtn.dataset.mapZoom === "+" ? 0.1 : -0.1);
      state.map.zoom = Math.min(1.7, Math.max(0.7, Math.round(next * 10) / 10));
      saveMapOptions();
      applyMapZoom();
      return;
    }
    const bayEl = e.target.closest(".bay");
    if (bayEl) {
      const bay = state.bays.find((b) => b.id === bayEl.dataset.bay);
      const here = bayMachines(bay);
      openDrawer({ bay, machine: here[0] || null });
      return;
    }
    const pick = e.target.closest("[data-pick-machine]");
    if (pick) {
      const machine = state.machines.find((m) => String(m.id) === pick.dataset.pickMachine);
      const bay = state.bays.find((b) => b.id === machine?.bay_id) || state.bays.find((b) => b.id === $("machine-form").dataset.bayId) || null;
      openDrawer({ bay, machine });
      return;
    }
    const addOnBay = e.target.closest("[data-new-on-bay]");
    if (addOnBay) {
      setView("inventory");
      const bay = state.bays.find((b) => b.id === addOnBay.dataset.newOnBay);
      openDrawer({ bay, creating: true });
      return;
    }
    const row = e.target.closest("tbody tr[data-machine]");
    if (row) {
      const machine = state.machines.find((m) => String(m.id) === row.dataset.machine);
      const bay = state.bays.find((b) => b.id === machine?.bay_id) || null;
      openDrawer({ bay, machine });
    }
  });

  $("drawer-close").addEventListener("click", closeDrawer);
  $("drawer").addEventListener("click", (e) => { if (e.target.id === "drawer") closeDrawer(); });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeDrawer();
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "s" && $("drawer").classList.contains("open")) {
      if ($("machine-form").dataset.mode === "assign") return;
      e.preventDefault();
      $("machine-form").requestSubmit();
    }
  });

  $("btn-new").addEventListener("click", () => openDrawer({ creating: true }));
  $("btn-export").addEventListener("click", () => { window.location.href = LAYOUT_BASE + "/api/export.csv"; });
  $("import-file").addEventListener("change", async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    const body = new FormData();
    body.append("file", file);
    const res = await fetch(LAYOUT_BASE + "/api/import.csv", { method: "POST", body });
    const data = await res.json();
    if (!res.ok) return toast(data.detail || "Falha ao importar");
    toast(`Importado: ${data.created} novas, ${data.updated} atualizadas`);
    e.target.value = "";
    await loadAll();
  });

  $("machine-form").addEventListener("click", async (e) => {
    const form = $("machine-form");
    if (e.target.classList.contains("reveal")) {
      const input = e.target.parentElement.querySelector("input");
      const hide = input.type === "text";
      input.type = hide ? "password" : "text";
      e.target.textContent = hide ? "Mostrar" : "Ocultar";
      return;
    }
    if (e.target.classList.contains("copy")) {
      const input = e.target.parentElement.querySelector("input");
      await navigator.clipboard.writeText(input.value || "");
      toast("Copiado");
      return;
    }
    if (e.target.id === "btn-assign") {
      const id = form.assign_existing?.value;
      if (!id) return toast("Selecione o colaborador pelo nome");
      await api(`/api/bays/${form.dataset.bayId}/assign`, {
        method: "PUT",
        body: JSON.stringify({ machine_id: Number(id) }),
      });
      toast("Colaborador alocado");
      await loadAll();
      const bay = state.bays.find((b) => b.id === form.dataset.bayId);
      const machine = state.machines.find((m) => String(m.id) === String(id));
      openDrawer({ bay, machine });
      return;
    }
    if (e.target.id === "btn-edit-inventory") {
      const machineId = form.dataset.machineId;
      const machine = state.machines.find((m) => String(m.id) === String(machineId));
      const bay = state.bays.find((b) => b.id === machine?.bay_id) || null;
      setView("inventory");
      openDrawer({ bay, machine });
      return;
    }
    if (e.target.id === "btn-unassign") {
      const machineId = Number(form.dataset.machineId);
      const bayId = form.dataset.bayId;
      await api(`/api/machines/${machineId}`, {
        method: "PUT",
        body: JSON.stringify({ bay_id: null }),
      });
      toast("Máquina retirada da baia");
      await loadAll();
      const bay = state.bays.find((b) => b.id === bayId);
      const here = bayMachines(bay);
      if (here.length) openDrawer({ bay, machine: here[0] });
      else closeDrawer();
      return;
    }
    if (e.target.id === "btn-delete") {
      if (!confirm("Excluir esta máquina do inventário? Se houver usuário GoTo vinculado, ele será suspenso (não excluído) na GoTo. Se existir no SysCall, o operador é apagado.")) return;
      await api(`/api/machines/${form.dataset.machineId}`, { method: "DELETE" });
      toast("Máquina excluída");
      await loadAll();
      closeDrawer();
    }
  });

  $("machine-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const form = e.currentTarget;
    if (form.dataset.mode === "assign") return;
    const payload = formPayload(form);
    try {
      if (!payload.hostname) return toast("Informe o hostname");
      if (form.dataset.mode === "create") {
        if (form.dataset.bayId && !payload.bay_id) payload.bay_id = form.dataset.bayId;
        await api("/api/machines", { method: "POST", body: JSON.stringify(payload) });
        toast("Salvo");
      } else {
        await api(`/api/machines/${form.dataset.machineId}`, {
          method: "PUT",
          body: JSON.stringify(payload),
        });
        toast("Salvo");
      }
      if (form.dataset.bayId && payload.bay_notes !== undefined) {
        await api(`/api/bays/${form.dataset.bayId}/notes`, {
          method: "PUT",
          body: JSON.stringify({ notes: payload.bay_notes || "" }),
        });
      }
      await loadAll();
      closeDrawer();
    } catch (err) {
      toast(err.message);
    }
  });

  $("goto-enable").addEventListener("click", () => {
    setGotoEnabled(!isGotoEnabled());
  });
  $("goto-area").addEventListener("change", () => {
    const area = selectedArea();
    setGotoEnabled(area === "COMPRAS");
  });
  $("goto-machine").addEventListener("change", updateGotoBayHint);
  $("goto-refresh").addEventListener("click", () => {
    loadPlataformaStatus();
    loadMondayStatus();
    loadSyscallStatus();
    loadGotoStatus();
    loadGotoPhones().then(() => loadGotoUsers());
  });
  $("goto-phones-refresh").addEventListener("click", async () => {
    const btn = $("goto-phones-refresh");
    btn.disabled = true;
    toast("Atualizando lista telefônica na GoTo…", "info");
    try {
      const data = await api("/api/goto/phones/refresh", { method: "POST" });
      state.gotoPhones = data.phones || [];
      renderGotoForm();
      renderGotoUsers();
      toast(`Lista telefônica atualizada: ${data.count} números`, "ok");
    } catch (err) {
      toast(err.message, "err");
    } finally {
      btn.disabled = false;
    }
  });
  let emailLookupTimer = null;
  const emailInput = $("goto-form").email;
  emailInput.addEventListener("blur", () => lookupGotoEmail(emailInput.value.trim()));
  emailInput.addEventListener("input", () => {
    clearTimeout(emailLookupTimer);
    emailLookupTimer = setTimeout(() => lookupGotoEmail(emailInput.value.trim()), 700);
  });
  $("goto-users-body").addEventListener("click", async (e) => {
    const btn = e.target.closest("[data-suspend-user]");
    if (!btn || btn.disabled) return;
    if (!confirm("Excluir este usuário? Os dados dele saem do inventário. Se a máquina estiver numa baia, ela permanece lá como Ocioso (alocada, porém disponível). A GoTo suspende a conta, sem apagar. Se existir no SysCall, o operador é apagado.")) return;
    btn.disabled = true;
    toast("Excluindo usuário na GoTo e no SysCall…", "info");
    try {
      const out = await api(`/api/goto/users/${encodeURIComponent(btn.dataset.suspendUser)}/suspend`, { method: "POST" });
      const syscall = out.syscall_deleted
        ? ` Operador ${out.syscall_username || ""} removido no SysCall.`
        : (out.syscall_warning || "");
      const gotoNote = out.goto_warning ? ` ${out.goto_warning}` : "";
      toast(`Usuário excluído. Dados removidos do inventário.${syscall}${gotoNote} Se havia baia, a máquina ficou Ociosa. Remova o plano de discagem no portal.`.replace(/\s+/g, " ").trim(), "ok");
      await loadAll();
      await loadGotoUsers();
      await loadGotoHistory();
    } catch (err) {
      btn.disabled = false;
      toast(err.message, "err");
    }
  });
  $("goto-users-body").addEventListener("change", async (e) => {
    const phoneInput = e.target.closest("[data-phone-user]");
    if (phoneInput) {
      const userKey = phoneInput.dataset.phoneUser;
      const phone = phoneInput.value.trim();
      toast(phone ? "Configurando identificador na GoTo…" : "Removendo número na GoTo…", "info");
      try {
        await api(`/api/goto/users/${encodeURIComponent(userKey)}/contact`, {
          method: "PUT",
          body: JSON.stringify({
            phone,
            email: phoneInput.dataset.email || "",
            first_name: phoneInput.dataset.first || "",
            last_name: phoneInput.dataset.last || "",
          }),
        });
        const row = (state.gotoUsers || []).find((user) => user.user_key === userKey);
        if (row) row.phone = phone;
        await loadGotoPhones();
        renderGotoUsers();
        toast(phone ? "Número configurado na GoTo: identificador e encaminhamento para o ramal." : "Número removido na GoTo", "ok");
      } catch (err) {
        toast(err.message, "err");
        await loadGotoPhones();
        await loadGotoUsers();
      }
      return;
    }
    const select = e.target.closest("select[data-link-user]");
    if (!select) return;
    const machineId = select.value ? Number(select.value) : null;
    try {
      await api(`/api/goto/users/${encodeURIComponent(select.dataset.linkUser)}/link`, {
        method: "PUT",
        body: JSON.stringify({
          machine_id: machineId,
          email: select.dataset.email || "",
          first_name: select.dataset.first || "",
          last_name: select.dataset.last || "",
        }),
      });
      toast(machineId ? "Usuário vinculado à máquina" : "Vínculo removido");
      await loadAll();
      await loadGotoUsers();
    } catch (err) {
      toast(err.message);
      await loadGotoUsers();
    }
  });

  $("goto-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const form = e.currentTarget;
    const submit = $("goto-submit");
    const area = selectedArea();
    const payload = {
      first_name: form.first_name.value.trim(),
      last_name: form.last_name.value.trim(),
      email: form.email.value.trim(),
      area,
      enable_goto: isGotoEnabled(),
      phone: isGotoEnabled() ? form.phone.value.trim() : "",
      send_welcome: isGotoEnabled() ? Boolean(form.send_welcome.checked) : false,
      machine_id: form.machine_id.value ? Number(form.machine_id.value) : null,
    };
    if (!payload.first_name || !payload.last_name) {
      toast("Preencha nome e sobrenome.", "info");
      setGotoResult("Nome e sobrenome são obrigatórios.", "info");
      return;
    }
    if (!area) {
      toast("Escolha o tipo do usuário.", "info");
      setGotoResult("Marque Comprador, Jurídico ou Calculista.", "info");
      return;
    }
    submit.disabled = true;
    submit.textContent = "Enviando…";
    const columnName = area === "COMPRAS"
      ? "Comprador (SISTEMA, SysCall - Inclusões e INCLUSÕES)"
      : area === "JURIDICO"
        ? "Responsável Jurídico"
        : "Calculista";
    const teamName = area === "COMPRAS" ? "COMPRAS" : area === "JURIDICO" ? "JURÍDICO" : "CÁLCULO";
    const mondayMsg = `Incluindo na Plataforma Central e enviando convite Monday (${teamName}) com rótulo na coluna ${columnName}…`;
    setGotoResult(payload.enable_goto ? `${mondayMsg} Depois a GoTo.` : mondayMsg, "info");
    toast(
      payload.enable_goto
        ? "Enviando para Plataforma, Monday e GoTo…"
        : "Enviando para Plataforma Central e Monday…",
      "info",
    );
    try {
      const created = await api("/api/goto/users", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      const bits = [];
      if (created.plataforma_existed && created.plataforma_username) {
        bits.push(`já existia na Plataforma Central (${created.plataforma_username})`);
      } else if (created.plataforma_created && created.plataforma_username) {
        const senha = created.plataforma_password ? ` senha ${created.plataforma_password}` : "";
        bits.push(`Plataforma Central: login ${created.plataforma_username}${senha}`);
      }
      if (created.monday_existed) bits.push("já existia na Monday");
      else if (created.monday_invited) bits.push("convite Monday enviado (membro, Work Management)");
      if (created.monday_team) bits.push(`equipe ${created.monday_team}`);
      if (created.monday_label_created) bits.push(`rótulo ${created.monday_column || columnName}: ${created.monday_label}`);
      else if (created.monday_label) bits.push(`rótulo ${created.monday_column || columnName} já existia: ${created.monday_label}`);
      if (created.syscall_existed && created.syscall_username) {
        bits.push(`já existia no SysCall (${created.syscall_username})`);
      } else if (created.syscall_created && created.syscall_username) {
        const senha = created.syscall_password ? ` senha ${created.syscall_password}` : "";
        bits.push(`SysCall: user ${created.syscall_username}${senha} (Operador / RedPre)`);
      }
      const mondayText = bits.join(". ");
      if (created.existed) {
        const msg = [mondayText, `Esse e-mail já existe na GoTo: ${created.email}`, created.monday_warning, created.plataforma_warning, created.syscall_warning, created.goto_warning].filter(Boolean).join(". ");
        toast(msg, "info");
        setGotoResult(msg, "info");
        showSyscallManual(created);
        showExistingGotoUser(created);
        const status = $("goto-email-status");
        if (status) {
          status.classList.remove("ok", "err");
          status.classList.add("warn");
          status.textContent = "Usuário já cadastrado na GoTo. Dados carregados abaixo.";
        }
      } else {
        const welcome = created.email && payload.enable_goto && payload.send_welcome && !created.goto_warning
          ? " E-mail de boas-vindas da GoTo enviado."
          : "";
        const gotoText = payload.enable_goto
          ? created.goto_warning
            ? ` Usuário ${created.email} criado na GoTo.`
            : ` Usuário ${created.email} criado na GoTo com licença GoTo Connect Standard.${welcome}`
          : "";
        const msg = [mondayText, gotoText.trim(), created.monday_warning, created.plataforma_warning, created.syscall_warning, created.goto_warning].filter(Boolean).join(". ");
        toast(msg, "ok");
        setGotoResult(msg, "ok");
        showSyscallManual(created);
        form.reset();
        if (form.send_welcome) form.send_welcome.checked = true;
        setGotoEnabled(false);
        showExistingGotoUser(null);
      }
      await loadAll();
      await loadGotoUsers();
      renderGotoForm();
    } catch (err) {
      toast(err.message, "err");
      setGotoResult(err.message, "err");
    } finally {
      submit.disabled = false;
      submit.textContent = "Criar usuário";
    }
  });
}

bindEvents();
loadAll().catch((err) => toast(err.message));
