/* global Telegram */
const API_BASE_URL = "https://api.astrabotai.online";
const API = API_BASE_URL.replace(/\/+$/, "");
const tg = window.Telegram?.WebApp || null;
const balanceEl = document.getElementById("balanceValue");
const packsEl = document.getElementById("packs");
const buyBtn = document.getElementById("buyBtn");
const refLinkEl = document.getElementById("refLink");
const copyBtn = document.getElementById("copyBtn");
const errorBox = document.getElementById("errorBox");
const supportLink = document.getElementById("supportLink");
const diagTg = document.getElementById("diagTg");
const diagUser = document.getElementById("diagUser");
const diagErr = document.getElementById("diagErr");
let sessionToken = null;

function setDiag() {
  const tgAvailable = !!tg;
  const userId = tg?.initDataUnsafe?.user?.id || "—";
  diagTg.textContent = `tg available: ${tgAvailable ? "yes" : "no"}`;
  diagUser.textContent = `user id: ${userId}`;
}

function setError(text) {
  diagErr.textContent = `error: ${text || "—"}`;
}

function showError() {
  errorBox.classList.remove("hidden");
}

function renderPacks(packs) {
  packsEl.innerHTML = "";
  packs.forEach((p) => {
    const row = document.createElement("div");
    row.className = "pack";
    row.innerHTML = `<div>${p.spreads} раскладов</div><div>${p.stars} ⭐</div>`;
    const btn = document.createElement("button");
    btn.className = "buy";
    btn.textContent = "Купить";
    btn.addEventListener("click", () => {
      if (tg) {
        createInvoice(p.key);
      }
    });
    row.appendChild(btn);
    packsEl.appendChild(row);
  });
}

async function refreshBalance() {
  if (!sessionToken) return;
  try {
    const resp = await fetch(`${API}/api/balance`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session: sessionToken }),
    });
    const data = await resp.json();
    if (data.ok) {
      balanceEl.textContent = `${data.balance} раскладов`;
    }
  } catch (e) {
    setError(`balance refresh failed: ${e?.message || e}`);
  }
}

async function createInvoice(packKey) {
  if (!sessionToken) {
    setError("no session");
    showError();
    return;
  }
  try {
    const resp = await fetch(`${API}/api/invoice`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session: sessionToken, pack_key: packKey }),
    });
    const data = await resp.json();
    if (!data.ok || !data.invoice_link) {
      setError(data.message || "invoice failed");
      showError();
      return;
    }
    if (!tg.openInvoice) {
      tg.openTelegramLink(data.invoice_link);
      return;
    }
    try {
      const open = tg.openInvoice(data.invoice_link, (status) => {
        if (status === "paid") {
          refreshBalance();
        } else if (status === "cancelled" || status === "failed") {
          setError(`invoice ${status}`);
          showError();
        }
      });
      if (open && typeof open.then === "function") {
        open.then((status) => {
          if (status === "paid") {
            refreshBalance();
          } else if (status === "cancelled" || status === "failed") {
            setError(`invoice ${status}`);
            showError();
          }
        });
      }
    } catch (e) {
      setError(`openInvoice failed: ${e?.message || e}`);
      showError();
      tg.openTelegramLink(data.invoice_link);
    }
  } catch (e) {
    setError(`invoice error: ${e?.message || e}`);
    showError();
  }
}

async function auth() {
  setDiag();
  if (!tg || !tg.initData) {
    showError();
    return;
  }

  try {
    tg.ready();
    tg.expand();
  } catch (e) {
    setError(`tg init failed: ${e?.message || e}`);
  }

  let resp;
  try {
    resp = await fetch(`${API_BASE_URL}/api/auth`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ initData: tg.initData }),
    });
  } catch (e) {
    setError(`fetch auth failed: ${e?.message || e}`);
    showError();
    return;
  }

  let data;
  let rawText = "";
  try {
    rawText = await resp.text();
    data = JSON.parse(rawText);
  } catch (e) {
    const status = resp?.status || "unknown";
    const preview = rawText ? rawText.slice(0, 200) : "empty";
    setError(`bad json (status ${status}): ${e?.message || e}; preview: ${preview}`);
    showError();
    return;
  }

  if (!data.ok) {
    const msg = data.message ? ` (${data.message})` : "";
    setError(`${data.error || "auth_failed"}${msg}`);
    showError();
    return;
  }

  sessionToken = data.session || null;
  if (sessionToken) {
    try {
      localStorage.setItem("astra_session", sessionToken);
    } catch (e) {
      /* ignore */
    }
  }
  balanceEl.textContent = `${data.balance} раскладов`;
  refLinkEl.value = data.ref_link || "—";
  renderPacks(data.packages || []);
  if (data.support_link) {
    supportLink.href = data.support_link;
  }
}

buyBtn.addEventListener("click", () => {
  packsEl.classList.toggle("hidden");
});

copyBtn.addEventListener("click", async () => {
  const val = refLinkEl.value;
  if (!val || val === "—") return;
  try {
    await navigator.clipboard.writeText(val);
    copyBtn.textContent = "Скопировано";
    setTimeout(() => (copyBtn.textContent = "Скопировать"), 1200);
  } catch (e) {
    /* ignore */
  }
});

supportLink.addEventListener("click", (e) => {
  e.preventDefault();
  const href = supportLink.getAttribute("href") || "";
  if (tg && href) {
    tg.openTelegramLink(href);
  }
});

auth();
