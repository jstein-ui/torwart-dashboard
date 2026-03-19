"""
╔══════════════════════════════════════════════════════════════╗
║  Torwarthandschuh-Dashboard  |  Google Sheets + Streamlit   ║
╚══════════════════════════════════════════════════════════════╝
Einmalig installieren:
  pip install streamlit plotly pandas gspread google-auth reportlab openpyxl

Starten:
  streamlit run torwart_dashboard.py
"""

import io
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import gspread
from google.oauth2.service_account import Credentials
from datetime import date
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas as rl_canvas
from reportlab.lib.colors import HexColor
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import os

# ═══════════════════════════════════════════════════════════════
# KONFIGURATION
# ═══════════════════════════════════════════════════════════════
SHEET_NAME       = "Torwarthandschuhe"
CREDENTIALS_FILE = "credentials.json"
WORKSHEET_NAME   = "Torwarthandschuhe"

COL_PERSON     = "Empfänger / Person"
COL_ARTIKEL    = "Artikelbezeichnung"
COL_GROESSE    = "Grösse"
COL_EP_BRUTTO  = "Einzelpreis Brutto"
COL_RAB_PCT    = "Rabatt Prozent"
COL_RAB_EUR    = "Rabatt Betrag"
COL_NETTO      = "Betrag Netto"
COL_RAB_VEREIN = "Rabatt Verein"
COL_ZAHLBETRAG = "Zu zahlender Betrag"
COL_ERHALTEN   = "Erhalten"
COL_BEZAHLT    = "Bezahlt Status"
COL_BEZAHLT_SHEET_COL = 12  # Spalte L im Sheet (1-basiert)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ═══════════════════════════════════════════════════════════════
# SCHRIFTART FÜR PDF (DejaVu falls vorhanden, sonst Helvetica)
# ═══════════════════════════════════════════════════════════════
PDF_FONT      = "Helvetica"
PDF_FONT_BOLD = "Helvetica-Bold"
_dejavu = "C:/Windows/Fonts/DejaVuSans.ttf"
if os.path.exists(_dejavu):
    try:
        pdfmetrics.registerFont(TTFont("DVS",  _dejavu))
        pdfmetrics.registerFont(TTFont("DVSB", _dejavu.replace("DejaVuSans", "DejaVuSans-Bold")))
        PDF_FONT      = "DVS"
        PDF_FONT_BOLD = "DVSB"
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════
# HILFSFUNKTIONEN
# ═══════════════════════════════════════════════════════════════
def fmt(wert):
    s = f"{abs(wert):,.2f} €".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"-{s}" if wert < 0 else s

def fmt_pct(wert):
    return f"{wert:.2f} %".replace(".", ",")

def fmt_groesse(val):
    if val % 1 != 0:
        return str(val).replace(".", ",")
    return str(int(val))

def clean_euro(series):
    return (series.astype(str)
            .str.replace("€", "", regex=False)
            .str.replace("\xa0", "", regex=False)
            .str.strip()
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0))

def clean_groesse(series):
    return (series.astype(str)
            .str.replace("€", "", regex=False)
            .str.replace("\xa0", "", regex=False)
            .str.strip()
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0))

def clean_pct(series):
    return (series.astype(str)
            .str.replace("%", "", regex=False)
            .str.replace("\xa0", "", regex=False)
            .str.strip()
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0))


# ═══════════════════════════════════════════════════════════════
# WHATSAPP
# ═══════════════════════════════════════════════════════════════
CHRISTOPHER_NR = ""   # Nummer von Christopher hier eintragen z.B. "4917612345678"
LIEFERADRESSE  = ""   # Deine Lieferadresse hier eintragen

def make_whatsapp_url(bestellungen: list, rg_name: str, rg_strasse: str,
                       rg_plzort: str, rg_email: str = "") -> str:
    """
    Erstellt einen wa.me-Link mit vorausgefüllter Nachricht an den Lieferanten.
    bestellungen = [{"artikel": ..., "groesse": ...}, ...]
    """
    import urllib.parse
    zeilen = ["Moin, ich hätte wieder eine kleine Bestellung."]
    for b in bestellungen:
        gr = fmt_groesse(float(b["groesse"]))
        zeilen.append(f"{b['artikel']} GR. {gr}")
    zeilen.append("")
    zeilen.append("Rechnungsadresse:")
    zeilen.append(rg_name)
    zeilen.append(rg_strasse)
    zeilen.append(rg_plzort)
    if rg_email:
        zeilen.append(rg_email)
    zeilen.append("")
    zeilen.append("Lieferadresse:")
    zeilen.append(LIEFERADRESSE)
    nachricht = "\n".join(zeilen)
    encoded   = urllib.parse.quote(nachricht)
    return f"https://wa.me/{CHRISTOPHER_NR}?text={encoded}"

# ═══════════════════════════════════════════════════════════════
# GOOGLE SHEETS
# ═══════════════════════════════════════════════════════════════
@st.cache_resource
def get_worksheet():
    import json, os
    if "gcp_service_account" in st.secrets:
        # Streamlit Cloud — einzelne TOML-Felder direkt als Dict übergeben
        info = {k: v for k, v in st.secrets["gcp_service_account"].items()}
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    elif os.path.exists(CREDENTIALS_FILE):
        # Lokaler PC — Datei lesen
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    else:
        raise FileNotFoundError(
            "Keine Credentials gefunden. Bitte entweder:\n"
            "1. credentials.json im App-Ordner ablegen (lokal), oder\n"
            "2. Streamlit Secrets CREDENTIALS_JSON konfigurieren (Cloud)."
        )
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)

@st.cache_data(ttl=30)
def load_data() -> pd.DataFrame:
    ws         = get_worksheet()
    alle_werte = ws.get_all_values()
    if len(alle_werte) < 2:
        return pd.DataFrame()
    headers = alle_werte[0]
    df      = pd.DataFrame(alle_werte[1:], columns=headers)
    df      = df[df.apply(lambda row: any(str(v).strip() != "" for v in row), axis=1)].copy()
    df["_row_idx"] = range(2, len(df) + 2)  # Google Sheets Zeilennummer (1-basiert + Header)

    for col in [COL_EP_BRUTTO, COL_RAB_EUR, COL_NETTO, COL_RAB_VEREIN, COL_ZAHLBETRAG]:
        if col in df.columns:
            df[col] = clean_euro(df[col])
    if COL_RAB_PCT in df.columns:
        df[COL_RAB_PCT] = clean_pct(df[COL_RAB_PCT])
    if COL_GROESSE in df.columns:
        df[COL_GROESSE] = clean_groesse(df[COL_GROESSE])
    df = df[df[COL_PERSON].astype(str).str.strip() != ""]
    return df

def update_bezahlt(row_idx: int, neuer_status: str):
    """Bezahlt-Status einer Zeile direkt im Sheet aktualisieren."""
    ws = get_worksheet()
    ws.update_cell(row_idx, COL_BEZAHLT_SHEET_COL, neuer_status)
    load_data.clear()

def save_row(person, artikel, groesse, ep_brutto, rab_pct, rab_verein, bezahlt, erhalten):
    ws         = get_worksheet()
    rab_eur    = round(ep_brutto * rab_pct / 100, 2)
    netto      = round(ep_brutto - rab_eur, 2)
    zahlbetrag = round(max(0, netto - rab_verein), 2)
    alle       = ws.get_all_values()
    ws.append_row([
        len(alle), person, artikel,
        str(groesse).replace(".", ","),
        str(ep_brutto).replace(".", ",") + " €",
        str(round(rab_pct, 2)).replace(".", ",") + " %",
        str(rab_eur).replace(".", ",") + " €",
        str(netto).replace(".", ",") + " €",
        str(rab_verein).replace(".", ",") + " €",
        str(zahlbetrag).replace(".", ",") + " €",
        str(erhalten) if erhalten else "",
        "Bezahlt" if bezahlt else "Offen",
    ])
    load_data.clear()

# ═══════════════════════════════════════════════════════════════
# EXCEL EXPORT
# ═══════════════════════════════════════════════════════════════
def make_excel(df_export: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_export.to_excel(writer, index=False, sheet_name="Bestellungen")
    return buf.getvalue()

# ═══════════════════════════════════════════════════════════════
# QUITTUNGS-PDF
# ═══════════════════════════════════════════════════════════════
def make_quittung(person: str, df_p: pd.DataFrame, vereinszuschuss: int) -> bytes:
    buf  = io.BytesIO()
    W, H = A4
    c    = rl_canvas.Canvas(buf, pagesize=A4)

    BLAU  = HexColor("#1f4e79")
    GRUEN = HexColor("#3FB950")
    GRAU  = HexColor("#666666")
    HELL  = HexColor("#f0f4f8")

    # Header
    c.setFillColor(BLAU)
    c.rect(0, H-35*mm, W, 35*mm, fill=1, stroke=0)
    c.setFillColor(HexColor("#ffffff"))
    c.setFont(PDF_FONT_BOLD, 20)
    c.drawString(20*mm, H-18*mm, "Quittung — Torwarthandschuh-Bestellung")
    c.setFont(PDF_FONT, 10)
    c.drawString(20*mm, H-28*mm, f"Erstellt am: {date.today().strftime('%d.%m.%Y')}")

    # Person
    c.setFillColor(BLAU)
    c.setFont(PDF_FONT_BOLD, 14)
    c.drawString(20*mm, H-50*mm, f"Empfänger: {person}")

    # Trennlinie
    c.setStrokeColor(BLAU)
    c.setLineWidth(0.8)
    c.line(20*mm, H-55*mm, W-20*mm, H-55*mm)

    # Tabellenkopf
    y = H - 65*mm
    c.setFillColor(HELL)
    c.rect(20*mm, y-5*mm, W-40*mm, 10*mm, fill=1, stroke=0)
    c.setFillColor(BLAU)
    c.setFont(PDF_FONT_BOLD, 9)
    cols_x = [20*mm, 80*mm, 105*mm, 125*mm, 148*mm, 168*mm]
    headers = ["Artikel", "Größe", "Brutto", "Rabatt", "Netto", "Zu zahlen"]
    for x, h in zip(cols_x, headers):
        c.drawString(x+1*mm, y, h)

    # Zeilen
    c.setFont(PDF_FONT, 9)
    y -= 12*mm
    for _, row in df_p.iterrows():
        c.setFillColor(HexColor("#1a1a2e"))
        c.drawString(cols_x[0]+1*mm, y, str(row[COL_ARTIKEL])[:28])
        c.drawString(cols_x[1]+1*mm, y, fmt_groesse(float(row[COL_GROESSE])))
        c.drawRightString(cols_x[2]+18*mm, y, fmt(float(row[COL_EP_BRUTTO])))
        c.drawRightString(cols_x[3]+18*mm, y, fmt_pct(float(row[COL_RAB_PCT])))
        c.drawRightString(cols_x[4]+18*mm, y, fmt(float(row[COL_NETTO])))
        c.drawRightString(cols_x[5]+20*mm, y, fmt(float(row[COL_ZAHLBETRAG])))
        c.setStrokeColor(HexColor("#eeeeee"))
        c.setLineWidth(0.3)
        c.line(20*mm, y-2*mm, W-20*mm, y-2*mm)
        y -= 8*mm

    # Summenzeile
    y -= 5*mm
    c.setStrokeColor(BLAU); c.setLineWidth(0.8)
    c.line(20*mm, y+4*mm, W-20*mm, y+4*mm)

    brutto_p  = df_p[COL_EP_BRUTTO].sum()
    netto_p   = df_p[COL_NETTO].sum()
    zahl_p    = df_p[COL_ZAHLBETRAG].sum()
    ersparnis = brutto_p - netto_p

    c.setFont(PDF_FONT_BOLD, 10)
    c.setFillColor(BLAU)
    c.drawString(20*mm, y-4*mm,  "Gesamtbetrag (Brutto):")
    c.drawString(20*mm, y-12*mm, "Ersparnis (Lieferantenrabatt):")
    c.drawString(20*mm, y-20*mm, f"Vereinszuschuss ({vereinszuschuss} €/Position):")

    c.setFont(PDF_FONT, 10)
    c.setFillColor(HexColor("#1a1a2e"))
    c.drawRightString(W-20*mm, y-4*mm,  fmt(brutto_p))
    c.drawRightString(W-20*mm, y-12*mm, f"- {fmt(ersparnis)}")
    c.drawRightString(W-20*mm, y-20*mm, f"- {fmt(df_p[COL_RAB_VEREIN].sum())}")

    # Endbetrag
    y -= 32*mm
    c.setFillColor(GRUEN)
    c.rect(20*mm, y-3*mm, W-40*mm, 12*mm, fill=1, stroke=0)
    c.setFillColor(HexColor("#ffffff"))
    c.setFont(PDF_FONT_BOLD, 12)
    c.drawString(22*mm, y+3*mm, "Zu zahlender Betrag:")
    c.drawRightString(W-22*mm, y+3*mm, fmt(zahl_p))

    # Status
    y -= 20*mm
    bezahlt_status = df_p[COL_BEZAHLT].astype(str).str.lower().iloc[0]
    if bezahlt_status == "bezahlt":
        c.setFillColor(GRUEN)
        c.setFont(PDF_FONT_BOLD, 11)
        c.drawString(20*mm, y, "✓ Bezahlt")
    else:
        c.setFillColor(HexColor("#F85149"))
        c.setFont(PDF_FONT_BOLD, 11)
        c.drawString(20*mm, y, "⚠ Noch offen")

    # Footer
    c.setFillColor(GRAU)
    c.setFont(PDF_FONT, 8)
    c.drawString(20*mm, 15*mm, "Torwarthandschuh-Bestellung  |  Automatisch erstellt")
    c.drawRightString(W-20*mm, 15*mm, f"Seite 1 / 1")

    c.save()
    return buf.getvalue()

# ═══════════════════════════════════════════════════════════════
# SEITEN-KONFIGURATION & STYLING
# ═══════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Torwarthandschuh-Dashboard",
    page_icon="🧤",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; }
    h2, h3 { color: #1f4e79; }
</style>
""", unsafe_allow_html=True)

LAYOUT = dict(
    plot_bgcolor="#ffffff", paper_bgcolor="#f8f9fa",
    font_color="#1a1a2e", showlegend=False,
    margin=dict(t=45, b=30, l=10, r=10)
)

# ═══════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## 🧤 Navigation")
    seite = st.radio("", [
        "📊 Zentrale Übersicht",
        "📋 Operative Abrechnung",
        "💰 Finanzanalyse",
        "📦 Mengenanalyse",
        "👤 Personen-Detail & Quittung",
        "📅 Zeitstrahl",
        "➕ Neue Bestellung",
    ], label_visibility="collapsed")
    st.divider()
    st.markdown("### 🎛️ Vereinszuschuss-Simulator")
    st.caption("Aktuell in den Daten: 30 € pro Person")
    vereinszuschuss = st.slider("Simulierter Zuschuss (€)", 0, 100, 30, 5)

# ═══════════════════════════════════════════════════════════════
# DATEN LADEN
# ═══════════════════════════════════════════════════════════════
try:
    df = load_data()
except FileNotFoundError:
    st.error("⚠️ credentials.json nicht gefunden.")
    st.stop()
except Exception as e:
    st.error(f"⚠️ Google Sheets Verbindung fehlgeschlagen:\n{e}")
    st.stop()

hat_daten = not df.empty

if hat_daten:
    gesamt_brutto  = df[COL_EP_BRUTTO].sum()
    gesamt_netto   = df[COL_NETTO].sum()
    gesamt_zahl    = df[COL_ZAHLBETRAG].sum()
    ersparnis      = gesamt_brutto - gesamt_netto
    sparquote      = (ersparnis / gesamt_brutto * 100) if gesamt_brutto > 0 else 0
    offen_mask     = df[COL_BEZAHLT].astype(str).str.lower().isin(["offen", "nein", ""])
    offen_betrag   = df.loc[offen_mask, COL_ZAHLBETRAG].sum()
    anzahl_ges     = len(df)
    anzahl_zeilen  = len(df)
    sim_verbleibend = max(0, gesamt_netto - vereinszuschuss * anzahl_zeilen)
else:
    gesamt_brutto = gesamt_netto = gesamt_zahl = ersparnis = sparquote = 0
    offen_betrag = anzahl_ges = sim_verbleibend = anzahl_zeilen = 0

# ═══════════════════════════════════════════════════════════════
# ── SEITE 1: ZENTRALE ÜBERSICHT
# ═══════════════════════════════════════════════════════════════
if seite == "📊 Zentrale Übersicht":
    st.title("🧤 Torwarthandschuh-Bestellung")

    if not hat_daten:
        st.info("Noch keine Daten. Starte mit '➕ Neue Bestellung'.")
    else:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Gesamtumsatz (Brutto)", fmt(gesamt_brutto))
        k2.metric("Gesamtersparnis",       fmt(ersparnis),
                  delta=fmt_pct(sparquote))
        k3.metric("Noch offen",            fmt(offen_betrag))
        k4.metric("Teilnehmer",            f"{anzahl_ges} Personen")

        if vereinszuschuss == 30:
            st.info(f"🎛️ Vereinszuschuss: {vereinszuschuss} € pro Position "
                    f"→ Verbleibend gesamt: **{fmt(sim_verbleibend)}**")
        elif vereinszuschuss > 30:
            st.success(f"🎛️ Erhöhter Zuschuss {vereinszuschuss} € "
                       f"→ Verbleibend: **{fmt(sim_verbleibend)}**")
        else:
            st.warning(f"🎛️ Reduzierter Zuschuss {vereinszuschuss} € "
                       f"→ Verbleibend: **{fmt(sim_verbleibend)}**")

        # ── ERINNERUNGS-ÜBERSICHT ─────────────────────────────────────────────
        offen_df = df[offen_mask]
        if not offen_df.empty:
            st.divider()
            st.subheader("⚠️ Noch nicht bezahlt")
            offen_pivot = (offen_df.groupby(COL_PERSON)[COL_ZAHLBETRAG]
                           .sum().reset_index()
                           .sort_values(COL_ZAHLBETRAG, ascending=False))
            for _, row in offen_pivot.iterrows():
                col_l, col_r = st.columns([3, 1])
                col_l.warning(f"🔴 **{row[COL_PERSON]}** — {fmt(row[COL_ZAHLBETRAG])} offen")
        else:
            st.success("🎉 Alle Teilnehmer haben bezahlt!")

        st.divider()
        st.subheader("Übersicht pro Person")

        zeilen_pro_person = df.groupby(COL_PERSON).size().rename("Zeilen")
        pivot = df.groupby(COL_PERSON).agg(
            Artikel=(COL_ARTIKEL, "first"),
            Grösse=(COL_GROESSE, "first"),
            Brutto=(COL_EP_BRUTTO, "sum"),
            Netto=(COL_NETTO, "sum"),
            Zahlbetrag=(COL_ZAHLBETRAG, "sum"),
            Status=(COL_BEZAHLT, "first"),
        ).reset_index().join(zeilen_pro_person, on=COL_PERSON)

        pivot["Sim. Zahlbetrag"] = (
            pivot["Netto"] - vereinszuschuss * pivot["Zeilen"]
        ).clip(lower=0)
        pivot = pivot.drop(columns=["Zeilen"])
        pivot.columns = ["Person","Artikel","Grösse","Brutto","Netto",
                         "Zahlbetrag (aktuell)","Status","Sim. Zahlbetrag"]

        pivot["Grösse"]               = pivot["Grösse"].apply(fmt_groesse)
        pivot["Brutto"]               = pivot["Brutto"].apply(fmt)
        pivot["Netto"]                = pivot["Netto"].apply(fmt)
        pivot["Zahlbetrag (aktuell)"] = pivot["Zahlbetrag (aktuell)"].apply(fmt)
        pivot["Sim. Zahlbetrag"]      = pivot["Sim. Zahlbetrag"].apply(fmt)
        st.dataframe(pivot, use_container_width=True, hide_index=True)

        # ── EXCEL EXPORT ──────────────────────────────────────────────────────
        st.divider()
        export_df = df.drop(columns=["_row_idx"], errors="ignore").copy()
        export_df[COL_GROESSE]    = export_df[COL_GROESSE].apply(fmt_groesse)
        export_df[COL_RAB_PCT]    = export_df[COL_RAB_PCT].apply(fmt_pct)
        for col in [COL_EP_BRUTTO, COL_RAB_EUR, COL_NETTO, COL_RAB_VEREIN, COL_ZAHLBETRAG]:
            if col in export_df.columns:
                export_df[col] = export_df[col].apply(fmt)
        st.download_button(
            label="📥 Alle Daten als Excel herunterladen",
            data=make_excel(export_df),
            file_name=f"Torwarthandschuhe_{date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

# ═══════════════════════════════════════════════════════════════
# ── SEITE 2: OPERATIVE ABRECHNUNG
# ═══════════════════════════════════════════════════════════════
elif seite == "📋 Operative Abrechnung":
    st.title("📋 Operative Abrechnung")

    if not hat_daten:
        st.info("Noch keine Daten vorhanden.")
    else:
        c1, c2 = st.columns(2)
        filter_status = c1.selectbox("Bezahlt-Status", ["Alle", "Bezahlt", "Offen"])
        filter_person = c2.selectbox("Person",
            ["Alle"] + sorted(df[COL_PERSON].unique().tolist()))

        df_f = df.copy()
        if filter_status == "Bezahlt":
            df_f = df_f[df_f[COL_BEZAHLT].astype(str).str.lower() == "bezahlt"]
        elif filter_status == "Offen":
            df_f = df_f[df_f[COL_BEZAHLT].astype(str).str.lower() == "offen"]
        if filter_person != "Alle":
            df_f = df_f[df_f[COL_PERSON] == filter_person]

        k1, k2 = st.columns(2)
        k1.metric("Gefilterte Summe",  fmt(df_f[COL_ZAHLBETRAG].sum()))
        k2.metric("Anzahl Positionen", len(df_f))
        st.divider()

        # ── BEZAHLT-STATUS DIREKT ÄNDERN ──────────────────────────────────────
        st.subheader("✏️ Bezahlt-Status ändern")
        offen_personen = (df[df[COL_BEZAHLT].astype(str).str.lower() == "offen"]
                          [COL_PERSON].unique().tolist())
        if offen_personen:
            ca, cb, cc = st.columns([2, 1, 1])
            person_aendern = ca.selectbox("Person", sorted(offen_personen),
                                          key="bezahlt_person")
            with cc:
                st.write("")
                st.write("")
                if st.button("✅ Als bezahlt markieren", use_container_width=True):
                    rows = df[df[COL_PERSON] == person_aendern]["_row_idx"].tolist()
                    for row_idx in rows:
                        update_bezahlt(row_idx, "Bezahlt")
                    st.success(f"✅ {person_aendern} als bezahlt markiert!")
                    st.rerun()
        else:
            st.success("✅ Alle Personen haben bezahlt!")

        st.divider()

        col_l, col_r = st.columns(2)
        with col_l:
            offen_bar = (df[df[COL_BEZAHLT].astype(str).str.lower() == "offen"]
                         .groupby(COL_PERSON)[COL_ZAHLBETRAG].sum().reset_index())
            if not offen_bar.empty:
                fig = px.bar(offen_bar, x=COL_PERSON, y=COL_ZAHLBETRAG,
                             title="Offene Beträge pro Person",
                             color_discrete_sequence=["#F85149"],
                             template="plotly_white")
                fig.update_layout(**LAYOUT)
                fig.update_yaxes(ticksuffix=" €")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.success("✅ Alle Beträge bezahlt!")

        with col_r:
            status_df = df.groupby(COL_BEZAHLT)[COL_ZAHLBETRAG].sum().reset_index()
            fig2 = px.pie(status_df, names=COL_BEZAHLT, values=COL_ZAHLBETRAG,
                          hole=0.5, title="Verteilung nach Zahlstatus",
                          color_discrete_map={"Bezahlt": "#3FB950", "Offen": "#F85149"},
                          template="plotly_white")
            fig2.update_layout(paper_bgcolor="#f8f9fa", font_color="#1a1a2e",
                               showlegend=True, margin=dict(t=45, b=20))
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Detailtabelle")
        anzeige = df_f[[COL_PERSON, COL_ARTIKEL, COL_GROESSE, COL_EP_BRUTTO,
                         COL_RAB_PCT, COL_NETTO, COL_RAB_VEREIN,
                         COL_ZAHLBETRAG, COL_BEZAHLT]].copy()
        anzeige[COL_GROESSE] = anzeige[COL_GROESSE].apply(fmt_groesse)
        anzeige[COL_RAB_PCT] = anzeige[COL_RAB_PCT].apply(fmt_pct)
        for col in [COL_EP_BRUTTO, COL_NETTO, COL_RAB_VEREIN, COL_ZAHLBETRAG]:
            anzeige[col] = anzeige[col].apply(fmt)
        st.dataframe(anzeige, use_container_width=True, hide_index=True)

        # Excel Export gefiltert
        st.download_button(
            label="📥 Gefilterte Tabelle als Excel",
            data=make_excel(anzeige),
            file_name=f"Abrechnung_gefiltert_{date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

# ═══════════════════════════════════════════════════════════════
# ── SEITE 3: FINANZANALYSE
# ═══════════════════════════════════════════════════════════════
elif seite == "💰 Finanzanalyse":
    st.title("💰 Finanzanalyse & Werttreiber")

    if not hat_daten:
        st.info("Noch keine Daten vorhanden.")
    else:
        k1, k2, k3 = st.columns(3)
        k1.metric("Gesamtumsatz Brutto", fmt(gesamt_brutto))
        k2.metric("Gesamtersparnis",     fmt(ersparnis))
        k3.metric("Effektive Sparquote", fmt_pct(sparquote))
        st.divider()

        gesamt_rab_verein_sim = vereinszuschuss * anzahl_zeilen
        steps = ["Brutto", "− Rabatt\n", "Netto",
                 f"− Vereins-\nzuschuss\n({vereinszuschuss} €/P.)", "Verbleibend"]
        werte = [gesamt_brutto, -ersparnis, gesamt_netto,
                 -gesamt_rab_verein_sim, sim_verbleibend]
        typen = ["absolute", "relative", "total", "relative", "total"]

        fig_wf = go.Figure(go.Waterfall(
            orientation="v", measure=typen, x=steps, y=werte,
            texttemplate="%{y:,.2f} €", textposition="outside",
            connector={"line": {"color": "#cccccc"}},
            increasing={"marker": {"color": "#3FB950"}},
            decreasing={"marker": {"color": "#F85149"}},
            totals={"marker": {"color": "#2E75B6"}},
        ))
        fig_wf.update_layout(
            title="Value Flow — Finanzfluss",
            template="plotly_white", height=430,
            plot_bgcolor="#ffffff", paper_bgcolor="#f8f9fa",
            font_color="#1a1a2e", showlegend=False,
            margin=dict(t=45, b=30, l=10, r=10)
        )
        st.plotly_chart(fig_wf, use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            p_df = df.groupby(COL_PERSON)[COL_EP_BRUTTO].sum().reset_index()
            f1 = px.bar(p_df, x=COL_PERSON, y=COL_EP_BRUTTO,
                        title="Umsatz pro Person (Brutto)",
                        color_discrete_sequence=["#2E75B6"], template="plotly_white")
            f1.update_layout(**LAYOUT); f1.update_yaxes(ticksuffix=" €")
            st.plotly_chart(f1, use_container_width=True)
        with c2:
            a_df = df.groupby(COL_ARTIKEL)[COL_EP_BRUTTO].sum().reset_index()
            f2 = px.bar(a_df, x=COL_ARTIKEL, y=COL_EP_BRUTTO,
                        title="Top Produkte nach Umsatz",
                        color_discrete_sequence=["#E8A838"], template="plotly_white")
            f2.update_layout(**LAYOUT); f2.update_xaxes(tickangle=-30)
            f2.update_yaxes(ticksuffix=" €")
            st.plotly_chart(f2, use_container_width=True)

# ═══════════════════════════════════════════════════════════════
# ── SEITE 4: MENGENANALYSE
# ═══════════════════════════════════════════════════════════════
elif seite == "📦 Mengenanalyse":
    st.title("📦 Mengen- & Verteilungsanalyse")

    if not hat_daten:
        st.info("Noch keine Daten vorhanden.")
    else:
        k1, k2 = st.columns(2)
        k1.metric("Teilnehmer gesamt",     f"{anzahl_ges} Personen")
        k2.metric("Ø Zahlbetrag / Person", fmt(df[COL_ZAHLBETRAG].mean()))
        st.divider()

        c1, c2 = st.columns(2)
        with c1:
            gr_df = (df.groupby(COL_GROESSE)[COL_PERSON].count()
                     .reset_index().rename(columns={COL_PERSON: "Anzahl"}))
            gr_df["Grösse_label"] = gr_df[COL_GROESSE].apply(fmt_groesse)
            f1 = px.bar(gr_df, x="Grösse_label", y="Anzahl",
                        title="Bestellte Größen",
                        color_discrete_sequence=["#BC8CFF"], template="plotly_white")
            f1.update_layout(**LAYOUT); f1.update_xaxes(title="Größe")
            st.plotly_chart(f1, use_container_width=True)
        with c2:
            art_df = (df.groupby(COL_ARTIKEL)[COL_PERSON].count()
                      .reset_index().rename(columns={COL_PERSON: "Anzahl"})
                      .sort_values("Anzahl", ascending=False))
            f2 = px.bar(art_df, x=COL_ARTIKEL, y="Anzahl",
                        title="Produkt-Popularität",
                        color_discrete_sequence=["#1ABC9C"], template="plotly_white")
            f2.update_layout(**LAYOUT); f2.update_xaxes(tickangle=-30)
            st.plotly_chart(f2, use_container_width=True)

# ═══════════════════════════════════════════════════════════════
# ── SEITE 5: PERSONEN-DETAIL & QUITTUNG
# ═══════════════════════════════════════════════════════════════
elif seite == "👤 Personen-Detail & Quittung":
    st.title("👤 Personen-Detailanalyse & Quittung")

    if not hat_daten:
        st.info("Noch keine Daten vorhanden.")
    else:
        ausgewaehlt = st.selectbox("Person auswählen",
            sorted(df[COL_PERSON].unique().tolist()))
        df_p = df[df[COL_PERSON] == ausgewaehlt]

        brutto_p  = df_p[COL_EP_BRUTTO].sum()
        netto_p   = df_p[COL_NETTO].sum()
        zahl_p    = df_p[COL_ZAHLBETRAG].sum()
        ers_p     = brutto_p - netto_p
        sq_p      = (ers_p / brutto_p * 100) if brutto_p > 0 else 0
        zeilen_p  = len(df_p)
        sim_p     = max(0, netto_p - vereinszuschuss * zeilen_p)
        bezahlt_p = df_p[COL_BEZAHLT].astype(str).str.lower().iloc[0] == "bezahlt"

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Brutto",    fmt(brutto_p))
        k2.metric("Netto",     fmt(netto_p))
        k3.metric("Zu zahlen", fmt(zahl_p))
        k4.metric("Sparquote", fmt_pct(sq_p))

        if vereinszuschuss != 30:
            st.info(f"🎛️ Mit simuliertem Zuschuss {vereinszuschuss} € → "
                    f"verbleibend: **{fmt(sim_p)}**")

        col_status, col_btn = st.columns([3, 1])
        if bezahlt_p:
            col_status.success(f"✅ {ausgewaehlt} hat bezahlt")
        else:
            col_status.warning(f"⚠️ {ausgewaehlt} hat noch **{fmt(zahl_p)}** offen")
            with col_btn:
                st.write("")
                if st.button("✅ Jetzt als bezahlt markieren", use_container_width=True):
                    rows = df_p["_row_idx"].tolist()
                    for row_idx in rows:
                        update_bezahlt(row_idx, "Bezahlt")
                    st.success("Gespeichert!")
                    st.rerun()

        st.divider()
        anzeige_p = df_p[[COL_ARTIKEL, COL_GROESSE, COL_EP_BRUTTO, COL_RAB_PCT,
                           COL_NETTO, COL_RAB_VEREIN, COL_ZAHLBETRAG,
                           COL_BEZAHLT, COL_ERHALTEN]].copy()
        anzeige_p[COL_GROESSE] = anzeige_p[COL_GROESSE].apply(fmt_groesse)
        anzeige_p[COL_RAB_PCT] = anzeige_p[COL_RAB_PCT].apply(fmt_pct)
        for col in [COL_EP_BRUTTO, COL_NETTO, COL_RAB_VEREIN, COL_ZAHLBETRAG]:
            anzeige_p[col] = anzeige_p[col].apply(fmt)
        st.dataframe(anzeige_p, use_container_width=True, hide_index=True)

        st.divider()

        # ── QUITTUNGS-PDF ─────────────────────────────────────────────────────
        st.subheader("🧾 Quittung erstellen")
        st.caption("PDF-Quittung für diese Person herunterladen")
        pdf_bytes = make_quittung(ausgewaehlt, df_p, vereinszuschuss)
        st.download_button(
            label=f"📄 Quittung für {ausgewaehlt} herunterladen (PDF)",
            data=pdf_bytes,
            file_name=f"Quittung_{ausgewaehlt}_{date.today()}.pdf",
            mime="application/pdf",
            use_container_width=True,
        )

# ═══════════════════════════════════════════════════════════════
# ── SEITE 6: ZEITSTRAHL
# ═══════════════════════════════════════════════════════════════
elif seite == "📅 Zeitstrahl":
    st.title("📅 Zeitstrahl — Bestellfortschritt")

    if not hat_daten:
        st.info("Noch keine Daten vorhanden.")
    else:
        # Bezahlt-Fortschritt
        st.subheader("Bezahlstatus Übersicht")
        personen = sorted(df[COL_PERSON].unique().tolist())
        status_data = []
        for p in personen:
            df_pp = df[df[COL_PERSON] == p]
            status = df_pp[COL_BEZAHLT].astype(str).str.lower().iloc[0]
            zahl   = df_pp[COL_ZAHLBETRAG].sum()
            erh    = df_pp[COL_ERHALTEN].iloc[0] if COL_ERHALTEN in df_pp.columns else ""
            status_data.append({
                "Person": p,
                "Status": "✅ Bezahlt" if status == "bezahlt" else "❌ Offen",
                "Zu zahlen": fmt(zahl),
                "Erhalten am": str(erh) if str(erh).strip() else "—",
            })

        status_tbl = pd.DataFrame(status_data)
        bezahlt_count = sum(1 for s in status_data if "Bezahlt" in s["Status"])
        offen_count   = len(personen) - bezahlt_count
        fortschritt   = bezahlt_count / len(personen) if personen else 0

        # Fortschrittsbalken
        k1, k2, k3 = st.columns(3)
        k1.metric("Bezahlt",       f"{bezahlt_count} / {len(personen)}")
        k2.metric("Noch offen",    f"{offen_count} Personen")
        k3.metric("Fortschritt",   f"{fortschritt*100:.0f} %")

        st.progress(fortschritt)
        st.divider()

        # Gantt-artiges Diagramm — wer hat wann erhalten
        erhalten_df = df[df[COL_ERHALTEN].astype(str).str.strip() != ""].copy()
        if not erhalten_df.empty:
            st.subheader("Lieferungen — wer hat wann erhalten")
            erhalten_df["Datum"] = pd.to_datetime(
                erhalten_df[COL_ERHALTEN], errors="coerce")
            erhalten_valid = erhalten_df.dropna(subset=["Datum"])
            if not erhalten_valid.empty:
                fig_t = px.scatter(
                    erhalten_valid, x="Datum", y=COL_PERSON,
                    color=COL_BEZAHLT, size_max=15,
                    color_discrete_map={"Bezahlt": "#3FB950", "Offen": "#F85149"},
                    title="Lieferdaten pro Person",
                    template="plotly_white",
                )
                fig_t.update_traces(marker=dict(size=16, symbol="diamond"))
                fig_t.update_layout(
                    plot_bgcolor="#ffffff", paper_bgcolor="#f8f9fa",
                    font_color="#1a1a2e", showlegend=True,
                    margin=dict(t=45, b=30, l=10, r=10),
                    yaxis_title="Person", xaxis_title="Datum"
                )
                st.plotly_chart(fig_t, use_container_width=True)
        else:
            st.info("Noch keine Lieferdaten eingetragen.")

        st.divider()
        st.subheader("Status pro Person")
        st.dataframe(status_tbl, use_container_width=True, hide_index=True)

# ═══════════════════════════════════════════════════════════════
# ── SEITE 7: NEUE BESTELLUNG + WHATSAPP
# ═══════════════════════════════════════════════════════════════
elif seite == "➕ Neue Bestellung":
    st.title("➕ Neue Bestellung eingeben")
    st.caption("Daten werden direkt in Google Sheets gespeichert.")

    # Session State für WhatsApp-Link nach dem Speichern
    if "wa_url"         not in st.session_state: st.session_state.wa_url         = None
    if "wa_bestellung"  not in st.session_state: st.session_state.wa_bestellung  = []
    if "wa_rg_name"     not in st.session_state: st.session_state.wa_rg_name     = ""

    # ── MEHRERE POSITIONEN SAMMELN ────────────────────────────────────────────
    if "positionen" not in st.session_state:
        st.session_state.positionen = []

    st.subheader("📦 Bestellpositionen")
    if st.session_state.positionen:
        for i, pos in enumerate(st.session_state.positionen):
            col_a, col_b = st.columns([4, 1])
            col_a.write(f"**{pos['artikel']}** — Gr. {fmt_groesse(pos['groesse'])} | {fmt(pos['ep_brutto'])} | {fmt(pos['zahlbetrag'])} zu zahlen")
            if col_b.button("🗑️", key=f"del_{i}"):
                st.session_state.positionen.pop(i)
                st.rerun()
        st.divider()

    # ── POSITION HINZUFÜGEN ───────────────────────────────────────────────────
    with st.expander("➕ Position hinzufügen", expanded=True):
        with st.form("pos_form", clear_on_submit=True):
            c1, c2 = st.columns(2)
            with c1:
                if hat_daten:
                    auswahl = st.selectbox("Bestehende Person",
                        ["-- Neue Person --"] + sorted(df[COL_PERSON].unique().tolist()))
                else:
                    auswahl = "-- Neue Person --"
                neu_name = st.text_input("Neuer Name", placeholder="z.B. Max")
                person_pos = neu_name.strip() if auswahl == "-- Neue Person --" else auswahl
                artikel  = st.text_input("Artikel", placeholder="z.B. Vivor Freaky Green")
                groesse  = st.number_input("Größe", min_value=5.0, max_value=13.0,
                                           value=9.0, step=0.5, format="%.1f")
            with c2:
                ep_brutto = st.number_input("Einzelpreis Brutto (€)",
                                            min_value=0.0, value=69.95,
                                            step=0.05, format="%.2f")
                letzter_rab = float(df[COL_RAB_PCT].iloc[-1]) if hat_daten else 30.0
                rab_pct   = st.number_input("Rabatt vom Lieferanten (%)",
                                            min_value=0.0, max_value=100.0,
                                            value=round(letzter_rab, 1),
                                            step=0.5, format="%.1f",
                                            help="Ändert sich der Rabatt, hier anpassen")
                rab_verein = st.number_input("Vereinszuschuss (€)", min_value=0.0,
                                             value=30.0, step=5.0, format="%.2f")
                bezahlt   = st.checkbox("Bereits bezahlt?")
                erhalten  = st.date_input("Erhalten am (optional)", value=None)

            if ep_brutto > 0:
                rab_eur_p  = ep_brutto * rab_pct / 100
                netto_p    = ep_brutto - rab_eur_p
                zahlb_p    = max(0, netto_p - rab_verein)
                st.info(f"Brutto: **{fmt(ep_brutto)}** → −{fmt(rab_eur_p)} → Netto: {fmt(netto_p)} → −{fmt(rab_verein)} → **Zu zahlen: {fmt(zahlb_p)}**")

            add_ok = st.form_submit_button("➕ Position zur Liste hinzufügen", use_container_width=True)
            if add_ok:
                if not person_pos:
                    st.error("Bitte einen Namen eingeben.")
                elif not artikel:
                    st.error("Bitte einen Artikel eingeben.")
                elif ep_brutto <= 0:
                    st.error("Bitte einen Preis eingeben.")
                else:
                    rab_eur_a = ep_brutto * rab_pct / 100
                    netto_a   = ep_brutto - rab_eur_a
                    zahlb_a   = max(0, netto_a - rab_verein)
                    st.session_state.positionen.append({
                        "person":    person_pos.strip(),
                        "artikel":   artikel.strip(),
                        "groesse":   groesse,
                        "ep_brutto": ep_brutto,
                        "rab_pct":   rab_pct,
                        "rab_verein":rab_verein,
                        "bezahlt":   bezahlt,
                        "erhalten":  erhalten,
                        "zahlbetrag":zahlb_a,
                    })
                    st.success(f"✅ {artikel} Gr. {fmt_groesse(groesse)} hinzugefügt!")
                    st.rerun()

    # ── RECHNUNGSADRESSE & ABSENDEN ──────────────────────────────────────────
    if st.session_state.positionen:
        st.divider()
        st.subheader("📬 Rechnungsadresse (für WhatsApp an Lieferanten)")
        with st.form("rg_form"):
            rg_c1, rg_c2 = st.columns(2)
            rg_name   = rg_c1.text_input("Verein / Name", placeholder="z.B. SV Lembeck")
            rg_person = rg_c1.text_input("Ansprechpartner", placeholder="z.B. Michael Heller")
            rg_str    = rg_c2.text_input("Straße + Hausnummer", placeholder="z.B. Holunderweg 21")
            rg_plzort = rg_c2.text_input("PLZ + Ort", placeholder="z.B. 46286 Dorsten")
            rg_email  = rg_c2.text_input("E-Mail (optional)", placeholder="z.B. heller@web.de")

            col_save, col_wa = st.columns(2)
            speichern_ok = col_save.form_submit_button(
                "💾 Alle Positionen speichern", use_container_width=True)
            wa_ok = col_wa.form_submit_button(
                "💾 Speichern + 💬 WhatsApp vorbereiten", use_container_width=True)

            if speichern_ok or wa_ok:
                fehler = False
                for pos in st.session_state.positionen:
                    try:
                        save_row(pos["person"], pos["artikel"], pos["groesse"],
                                 pos["ep_brutto"], pos["rab_pct"], pos["rab_verein"],
                                 pos["bezahlt"], pos["erhalten"])
                    except Exception as e:
                        st.error(f"Fehler: {e}")
                        fehler = True
                        break

                if not fehler:
                    if wa_ok and rg_name:
                        rg_adresse = f"{rg_name}\n{rg_person}\n{rg_str}\n{rg_plzort}"
                        st.session_state.wa_url = make_whatsapp_url(
                            st.session_state.positionen,
                            rg_adresse, rg_str, rg_plzort, rg_email
                        )
                        st.session_state.wa_bestellung = st.session_state.positionen.copy()
                        st.session_state.wa_rg_name    = rg_name
                    st.session_state.positionen = []
                    st.success(f"✅ {len(st.session_state.wa_bestellung or st.session_state.positionen)+1} Positionen gespeichert!")
                    st.balloons()
                    st.rerun()

    # ── WHATSAPP BUTTON ───────────────────────────────────────────────────────
    if st.session_state.wa_url:
        st.divider()
        st.subheader("💬 WhatsApp an Lieferanten senden")
        st.caption("WhatsApp an deinen Lieferanten")

        # Vorschau der Nachricht
        positionen_preview = "\n".join([
            f"• {p['artikel']} GR. {fmt_groesse(p['groesse'])}"
            for p in st.session_state.wa_bestellung
        ])
        st.info(
            f"**Nachricht wird vorausgefüllt:**\n\n"
            f"Moin, ich hätte wieder eine kleine Bestellung.\n"
            f"{positionen_preview}\n\n"
            f"Rechnungsadresse: {st.session_state.wa_rg_name}\n"
            f"Lieferadresse: {LIEFERADRESSE.replace(chr(10), ", ")}"
        )

        st.link_button(
            "💬 WhatsApp öffnen & Nachricht senden",
            st.session_state.wa_url,
            use_container_width=True,
        )

        if st.button("✖️ WhatsApp-Link schließen"):
            st.session_state.wa_url        = None
            st.session_state.wa_bestellung = []
            st.rerun()
