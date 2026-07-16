# =============================================================================
# collector/app.py — Data Ingestion, tappa 1.
#
# Responsabilità:
#   1. interrogare periodicamente (polling HTTP GET) una API ADS-B pubblica;
#   2. NORMALIZZARE ogni aeromobile allo schema canonico del progetto
#      (stessi nomi di campo e stesse unità qualunque sia la sorgente);
#   3. inviare un evento JSON per aeromobile a Logstash (HTTP POST).
#
# Sorgenti supportate (variabile d'ambiente SOURCE):
#   - adsbfi        → oggetti JSON tar1090/readsb, gratuita, senza API key
#   - airplaneslive → identico formato, alternativa
#   - opensky       → ARRAY POSIZIONALI (non oggetti!) + unità metriche,
#                     da mappare per indice e convertire in ft/kt/fpm
#
# Fail-safe: un errore HTTP non fa crashare il processo; si logga e si
# riprova al ciclo successivo (il ciclo di polling non muore mai).
# =============================================================================
import os
import time
import requests

# --- Configurazione via variabili d'ambiente (12-factor, vedi .env) ---
SOURCE = os.getenv("SOURCE", "adsbfi")  # adsbfi | airplaneslive | opensky
LOGSTASH_URL = os.getenv("LOGSTASH_URL", "http://logstash:5044")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))

# Area monitorata: cerchio di raggio AREA_DIST_NM (miglia nautiche)
# centrato su (AREA_LAT, AREA_LON).
AREA_LAT = os.getenv("AREA_LAT", "41.9")
AREA_LON = os.getenv("AREA_LON", "12.5")
AREA_DIST_NM = os.getenv("AREA_DIST_NM", "250")

# Credenziali OpenSky opzionali (mai hardcoded nel codice).
OPENSKY_USER = os.getenv("OPENSKY_USER", "")
OPENSKY_PASS = os.getenv("OPENSKY_PASS", "")

# User-Agent identificativo: buona educazione verso le API community.
UA = {"User-Agent": "tap-flight-monitor/1.0"}


def utc_now_iso():
    """Timestamp UTC in formato ISO8601 (tempo di osservazione dell'evento)."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def fetch_adsbfi():
    """adsb.fi opendata: aeromobili entro un raggio (nm) da un punto.
    NB (verificato sul campo): adsb.fi usa la chiave "aircraft", mentre
    airplanes.live usa "ac" — si accettano entrambe per robustezza."""
    url = f"https://opendata.adsb.fi/api/v2/lat/{AREA_LAT}/lon/{AREA_LON}/dist/{AREA_DIST_NM}"
    r = requests.get(url, headers=UA, timeout=10)
    r.raise_for_status()  # codici 4xx/5xx → eccezione (la gestisce il ciclo principale)
    j = r.json()
    return j.get("aircraft") or j.get("ac") or []


def fetch_airplaneslive():
    """airplanes.live: stesso formato tar1090/readsb di adsb.fi."""
    url = f"https://api.airplanes.live/v2/point/{AREA_LAT}/{AREA_LON}/{AREA_DIST_NM}"
    r = requests.get(url, headers=UA, timeout=10)
    r.raise_for_status()
    j = r.json()
    return j.get("ac") or j.get("aircraft") or []


def fetch_opensky():
    """OpenSky: stati in un bounding box grezzo ~ (lat±3°, lon±4°) attorno al centro."""
    lat, lon = float(AREA_LAT), float(AREA_LON)
    params = {"lamin": lat - 3, "lamax": lat + 3, "lomin": lon - 4, "lomax": lon + 4}
    auth = (OPENSKY_USER, OPENSKY_PASS) if OPENSKY_USER else None
    r = requests.get("https://opensky-network.org/api/states/all",
                     params=params, auth=auth, headers=UA, timeout=10)
    r.raise_for_status()
    return r.json().get("states", []) or []


def to_float(v):
    """Conversione difensiva: l'API può restituire None, stringhe ('ground')
    o valori sporchi. In caso di dubbio → None, mai un'eccezione."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def normalize_obj(ac):
    """Normalizza un oggetto tar1090/readsb (adsb.fi / airplanes.live)
    allo schema canonico del progetto. Unità già imperiali (ft, kt, fpm).

    Nota reale sul campo alt_baro: per gli aerei al suolo l'API restituisce
    la STRINGA "ground" invece di un numero → to_float dà None e la stringa
    viene usata per dedurre on_ground."""
    return {
        "event_time": utc_now_iso(),
        "source": SOURCE,
        "icao24": (ac.get("hex") or "").strip().lower() or None,
        "callsign": (ac.get("flight") or "").strip() or None,
        "lat": to_float(ac.get("lat")),
        "lon": to_float(ac.get("lon")),
        "baro_altitude_ft": to_float(ac.get("alt_baro")),
        "geo_altitude_ft": to_float(ac.get("alt_geom")),
        "ground_speed_kt": to_float(ac.get("gs")),
        "heading_deg": to_float(ac.get("track")),
        "vertical_rate_fpm": to_float(ac.get("baro_rate")),
        "squawk": (str(ac.get("squawk")).strip() if ac.get("squawk") is not None else None),
        "category": ac.get("category"),
        "on_ground": (str(ac.get("alt_baro")).lower() == "ground"),
    }


def normalize_opensky(s):
    """Normalizza uno stato OpenSky: è un ARRAY POSIZIONALE, non un oggetto.
    Indici: 0=icao24, 1=callsign, 5=lon, 6=lat, 7=baro_altitude(m),
    8=on_ground, 9=velocity(m/s), 10=true_track, 11=vertical_rate(m/s),
    13=geo_altitude(m), 14=squawk. Conversioni: m→ft, m/s→kt, m/s→fpm."""
    return {
        "event_time": utc_now_iso(),
        "source": "opensky",
        "icao24": (s[0] or "").strip().lower() or None,
        "callsign": (s[1] or "").strip() if s[1] else None,
        "lat": to_float(s[6]),
        "lon": to_float(s[5]),
        "baro_altitude_ft": (to_float(s[7]) * 3.28084) if s[7] is not None else None,   # m → ft
        "geo_altitude_ft": (to_float(s[13]) * 3.28084) if s[13] is not None else None,  # m → ft
        "ground_speed_kt": (to_float(s[9]) * 1.94384) if s[9] is not None else None,    # m/s → kt
        "heading_deg": to_float(s[10]),
        "vertical_rate_fpm": (to_float(s[11]) * 196.85) if s[11] is not None else None, # m/s → fpm
        "squawk": (str(s[14]).strip() if s[14] is not None else None),
        "category": None,
        "on_ground": bool(s[8]),
    }


def fetch_normalized():
    """Dispatch sulla sorgente configurata; default adsb.fi."""
    if SOURCE == "airplaneslive":
        return [normalize_obj(a) for a in fetch_airplaneslive()]
    if SOURCE == "opensky":
        return [normalize_opensky(s) for s in fetch_opensky()]
    return [normalize_obj(a) for a in fetch_adsbfi()]  # default adsb.fi


def send_to_logstash(events):
    """Invia un POST per aeromobile a Logstash. Gli eventi senza icao24
    (identificativo transponder) sono inutilizzabili e vengono scartati qui.
    Un errore su un singolo evento non blocca gli altri."""
    ok = 0
    for ev in events:
        if not ev.get("icao24"):
            continue
        try:
            requests.post(LOGSTASH_URL, json=ev, timeout=5)
            ok += 1
        except requests.RequestException as e:
            print(f"[ERROR] send {ev.get('icao24')}: {e}", flush=True)
    return ok


def main():
    print(f"[collector] boot source={SOURCE} area=({AREA_LAT},{AREA_LON},{AREA_DIST_NM}nm)", flush=True)
    # Ciclo di polling infinito e fail-safe: qualunque eccezione viene
    # loggata e si riprova al giro successivo (mai un crash del container).
    while True:
        try:
            events = fetch_normalized()
            sent = send_to_logstash(events)
            print(f"[OK] fetched={len(events)} sent={sent} at {time.strftime('%H:%M:%S')}", flush=True)
        except Exception as e:
            print(f"[ERROR] fetch cycle failed: {e}", flush=True)
        time.sleep(POLL_INTERVAL)  # pausa fino al prossimo giro di polling (default 30s)


if __name__ == "__main__":
    main()
