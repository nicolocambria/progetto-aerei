#!/usr/bin/env python3
# =============================================================================
# scripts/inject_test_event.py — Iniezione di eventi di test per la demo.
#
# Pubblica sulla pipeline uno o più eventi di telemetria SINTETICI con
# squawk=7700 (emergenza generale) o con un outlier fisico, così la
# generazione dell'alert è dimostrabile A COMANDO, senza dipendere dal
# traffico aereo reale del momento.
#
# L'evento viene POSTato a Logstash (http://localhost:5044), cioè entra
# dalla STESSA porta della telemetria vera: attraversa validazione e routing
# e finisce su flights.telemetry esattamente come un evento reale
# (dimostrazione onesta dell'intera pipeline, non un bypass).
#
# Usa solo la libreria standard (urllib): nessun pip install richiesto.
#
# Esempi:
#   python3 scripts/inject_test_event.py                    # 1 evento squawk=7700
#   python3 scripts/inject_test_event.py --type alt         # quota implausibile
#   python3 scripts/inject_test_event.py --n 30             # 30 eventi (per il training ML)
#   python3 scripts/inject_test_event.py --url http://localhost:5044
# =============================================================================
import argparse
import json
import random
import time
import urllib.request


def utc_now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def build_event(kind: str, i: int) -> dict:
    """Costruisce un evento di telemetria canonico, completo di tutti i campi
    (il training ML scarta le righe con null: qui non ce ne sono).
    I valori vengono leggermente randomizzati per non produrre duplicati."""
    ev = {
        "event_time": utc_now_iso(),
        "source": "adsbfi",
        # icao24 fittizio riconoscibile: aaaaXX (fuori dai range reali italiani)
        "icao24": f"aaaa{i % 100:02d}",
        "callsign": f"TEST{i % 100:02d}",
        # posizione dentro l'area monitorata (vicino Roma), con jitter
        "lat": round(41.9 + random.uniform(-0.5, 0.5), 4),
        "lon": round(12.5 + random.uniform(-0.5, 0.5), 4),
        "baro_altitude_ft": round(random.uniform(30000, 38000), 0),
        "geo_altitude_ft": round(random.uniform(30200, 38200), 0),
        "ground_speed_kt": round(random.uniform(400, 480), 0),
        "heading_deg": round(random.uniform(0, 359), 0),
        "vertical_rate_fpm": round(random.uniform(-500, 500), 0),
        "squawk": "1000",
        "category": "A3",
        "on_ground": False,
    }
    # Il tipo di anomalia scelto sovrascrive il campo corrispondente.
    if kind == "squawk":
        ev["squawk"] = "7700"                       # → SQUAWK_EMERGENCY (sev 5)
    elif kind == "alt":
        ev["baro_altitude_ft"] = round(random.uniform(65000, 80000), 0)  # → ALT_OUTLIER
    elif kind == "speed":
        ev["ground_speed_kt"] = round(random.uniform(750, 900), 0)       # → SPEED_OUTLIER
    elif kind == "vs":
        ev["vertical_rate_fpm"] = round(random.choice([-1, 1]) * random.uniform(11000, 15000), 0)  # → VS_OUTLIER
    return ev


def main():
    ap = argparse.ArgumentParser(description="Inietta eventi di test nella pipeline via Logstash")
    ap.add_argument("--url", default="http://localhost:5044", help="endpoint HTTP di Logstash")
    ap.add_argument("--type", default="squawk", choices=["squawk", "alt", "speed", "vs", "mix"],
                    help="tipo di anomalia da iniettare (mix = alterna i 4 tipi)")
    ap.add_argument("--n", type=int, default=1, help="numero di eventi da inviare")
    ap.add_argument("--sleep", type=float, default=0.1, help="pausa tra un evento e l'altro (s)")
    args = ap.parse_args()

    kinds = ["squawk", "alt", "speed", "vs"]
    for i in range(args.n):
        kind = kinds[i % 4] if args.type == "mix" else args.type
        ev = build_event(kind, i)
        req = urllib.request.Request(
            args.url,
            data=json.dumps(ev).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            print(f"[inject] {i+1}/{args.n} type={kind} icao24={ev['icao24']} "
                  f"squawk={ev['squawk']} -> HTTP {resp.status}")
        time.sleep(args.sleep)

    print(f"[inject] fatto: {args.n} evento/i inviato/i a {args.url}")


if __name__ == "__main__":
    main()
