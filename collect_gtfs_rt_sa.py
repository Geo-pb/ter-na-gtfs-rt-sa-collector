import requests
from datetime import datetime
import os
import pandas as pd
from google.transit import gtfs_realtime_pb2

# ======================================================
# PARAMÈTRES
# ======================================================

GTFS_RT_SA_URL = "https://proxy.transport.data.gouv.fr/resource/sncf-gtfs-rt-service-alerts"

ARCHIVE_DIR = "archives_gtfs_rt_sa"

# ======================================================
# FONCTION PRINCIPALE
# ======================================================

def sauvegarde_gtfs_rt():

    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%d_%H-%M")

    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    pb_filename = f"gtfs_rt_sa_{timestamp}.pb"
    excel_filename = f"gtfs_rt_sa_{timestamp}.xlsx"

    pb_path = os.path.join(ARCHIVE_DIR, pb_filename)
    excel_path = os.path.join(ARCHIVE_DIR, excel_filename)

    try:
        response = requests.get(GTFS_RT_SA_URL, timeout=30)
        response.raise_for_status()

        # ==========================
        # 1️⃣ Sauvegarde brute .pb
        # ==========================
        with open(pb_path, "wb") as f:
            f.write(response.content)

        print(f"✅ Fichier PB sauvegardé : {pb_filename}")

        # ==========================
        # 2️⃣ Conversion en DataFrame
        # ==========================
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        records = []

        for entity in feed.entity:
            if not entity.HasField("alert"):
                continue

            alert = entity.alert
            effect = gtfs_realtime_pb2.Alert.Effect.Name(alert.effect)

            description = " ".join(
                t.text for t in alert.description_text
            )

            for period in alert.active_period:
                start = datetime.fromtimestamp(period.start) if period.start else None
                end = datetime.fromtimestamp(period.end) if period.end else None

                records.append({
                    "extraction_utc": timestamp,
                    "effect": effect,
                    "description": description,
                    "start": start,
                    "end": end
                })

        df = pd.DataFrame(records)

        # ==========================
        # 3️⃣ Export Excel
        # ==========================
        df.to_excel(excel_path, index=False)

        print(f"✅ Fichier Excel généré : {excel_filename}")

    except Exception as e:
        print(f"❌ Erreur : {e}")

# ======================================================
# EXECUTION
# ======================================================

if __name__ == "__main__":
    sauvegarde_gtfs_rt()
