import requests
from datetime import datetime
import os
import pandas as pd
import zipfile
from google.transit import gtfs_realtime_pb2

# ======================================================
# PARAMÈTRES GÉNÉRAUX
# ======================================================

GTFS_RT_SA_URL = "https://proxy.transport.data.gouv.fr/resource/sncf-gtfs-rt-service-alerts"

ARCHIVE_DIR = "archives_gtfs_rt_sa"

# ======================================================
# 1️⃣ EXTRACTION + CONVERSION EXCEL
# ======================================================

def sauvegarde_gtfs_rt():

    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%d_%H-%M")
    today_str = now.strftime("%Y-%m-%d")

    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    pb_filename = f"gtfs_rt_sa_{timestamp}.pb"
    excel_filename = f"gtfs_rt_sa_{timestamp}.xlsx"

    pb_path = os.path.join(ARCHIVE_DIR, pb_filename)
    excel_path = os.path.join(ARCHIVE_DIR, excel_filename)

    try:
        print("🔄 Téléchargement du flux GTFS-RT SA...")
        response = requests.get(GTFS_RT_SA_URL, timeout=30)
        response.raise_for_status()

        # -------------------------
        # 1️⃣ Sauvegarde brute .pb
        # -------------------------
        with open(pb_path, "wb") as f:
            f.write(response.content)

        print(f"✅ Fichier PB sauvegardé : {pb_filename}")

        # -------------------------
        # 2️⃣ Lecture protobuf
        # -------------------------
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        records = []

        for entity in feed.entity:
            if not entity.HasField("alert"):
                continue

            alert = entity.alert
            effect = gtfs_realtime_pb2.Alert.Effect.Name(alert.effect)

            description = ""
            if alert.description_text:
                description = " | ".join(
                    t.text for t in alert.description_text.translation
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

        # -------------------------
        # 3️⃣ Sécurisation si vide
        # -------------------------
        if df.empty:
            print("⚠️ Aucune alerte détectée dans ce flux.")
            df = pd.DataFrame([{
                "extraction_utc": timestamp,
                "effect": "NO_ALERT",
                "description": "Aucune alerte active",
                "start": None,
                "end": None
            }])

        # -------------------------
        # 4️⃣ Export Excel
        # -------------------------
        df.to_excel(excel_path, index=False, engine="openpyxl")

        print(f"✅ Fichier Excel généré : {excel_filename}")

    except Exception as e:
        print(f"❌ Erreur lors de l'extraction : {e}")


# ======================================================
# 2️⃣ COMPRESSION JOURNALIÈRE
# ======================================================

def compression_journaliere():

    now = datetime.utcnow()
    today_str = now.strftime("%Y-%m-%d")

    zip_filename = os.path.join(ARCHIVE_DIR, f"{today_str}.zip")

    try:
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:

            for file in os.listdir(ARCHIVE_DIR):

                # On compresse uniquement les fichiers du jour
                if today_str in file and not file.endswith(".zip"):
                    file_path = os.path.join(ARCHIVE_DIR, file)
                    zipf.write(file_path, arcname=file)

        print(f"📦 Archive journalière créée : {today_str}.zip")

    except Exception as e:
        print(f"❌ Erreur lors de la compression : {e}")


# ======================================================
# 3️⃣ EXECUTION PRINCIPALE
# ======================================================

if __name__ == "__main__":

    print("=========================================")
    print("🚄 Collecte automatique GTFS-RT SA")
    print("=========================================")

    sauvegarde_gtfs_rt()
    compression_journaliere()

    print("🏁 Fin du script.")
