from datetime import datetime
import os
import threading
import mysql.connector
import subprocess
from playwright.sync_api import sync_playwright
import re

from config import DB_CONFIG, Z_FOLDER

# Procesar los canales y grabar simultáneamente
def procesar_canales():
    fecha = datetime.today().strftime('%d-%m-%Y_%H-%M')
    try:
        mydb = mysql.connector.connect(**DB_CONFIG)
        mycursor = mydb.cursor()
        mycursor.execute("SELECT id_canales, nombre, url, tipo FROM canales")
        canales = mycursor.fetchall()
        mycursor.close()
        mydb.close()
        
        threads = []
        for canal_id, nombre, url, tipo in canales:
            if tipo == "vera":
                t = threading.Thread(target=grabar_stream_vera, args=(url, nombre, fecha, canal_id))
            elif tipo == "youtube":
                t = threading.Thread(target=grabar_stream_youtube, args=(url, nombre, fecha, canal_id))
            elif tipo == "no-dinamica":
                t = threading.Thread(target=grabar_stream, args=(url, nombre, fecha, canal_id))
            else:
                print(f"[ERROR] Tipo de canal desconocido: {tipo}")
                continue

            t.start()
            threads.append(t)

        for t in threads:
            t.join()
    except Exception as e:
        print(f"[ERROR] Error al procesar canales: {e}")

# Función para encontrar y grabar el stream de "vera"
def grabar_stream_vera(url, nombre, fecha, canal_id):
    responses = []

    def save_response(response):
        responses.append(response.url)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.on("response", save_response)
            page.goto(url, wait_until="networkidle", timeout=900000)

            stream_url = next((r for r in responses if ('.m3u8' in r and ('live' in r or 'xlive' in r))), None)
            if stream_url:
                print(f"Stream URL encontrado: {stream_url}")
                grabar_stream(stream_url, nombre, fecha, canal_id)
            else:       
                print(f"[ERROR] No se encontró una URL de stream válida para {nombre}")
                
    except Exception as e:
        print(f"[ERROR] Error al encontrar el stream para {nombre}: {e}")

# Función para encontrar un stream en vivo en YouTube
def grabar_stream_youtube(url, nombre, fecha, canal_id):
    try:
        stream_url = buscar_youtube_live(url)
        if stream_url:
            print(f"Transmisión en vivo encontrada para {nombre}: {stream_url}")
            grabar_stream(stream_url, nombre, fecha, canal_id)
        else:
            print(f"[INFO] No hay transmisión en vivo para {nombre}")
    except Exception as e:
        print(f"[ERROR] Error al buscar live en YouTube para {nombre}: {e}")

# Buscar si un canal de YouTube tiene un en vivo
def buscar_youtube_live(channel_url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(channel_url, wait_until="networkidle", timeout=60000)

            html_content = page.content()
            live_match = re.search(r'\"url\":\"(https:\\/\\/www\.youtube\.com\\/watch\\?v=[^\"]+)', html_content)

            if live_match:
                video_url = live_match.group(1).replace("\\u0026", "&")
                return video_url
            else:
                return None
    except Exception as e:
        print(f"[ERROR] Playwright falló al buscar live en YouTube: {e}")
        return None

def grabar_stream(url, nombre, fecha, canal_id):
    try:
        print(f"\n>> INICIANDO GRABACIÓN (ffmpeg): {nombre} ({url})")
        output_file = os.path.join(Z_FOLDER, f"{nombre}_{fecha}.mp4")
        os.makedirs(Z_FOLDER, exist_ok=True)

        ffmpeg_path = r'C:\Users\auditor\Desktop\PROGRAMAS\ffmpeg\bin\ffmpeg.exe'

        # Armamos comando como lista (más seguro que string plano)
        command = [
            ffmpeg_path,
            '-headers', 'User-Agent: Mozilla/5.0\r\n',
            '-i', url,
            '-c', 'copy',
            '-bsf:a', 'aac_adtstoasc',
            '-t', '5400',
            output_file
        ]

        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in process.stdout:
            print(line.decode().strip())
        process.wait()

        # Actualizar base de datos si todo sale bien
        mydb = mysql.connector.connect(**DB_CONFIG)
        cursor = mydb.cursor()
        cursor.execute(f"UPDATE canales SET estado='GRABADO' WHERE id_canales={canal_id}")
        mydb.commit()
        cursor.close()
        mydb.close()

    except Exception as e:
        print(f"[ERROR] {nombre}: {e}")
        try:
            mydb = mysql.connector.connect(**DB_CONFIG)
            cursor = mydb.cursor()
            cursor.execute(f"UPDATE canales SET estado='ERROR' WHERE id_canales={canal_id}")
            mydb.commit()
            cursor.close()
            mydb.close()
        except:
            print("No se pudo registrar el estado ERROR en la base de datos")


def main():
    procesar_canales()

if __name__ == "__main__":
    main()
