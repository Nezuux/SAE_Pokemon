import subprocess
import os
import time

scripts_to_run = [
    "01_extension.py",
    "02_tournament.py",
    "03_player.py",
    "04_participation.py",
    "05_card.py",
    "06_card_complement.py",
    "07_card_evolve.py",
    "08_deck.py",
    "09_match.py",
    "10_deck_match.py",
    "11_deck_card.py"
]

json_folder_path = r"E:\DataCollection\output"
path_excel = r"E:\DataCollection\Table correpondance Extension pokémon.xlsx"

def decode_output(output_bytes):
    """Décodage ultra-robuste des sorties"""
    if not output_bytes:
        return ""
    
    # Essaie plusieurs encodages dans l'ordre
    encodings = ['utf-8', 'cp1252', 'latin-1', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            return output_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    
    # Dernier recours : décodage avec remplacement des caractères problématiques
    return output_bytes.decode('utf-8', errors='replace')

def run_script(script_name):
    start_time = time.perf_counter()
    try:
        env = os.environ.copy()
        env["JSON_FOLDER"] = json_folder_path
        env["PATH_EXCEL"] = path_excel
        
        # Configuration ultra-robuste pour éviter les problèmes d'encodage
        env["PYTHONUTF8"] = "1"  # Force UTF-8 sur Windows
        env["PYTHONIOENCODING"] = "utf-8:replace"  # Gestion des erreurs d'encodage

        result = subprocess.run(
            ["python", script_name],
            capture_output=True,
            env=env,
            check=False,
            # Ajout de timeout pour éviter les blocages
            timeout=3600  # 1 heure max par script
        )
    except subprocess.TimeoutExpired:
        return None, None, f"Timeout : {script_name} a dépassé 1 heure d'exécution"
    except Exception as e:
        return None, None, f"Exception lors de l'exécution : {e}"
    
    duration = time.perf_counter() - start_time
    return result, duration, None

def main():
    print("Démarrage du pipeline de traitement des données...")
    total_start_time = time.perf_counter()
    
    for i, script_name in enumerate(scripts_to_run, 1):
        if not os.path.exists(script_name):
            print(f"[ERREUR] Le script {script_name} est introuvable.\n")
            continue

        print(f"\n{'='*60}")
        print(f"[{i}/{len(scripts_to_run)}] Exécution de {script_name}")
        print('='*60)
        
        result, duration, error = run_script(script_name)

        if error:
            print(f"[ERREUR CRITIQUE] {error}")
            print("Arrêt du processus.")
            break

        print(f"\n[TIMING] Temps d'exécution : {duration:.2f} secondes")

        # Affichage sécurisé des sorties
        if result.stdout:
            try:
                decoded_stdout = decode_output(result.stdout)
                print("\n[SORTIE STANDARD]")
                print(decoded_stdout.strip())
            except Exception as e:
                print(f"[ERREUR] Impossible de décoder la sortie standard : {e}")

        if result.stderr:
            try:
                decoded_stderr = decode_output(result.stderr)
                print("\n[ERREURS]")
                print(decoded_stderr.strip())
            except Exception as e:
                print(f"[ERREUR] Impossible de décoder les erreurs : {e}")

        if result.returncode != 0:
            print(f"\n[ECHEC] Le script {script_name} a échoué avec le code {result.returncode}")
            print("Arrêt du processus.")
            break
        else:
            print(f"\n[SUCCES] {script_name} terminé avec succès")

    total_duration = time.perf_counter() - total_start_time
    print(f"\n{'='*60}")
    print(f"[PIPELINE] Durée totale : {total_duration:.2f} secondes")
    print('='*60)

if __name__ == "__main__":
    main()
