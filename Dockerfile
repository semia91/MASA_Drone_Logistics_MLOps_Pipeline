# 1. Utiliser une image Python officielle légère comme fondation
FROM python:3.10-slim

# 2. Définir le dossier de travail interne du conteneur
WORKDIR /code

# 3. Copier le fichier des dépendances
COPY ./requirements.txt /code/requirements.txt

# 4. Installer les paquets Python sans stocker de cache lourd inutile
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# 5. Copier tout le dossier applicatif (main.py, modèle .pkl, CSV des Hubs)
COPY ./app /code/app

# 6. Exposer le port de communication web de FastAPI
EXPOSE 8000

# 7. Commande de lancement de l'API avec Uvicorn en mode de production
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]